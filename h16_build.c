/*
 * h16_build.c
 *
 * One-shot build of the blocks_h16 secondary index. See h16_build.h
 * for the high-level design.
 *
 * Key design points:
 *
 *   - Each fileid is processed in its own transaction. The fileid's
 *     INSERTs and its row in blocks_h16_build_progress commit
 *     atomically together, so a row in blocks_h16_build_progress
 *     reliably indicates the fileid's h16 entries are durable. On
 *     crash recovery, we resume by simply skipping completed fileids.
 *
 *   - Within a fileid we stream rows from blocks ORDER BY loff using
 *     a sliding 4-row window. We do NOT load all of a file's rows
 *     into memory; the window is fixed-size, so memory usage is O(1)
 *     per worker regardless of file size.
 *
 *   - The window resets on any (loff, loff+blocksize) gap. Phase 1
 *     scans with --skip-zeroes leave gaps in blocks for skipped
 *     zero-blocks; an h16 across such a gap would describe a 16 KB
 *     range that is partly synthetic-zero and partly real data, which
 *     would not match anything during --lookup-only (image.raw also
 *     skips zeros). Dropping those windows is correct, not lossy.
 *
 *   - WAL checkpointing fires every H16_CHECKPOINT_FILES files to
 *     keep the WAL bounded. Without periodic checkpoints, WAL has
 *     historically grown to 27-30 GB on this hashfile size.
 *
 *   - Disk-space monitoring fires every H16_DF_CHECK_FILES files. If
 *     free space drops below H16_MIN_FREE_BYTES, we commit the
 *     in-flight fileid and exit cleanly. The blocks_h16_build_progress
 *     state lets a future invocation pick up where we left off.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <sqlite3.h>

#include "csum.h"
#include "dbfile.h"
#include "debug.h"
#include "h16_build.h"
#include "opt.h"
#include "util.h"

/*
 * Local mirror of the perror_sqlite macro from dbfile.c. The
 * canonical version is private to dbfile.c (defined inside the .c
 * file rather than the header) so it cannot be re-used from here
 * directly. Keep the message format consistent with the canonical so
 * grep on error messages stays useful.
 */
#if (SQLITE_VERSION_NUMBER < 3007015)
#define perror_sqlite(_err, _why)					\
	eprintf("%s(): Database error %d while %s: %s\n",		\
		__FUNCTION__, _err, _why, "[sqlite3_errstr() unavailable]")
#else
#define perror_sqlite(_err, _why)					\
	eprintf("%s(): Database error %d while %s: %s\n",		\
		__FUNCTION__, _err, _why, sqlite3_errstr(_err))
#endif

/* How often to PRAGMA wal_checkpoint(PASSIVE) (in fileids processed). */
#define H16_CHECKPOINT_FILES	100

/* How often to check free disk space (in fileids processed). */
#define H16_DF_CHECK_FILES	10

/*
 * Minimum free bytes on the hashfile's volume before we bail out of
 * the build. Sized so that a single fileid's transaction has plenty
 * of headroom even on the largest files, and that the WAL has room to
 * grow to its checkpoint interval.
 */
#define H16_MIN_FREE_BYTES	(20ULL * 1024ULL * 1024ULL * 1024ULL)

/* Window size in 4 KB blocks; an h16 covers 4 * blocksize = 16 KB. */
#define H16_WINDOW_BLOCKS	4

/*
 * Return free bytes on the filesystem hosting `path` via statvfs.
 * Returns 0 on failure (and logs); callers should treat 0 as
 * "couldn't measure" and continue rather than spuriously aborting.
 */
static uint64_t h16_free_bytes(const char *path)
{
	struct statvfs sv;
	if (statvfs(path, &sv) != 0) {
		eprintf("h16: statvfs(\"%s\") failed: %s\n",
			path, strerror(errno));
		return 0;
	}
	return (uint64_t)sv.f_bavail * (uint64_t)sv.f_frsize;
}

/*
 * One sliding window of H16_WINDOW_BLOCKS contiguous (loff, digest)
 * pairs. `count` tracks how many slots are currently valid (0-4).
 * `next_loff` is the loff value we expect on the next row to keep
 * the window contiguous; a row with any other loff resets the window.
 */
struct h16_window {
	uint64_t loff[H16_WINDOW_BLOCKS];
	unsigned char digest[H16_WINDOW_BLOCKS][DIGEST_LEN];
	int count;
	uint64_t next_loff;
};

static void h16_window_reset(struct h16_window *w)
{
	w->count = 0;
	w->next_loff = 0;
}

/*
 * Append a (loff, digest) pair to the window. If the window is
 * already full, shift it left by one before appending so the
 * most-recent H16_WINDOW_BLOCKS rows are always at indices 0..count-1.
 */
static void h16_window_append(struct h16_window *w, uint64_t loff,
			      const unsigned char *digest)
{
	if (w->count == H16_WINDOW_BLOCKS) {
		memmove(w->loff, &w->loff[1],
			(H16_WINDOW_BLOCKS - 1) * sizeof(uint64_t));
		memmove(w->digest, &w->digest[1],
			(H16_WINDOW_BLOCKS - 1) * DIGEST_LEN);
		w->count = H16_WINDOW_BLOCKS - 1;
	}

	w->loff[w->count] = loff;
	memcpy(w->digest[w->count], digest, DIGEST_LEN);
	w->count++;
}

/*
 * Compute the h16 hash from the 4 digests in the window (which must
 * be full) and write it to `out`. The hash is XXH128 of the four
 * digests concatenated in loff order.
 */
static void h16_window_hash(const struct h16_window *w, unsigned char *out)
{
	unsigned char concat[H16_WINDOW_BLOCKS * DIGEST_LEN];

	memcpy(concat, w->digest, H16_WINDOW_BLOCKS * DIGEST_LEN);
	checksum_block((char *)concat, sizeof(concat), out);
}

/*
 * Build h16 entries for a single fileid in its own transaction. On
 * success records the fileid in blocks_h16_build_progress in the
 * same transaction and commits.
 *
 * select_stmt: prepared SELECT(loff, digest) FROM blocks WHERE fileid=?
 *              ORDER BY loff
 * insert_stmt: prepared INSERT INTO blocks_h16 (h16, fileid, loff)
 * progress_stmt: prepared INSERT INTO blocks_h16_build_progress
 *                (fileid, completed_at)
 *
 * Returns 0 on success or an errno-style code on failure (with the
 * transaction rolled back).
 */
static int h16_build_one_fileid(sqlite3 *db, int64_t fileid,
				 unsigned int blocksize,
				 sqlite3_stmt *select_stmt,
				 sqlite3_stmt *insert_stmt,
				 sqlite3_stmt *progress_stmt,
				 uint64_t *rows_inserted)
{
	struct h16_window window;
	int ret;
	int rc;
	uint64_t inserted = 0;
	bool in_transaction = false;

	h16_window_reset(&window);

	ret = sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "h16 build: BEGIN");
		return EIO;
	}
	in_transaction = true;

	sqlite3_reset(select_stmt);
	ret = sqlite3_bind_int64(select_stmt, 1, fileid);
	if (ret) {
		perror_sqlite(ret, "h16 build: bind select fileid");
		ret = EIO;
		goto out;
	}

	while ((rc = sqlite3_step(select_stmt)) == SQLITE_ROW) {
		uint64_t loff = (uint64_t)sqlite3_column_int64(select_stmt, 0);
		const unsigned char *digest =
			sqlite3_column_blob(select_stmt, 1);
		int digest_len = sqlite3_column_bytes(select_stmt, 1);

		if (digest_len != DIGEST_LEN) {
			eprintf("h16 build: fileid %"PRId64" loff %"PRIu64
				" has digest length %d (expected %d); "
				"aborting build\n",
				fileid, loff, digest_len, DIGEST_LEN);
			ret = EIO;
			goto out;
		}

		if (window.count > 0 && loff != window.next_loff) {
			/* Non-contiguous (zero-skipped gap); reset. */
			h16_window_reset(&window);
		}

		h16_window_append(&window, loff, digest);
		window.next_loff = loff + blocksize;

		if (window.count == H16_WINDOW_BLOCKS) {
			unsigned char h16[DIGEST_LEN];

			h16_window_hash(&window, h16);

			sqlite3_reset(insert_stmt);
			ret = sqlite3_bind_blob(insert_stmt, 1, h16,
						DIGEST_LEN, SQLITE_TRANSIENT);
			if (ret) {
				perror_sqlite(ret,
					"h16 build: bind h16 blob");
				ret = EIO;
				goto out;
			}
			ret = sqlite3_bind_int64(insert_stmt, 2, fileid);
			if (ret) {
				perror_sqlite(ret,
					"h16 build: bind insert fileid");
				ret = EIO;
				goto out;
			}
			ret = sqlite3_bind_int64(insert_stmt, 3,
						 window.loff[0]);
			if (ret) {
				perror_sqlite(ret,
					"h16 build: bind insert loff");
				ret = EIO;
				goto out;
			}

			ret = sqlite3_step(insert_stmt);
			if (ret != SQLITE_DONE) {
				perror_sqlite(ret,
					"h16 build: stepping insert");
				ret = EIO;
				goto out;
			}
			ret = 0;
			inserted++;
		}
	}

	if (rc != SQLITE_DONE) {
		perror_sqlite(rc, "h16 build: stepping select");
		ret = EIO;
		goto out;
	}

	sqlite3_reset(progress_stmt);
	ret = sqlite3_bind_int64(progress_stmt, 1, fileid);
	if (ret) {
		perror_sqlite(ret, "h16 build: bind progress fileid");
		ret = EIO;
		goto out;
	}
	ret = sqlite3_bind_int64(progress_stmt, 2, (int64_t)time(NULL));
	if (ret) {
		perror_sqlite(ret, "h16 build: bind progress completed_at");
		ret = EIO;
		goto out;
	}
	ret = sqlite3_step(progress_stmt);
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "h16 build: stepping progress");
		ret = EIO;
		goto out;
	}
	ret = 0;

	ret = sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "h16 build: COMMIT");
		ret = EIO;
		goto out;
	}
	in_transaction = false;

	*rows_inserted = inserted;

out:
	if (in_transaction) {
		int rb = sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
		if (rb)
			perror_sqlite(rb, "h16 build: ROLLBACK");
	}
	return ret;
}

int h16_build_index(struct dbhandle *db)
{
	struct dbfile_config cfg;
	sqlite3 *sdb = db->db;
	sqlite3_stmt *list_stmt = NULL;
	sqlite3_stmt *select_stmt = NULL;
	sqlite3_stmt *insert_stmt = NULL;
	sqlite3_stmt *progress_stmt = NULL;
	const char *dbpath = NULL;
	int ret;
	int rc;
	uint64_t files_done = 0;
	uint64_t rows_total = 0;
	struct timespec start_ts, last_progress_ts;
	bool aborted_low_disk = false;

	ret = dbfile_get_config(sdb, &cfg);
	if (ret) {
		perror_sqlite(ret, "h16 build: reading hashfile config");
		return EIO;
	}

	if (cfg.blocksize == 0) {
		eprintf("h16 build: hashfile blocksize is 0; refusing.\n");
		return EINVAL;
	}

	/*
	 * options.hashfile is the user-supplied path to the hashfile,
	 * which is what we need for the df check below. It is not
	 * necessarily the canonical sqlite-resolved path but for
	 * statvfs() purposes any path on the right filesystem is
	 * sufficient. If it is NULL we skip df monitoring.
	 */
	dbpath = options.hashfile;

	/*
	 * PRAGMA wal_autocheckpoint causes the SQLite library to
	 * checkpoint when the WAL reaches the given page count, which
	 * bounds the WAL during long INSERT-heavy operations like
	 * this one. We also issue explicit checkpoints from the loop
	 * below for additional safety.
	 *
	 * 5000 pages * default 4 KB page size = 20 MB checkpoint
	 * interval. Aggressive enough to keep the WAL bounded under
	 * the heavy INSERT pattern.
	 */
	ret = sqlite3_exec(sdb, "PRAGMA wal_autocheckpoint = 5000;",
			   NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "h16 build: setting wal_autocheckpoint");
		/* Non-fatal: defaults are workable. */
	}

	/*
	 * Bulk-load: drop idx_blocks_h16 before INSERTing, recreate
	 * once at the end.
	 *
	 * h16 values are uniformly random (XXH128 of random digests),
	 * so per-row index maintenance during INSERTs touches the
	 * leaf B-tree pages in random order. Once the index outgrows
	 * the SQLite page cache (a few GB on a multi-tens-of-GB
	 * index), every insert becomes a random write to a different
	 * leaf page - the cache thrash pattern that motivated the
	 * existing dbfile_drop_bulk_load_indexes optimization for
	 * idx_blocks_digest. Same fix applies here.
	 *
	 * Re-creating the index at the end is a single sequential
	 * sort-and-build pass which is dramatically faster on
	 * already-populated data than maintaining the index per-row.
	 *
	 * Resumability interaction: if we crash mid-build, the next
	 * dbfile_prepare() will recreate idx_blocks_h16 (via
	 * create_indexes()) on whatever partial data is present.
	 * That's wasted work, but a subsequent --build-h16-index
	 * resume will simply drop it again at this point and
	 * continue. The end-state correctness is preserved.
	 */
	ret = sqlite3_exec(sdb, "DROP INDEX IF EXISTS idx_blocks_h16;",
			   NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "h16 build: dropping idx_blocks_h16");
		return EIO;
	}

	/*
	 * List of fileids that still need to be processed. We
	 * deliberately compute this once up front (rather than
	 * re-querying after each commit), since the list is bounded
	 * by the number of files in the hashfile and won't grow during
	 * the build.
	 */
#define LIST_FILEIDS_TODO						\
"SELECT id FROM files WHERE id NOT IN "					\
"(SELECT fileid FROM blocks_h16_build_progress) "			\
"ORDER BY id ASC;"
	ret = sqlite3_prepare_v2(sdb, LIST_FILEIDS_TODO, -1,
				 &list_stmt, NULL);
	if (ret) {
		perror_sqlite(ret, "h16 build: preparing fileid list");
		ret = EIO;
		goto out;
	}

#define SELECT_BLOCK_DIGESTS						\
"SELECT loff, digest FROM blocks WHERE fileid = ?1 ORDER BY loff;"
	ret = sqlite3_prepare_v2(sdb, SELECT_BLOCK_DIGESTS, -1,
				 &select_stmt, NULL);
	if (ret) {
		perror_sqlite(ret, "h16 build: preparing block-digest select");
		ret = EIO;
		goto out;
	}

#define INSERT_H16							\
"INSERT INTO blocks_h16 (h16, fileid, loff) VALUES (?1, ?2, ?3);"
	ret = sqlite3_prepare_v2(sdb, INSERT_H16, -1, &insert_stmt, NULL);
	if (ret) {
		perror_sqlite(ret, "h16 build: preparing h16 insert");
		ret = EIO;
		goto out;
	}

#define INSERT_PROGRESS							\
"INSERT INTO blocks_h16_build_progress (fileid, completed_at) "		\
"VALUES (?1, ?2);"
	ret = sqlite3_prepare_v2(sdb, INSERT_PROGRESS, -1,
				 &progress_stmt, NULL);
	if (ret) {
		perror_sqlite(ret, "h16 build: preparing progress insert");
		ret = EIO;
		goto out;
	}

	clock_gettime(CLOCK_MONOTONIC, &start_ts);
	last_progress_ts = start_ts;

	qprintf("h16 build: starting (blocksize=%u, window=%u blocks = %u KB)\n",
		cfg.blocksize, H16_WINDOW_BLOCKS,
		(cfg.blocksize * H16_WINDOW_BLOCKS) / 1024);

	while ((rc = sqlite3_step(list_stmt)) == SQLITE_ROW) {
		int64_t fileid = sqlite3_column_int64(list_stmt, 0);
		uint64_t inserted = 0;

		ret = h16_build_one_fileid(sdb, fileid, cfg.blocksize,
					   select_stmt, insert_stmt,
					   progress_stmt, &inserted);
		if (ret) {
			eprintf("h16 build: fileid %"PRId64" failed; "
				"aborting (resumable on rerun)\n", fileid);
			goto out;
		}

		files_done++;
		rows_total += inserted;

		/* Periodic WAL checkpoint to bound on-disk WAL size. */
		if ((files_done % H16_CHECKPOINT_FILES) == 0) {
			int crc = sqlite3_exec(sdb,
				"PRAGMA wal_checkpoint(PASSIVE);",
				NULL, NULL, NULL);
			if (crc) {
				/* Non-fatal: autocheckpoint will eventually
				 * catch up. */
				perror_sqlite(crc,
					"h16 build: wal_checkpoint");
			}
		}

		/* Periodic disk-space safety check. */
		if (dbpath && (files_done % H16_DF_CHECK_FILES) == 0) {
			uint64_t free = h16_free_bytes(dbpath);
			if (free && free < H16_MIN_FREE_BYTES) {
				eprintf("h16 build: free space on volume "
					"hosting hashfile dropped to %s "
					"(below %s threshold). Stopping "
					"cleanly; rerun --build-h16-index "
					"after freeing space to resume.\n",
					pretty_size(free),
					pretty_size(H16_MIN_FREE_BYTES));
				aborted_low_disk = true;
				ret = ENOSPC;
				goto out;
			}
		}

		/* Throttled progress line: emit at most once every 5s. */
		{
			struct timespec now;
			double dt;

			clock_gettime(CLOCK_MONOTONIC, &now);
			dt = (now.tv_sec - last_progress_ts.tv_sec) +
			     (now.tv_nsec - last_progress_ts.tv_nsec) / 1e9;
			if (dt >= 5.0) {
				double total_dt =
					(now.tv_sec - start_ts.tv_sec) +
					(now.tv_nsec - start_ts.tv_nsec)
						/ 1e9;
				qprintf("h16 build: %"PRIu64" files done, "
					"%"PRIu64" h16 rows inserted, "
					"%.1f min elapsed\n",
					files_done, rows_total,
					total_dt / 60.0);
				last_progress_ts = now;
			}
		}
	}

	if (rc != SQLITE_DONE) {
		perror_sqlite(rc, "h16 build: stepping fileid list");
		ret = EIO;
		goto out;
	}

	/*
	 * Rebuild idx_blocks_h16 now that all rows are in. This is
	 * the matching half of the bulk-load drop above. CREATE INDEX
	 * sorts the table once and writes the leaf pages in order,
	 * which is dramatically faster than maintaining the index
	 * per-row during INSERT for a uniformly-random key column.
	 */
	{
		struct timespec idx_start, idx_end;
		double idx_dt;

		qprintf("h16 build: rebuilding idx_blocks_h16 covering "
			"index (sort + write); this may take a while...\n");
		clock_gettime(CLOCK_MONOTONIC, &idx_start);
		ret = sqlite3_exec(sdb,
			"CREATE INDEX IF NOT EXISTS idx_blocks_h16 "
			"ON blocks_h16(h16, fileid, loff);",
			NULL, NULL, NULL);
		if (ret) {
			perror_sqlite(ret,
				"h16 build: rebuilding idx_blocks_h16");
			ret = EIO;
			goto out;
		}
		clock_gettime(CLOCK_MONOTONIC, &idx_end);
		idx_dt = (idx_end.tv_sec - idx_start.tv_sec) +
			 (idx_end.tv_nsec - idx_start.tv_nsec) / 1e9;
		qprintf("h16 build: idx_blocks_h16 rebuilt in %.1f min\n",
			idx_dt / 60.0);
	}

	/* Final checkpoint to flush any remaining WAL into the main DB. */
	{
		int crc = sqlite3_exec(sdb,
			"PRAGMA wal_checkpoint(TRUNCATE);",
			NULL, NULL, NULL);
		if (crc)
			perror_sqlite(crc,
				"h16 build: final wal_checkpoint");
	}

	{
		struct timespec now;
		double total_dt;

		clock_gettime(CLOCK_MONOTONIC, &now);
		total_dt = (now.tv_sec - start_ts.tv_sec) +
			   (now.tv_nsec - start_ts.tv_nsec) / 1e9;
		qprintf("h16 build: done. %"PRIu64" files processed, "
			"%"PRIu64" h16 rows inserted, %.1f min total.\n",
			files_done, rows_total, total_dt / 60.0);
	}

	ret = 0;

out:
	if (list_stmt)
		sqlite3_finalize(list_stmt);
	if (select_stmt)
		sqlite3_finalize(select_stmt);
	if (insert_stmt)
		sqlite3_finalize(insert_stmt);
	if (progress_stmt)
		sqlite3_finalize(progress_stmt);

	if (aborted_low_disk)
		return ENOSPC;
	return ret;
}
