#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sqlite3.h>
#include <errno.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/syscall.h>

#include "csum.h"
#include "filerec.h"
#include "hash-tree.h"
#include "results-tree.h"
#include "file_scan.h"
#include "debug.h"

#include "dbfile.h"
#include "opt.h"

static struct dbhandle *gdb = NULL;

/*
 * Process-local flag coordinating the H_WRITE bulk-load pattern with
 * worker connections opened later by dbfile_open_handle_thread().
 *
 * dbfile_drop_bulk_load_indexes() sets this true before issuing the
 * DROP statements; dbfile_create_bulk_load_indexes() clears it after
 * the rebuild succeeds. While set, create_indexes() skips the
 * digest indexes (idx_blocks_digest, idx_extents_digest_len) so that
 * a worker thread opening its first SQLite connection mid-scan does
 * not silently rebuild the indexes we just dropped (which would both
 * undo the optimization and block the worker for the duration of the
 * rebuild). The fileid indexes are always created so worker DELETEs
 * via dbfile_remove_hashes() keep using them.
 *
 * Synchronization: the flag is set by the main thread before any
 * worker is spawned, and cleared after scan_files() returns (i.e.
 * after all workers have joined). g_thread_pool_push and pthread
 * thread creation establish happens-before between the main thread's
 * write and each worker's first read, so no explicit barrier or
 * mutex is required for the bool itself; reads under dbfile_lock()
 * via dbfile_open_handle_thread() are an additional safety net.
 */
static bool dbfile_skip_digest_index_creation = false;

static sqlite3 *__dbfile_open_handle(char *filename, bool force_create);

static GMutex io_mutex; /* Locks db writes */

#if (SQLITE_VERSION_NUMBER < 3007015)
#define	perror_sqlite(_err, _why)					\
	eprintf("%s(): Database error %d while %s: %s\n",	\
		__FUNCTION__, _err, _why, "[sqlite3_errstr() unavailable]")
#else
#define	perror_sqlite(_err, _why)					\
	eprintf("%s()/%ld: Database error %d while %s: %s\n",	\
		__FUNCTION__, syscall(SYS_gettid), _err, _why, sqlite3_errstr(_err))
#endif

#define	perror_sqlite_open(_ptr, _filename)				\
	eprintf("Error opening db \"%s\": %s\n", _filename,	\
		sqlite3_errmsg(_ptr))

struct dbhandle *dbfile_get_handle(void)
{
	return gdb;
}

void dbfile_lock(void)
{
	g_mutex_lock(&io_mutex);
}

void dbfile_unlock(void)
{
	g_mutex_unlock(&io_mutex);
}

static void dbfile_config_defaults(struct dbfile_config *cfg)
{
	memset(cfg, 0, sizeof(*cfg));
	cfg->blocksize = blocksize;
	memcpy(cfg->hash_type, HASH_TYPE, 8);

	cfg->major = DB_FILE_MAJOR;
	cfg->minor = DB_FILE_MINOR;

	cfg->blocksize = blocksize;
}

static int dbfile_get_dbpath(sqlite3 *db, char *path)
{
	int ret;
	_cleanup_(sqlite3_stmt_cleanup) sqlite3_stmt *stmt = NULL;
	const char *buf;

#define GET_DBPATH "select file from pragma_database_list where name = 'main' limit 1;"
	ret = sqlite3_prepare_v2(db, GET_DBPATH, -1, &stmt, NULL);
	if (ret) {
		perror_sqlite(ret, "preparing statement");
		return ret;
	}

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_ROW) {
		perror_sqlite(ret, "fetching database's backend path");
		return ret;
	}

	buf = (char *)sqlite3_column_text(stmt, 0);
	if (strnlen(buf, PATH_MAX) != 0) {
		strncpy(path, buf, PATH_MAX);
	} else {
		strcpy(path, "(null)");
	}

	return 0;
}

static int dbfile_check(sqlite3 *db, struct dbfile_config *cfg)
{
	char path[PATH_MAX + 1];

	if (cfg->major != DB_FILE_MAJOR || cfg->minor != DB_FILE_MINOR) {
		eprintf("Hash db version mismatch (mine: %d.%d, file: %d.%d)\n",
			DB_FILE_MAJOR, DB_FILE_MINOR, cfg->major, cfg->minor);
		return EIO;
	}

	dbfile_get_dbpath(db, path);

	if (strncasecmp(cfg->hash_type, HASH_TYPE, 8)) {
		eprintf("Error: Hashfile %s uses \"%.*s\" for checksums "
			"but we are using %.*s.\nYou are probably "
			"using a hashfile generated from an old version, "
			"which cannot be read anymore.\n", path, 8,
			cfg->hash_type, 8, HASH_TYPE);
		return EINVAL;
	}

	if (cfg->blocksize != blocksize) {
		vprintf("Using blocksize %uK from hashfile (%uK "
			"blocksize requested).\n", cfg->blocksize/1024,
			blocksize/1024);
		blocksize = cfg->blocksize;
	}

	return 0;
}

/*
 * Idempotent column presence check via PRAGMA table_info.
 * SQLite has no ALTER TABLE ... ADD COLUMN IF NOT EXISTS, so we query
 * the schema directly. Returns 1 if present, 0 if absent, <0 on error.
 */
static int dbfile_column_present(sqlite3 *db, const char *table,
				 const char *column)
{
	sqlite3_stmt *stmt = NULL;
	char query[256];
	int ret, found = 0;

	snprintf(query, sizeof(query), "PRAGMA table_info(%s);", table);
	ret = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
	if (ret) {
		perror_sqlite(ret, "preparing PRAGMA table_info");
		return -1;
	}

	while ((ret = sqlite3_step(stmt)) == SQLITE_ROW) {
		const char *name = (const char *)sqlite3_column_text(stmt, 1);
		if (name && strcmp(name, column) == 0) {
			found = 1;
			break;
		}
	}

	sqlite3_finalize(stmt);

	if (ret != SQLITE_ROW && ret != SQLITE_DONE) {
		perror_sqlite(ret, "stepping PRAGMA table_info");
		return -1;
	}

	return found;
}

/*
 * Idempotent migration of the blocks table to add the columns needed
 * for the h16-based lookup pipeline:
 *
 *   srccount           - per-position cumulative reflink count, capped
 *                        at --lookup-max-reflinks during dedupe.
 *                        Default -1 means "not yet seeded"; lazy-seeded
 *                        via LOGICAL_INO_V2 on first encounter as a
 *                        canonical candidate.
 *   alias_root_fileid  - canonical (fileid, loff) for this position's
 *   alias_root_loff      physical extent. NULL means "this position is
 *                        its own root" (no prior dedupe). Set after
 *                        each successful FIDEDUPERANGE so future
 *                        candidate iteration converges to the right
 *                        srccount via path compression.
 *
 * SQLite ALTER TABLE ADD COLUMN does not support IF NOT EXISTS in the
 * versions we target, so we probe with PRAGMA table_info first.
 *
 * Runs from dbfile_prepare() on every RW open. The first run on an
 * existing hashfile applies the migration; subsequent runs are pure
 * PRAGMA checks (a few hundred microseconds total).
 */
static int dbfile_migrate_blocks_columns(sqlite3 *db)
{
	int ret;
	int present;

	present = dbfile_column_present(db, "blocks", "srccount");
	if (present < 0)
		return EIO;
	if (!present) {
		ret = sqlite3_exec(db,
			"ALTER TABLE blocks ADD COLUMN "
			"srccount INTEGER NOT NULL DEFAULT -1;",
			NULL, NULL, NULL);
		if (ret) {
			perror_sqlite(ret, "adding blocks.srccount column");
			return ret;
		}
	}

	present = dbfile_column_present(db, "blocks", "alias_root_fileid");
	if (present < 0)
		return EIO;
	if (!present) {
		ret = sqlite3_exec(db,
			"ALTER TABLE blocks ADD COLUMN "
			"alias_root_fileid INTEGER;",
			NULL, NULL, NULL);
		if (ret) {
			perror_sqlite(ret, "adding blocks.alias_root_fileid column");
			return ret;
		}
	}

	present = dbfile_column_present(db, "blocks", "alias_root_loff");
	if (present < 0)
		return EIO;
	if (!present) {
		ret = sqlite3_exec(db,
			"ALTER TABLE blocks ADD COLUMN "
			"alias_root_loff INTEGER;",
			NULL, NULL, NULL);
		if (ret) {
			perror_sqlite(ret, "adding blocks.alias_root_loff column");
			return ret;
		}
	}

	/*
	 * srccount_gen: per-row generation tag for the srccount value.
	 * Phase 5 records the global "current generation" here when it
	 * fires. The outer-skip rule in lookup_dedupe treats a row as
	 * stale (srccount value not trustworthy) if srccount_gen <
	 * current generation. A --bump-srccount-gen invalidates all
	 * existing srccount values O(1) without rewriting the table.
	 *
	 * Default 0 matches the initial value of the global
	 * "srccount_gen" config row (also defaulting to 0 when absent),
	 * so before any --bump, ALL rows are "fresh at gen 0" - the
	 * stale branch of the skip rule never fires on an unbumped db.
	 */
	present = dbfile_column_present(db, "blocks", "srccount_gen");
	if (present < 0)
		return EIO;
	if (!present) {
		ret = sqlite3_exec(db,
			"ALTER TABLE blocks ADD COLUMN "
			"srccount_gen INTEGER NOT NULL DEFAULT 0;",
			NULL, NULL, NULL);
		if (ret) {
			perror_sqlite(ret, "adding blocks.srccount_gen column");
			return ret;
		}
	}

	return 0;
}

static int create_tables(sqlite3 *db)
{
	int ret;

#define CREATE_TABLE_CONFIG						\
"CREATE TABLE IF NOT EXISTS config(keyname TEXT PRIMARY KEY NOT NULL, "	\
"keyval BLOB, UNIQUE(keyname));"
	ret = sqlite3_exec(db, CREATE_TABLE_CONFIG, NULL, NULL, NULL);
	if (ret)
		goto out;

#define	CREATE_TABLE_FILES						\
"CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY NOT NULL, "	\
"filename TEXT NOT NULL, "						\
"ino INTEGER, subvol INTEGER, size INTEGER, "				\
"mtime INTEGER, dedupe_seq INTEGER, digest BLOB, "			\
"flags INTEGER, UNIQUE(ino, subvol), UNIQUE(filename));"
	ret = sqlite3_exec(db, CREATE_TABLE_FILES, NULL, NULL, NULL);
	if (ret)
		goto out;

/*
 * The UNIQUE(fileid, loff, len) constraint was previously here. It
 * forced SQLite to maintain an implicit autoindex on (fileid, loff,
 * len), which becomes a random-key index at insertion time once the
 * extent array is sorted by digest before INSERT (the sort eliminates
 * the digest-index thrash but inadvertently scrambles loff order
 * inside the autoindex). At scale that random autoindex was the
 * dominant SQLite cache-thrash source. We drop the UNIQUE constraint
 * here: the application never re-inserts the same (fileid, loff, len)
 * row in a single scan, and find_dupes does not depend on uniqueness
 * for correctness.
 */
#define	CREATE_TABLE_EXTENTS						\
"CREATE TABLE IF NOT EXISTS extents(digest BLOB KEY NOT NULL, "		\
"fileid INTEGER, loff INTEGER, poff INTEGER, len INTEGER, "		\
"FOREIGN KEY(fileid) REFERENCES files(id) ON DELETE CASCADE);"
	ret = sqlite3_exec(db, CREATE_TABLE_EXTENTS, NULL, NULL, NULL);
	if (ret)
		goto out;

/*
 * The UNIQUE(fileid, loff) constraint was previously here. See the
 * matching comment above CREATE_TABLE_EXTENTS for the rationale; the
 * same random-autoindex cache thrash applied to the blocks table
 * after sort-by-digest. The application never re-inserts the same
 * (fileid, loff) row in a single scan, and no query in the codebase
 * depends on the uniqueness for correctness.
 */
#define	CREATE_TABLE_BLOCKS						\
"CREATE TABLE IF NOT EXISTS blocks(digest BLOB KEY NOT NULL, "		\
"fileid INTEGER, loff INTEGER, "					\
"FOREIGN KEY(fileid) REFERENCES files(id) ON DELETE CASCADE);"
	ret = sqlite3_exec(db, CREATE_TABLE_BLOCKS, NULL, NULL, NULL);
	if (ret)
		goto out;

/*
 * blocks_h16: secondary index for fast h16-based candidate lookups
 * in --lookup-only mode. Each row represents a 16 KB rolling window
 * starting at (fileid, loff); h16 is XXH128 of the four consecutive
 * 4 KB block digests at loff, loff+blocksize, loff+2*blocksize, and
 * loff+3*blocksize.
 *
 * The h16 hash guarantees a 16 KB byte match between any two positions
 * sharing the same h16 value (modulo cryptographic XXH128 collision,
 * negligible at any realistic scale). This eliminates the wasted
 * extend_match cycles seen with the old 4 KB digest lookup, where most
 * candidates extended only a few KB before failing the min_dedupe_size
 * check.
 *
 * Built once via --build-h16-index after the regular Phase 1 scan.
 */
#define CREATE_TABLE_BLOCKS_H16						\
"CREATE TABLE IF NOT EXISTS blocks_h16(h16 BLOB NOT NULL, "		\
"fileid INTEGER NOT NULL, loff INTEGER NOT NULL, "			\
"FOREIGN KEY(fileid) REFERENCES files(id) ON DELETE CASCADE);"
	ret = sqlite3_exec(db, CREATE_TABLE_BLOCKS_H16, NULL, NULL, NULL);
	if (ret)
		goto out;

/*
 * blocks_h16_build_progress: per-fileid completion tracking for the
 * h16 build. Each row records that all h16 entries for the given
 * fileid have been inserted and committed. Allows the build to resume
 * after a crash without re-processing already-completed files.
 *
 * Populated atomically with the per-fileid INSERT batch inside one
 * transaction, so a row is present iff the fileid's h16 entries are
 * durable in the WAL.
 */
#define CREATE_TABLE_BLOCKS_H16_PROGRESS				\
"CREATE TABLE IF NOT EXISTS blocks_h16_build_progress("			\
"fileid INTEGER PRIMARY KEY, "						\
"completed_at INTEGER NOT NULL, "					\
"FOREIGN KEY(fileid) REFERENCES files(id) ON DELETE CASCADE);"
	ret = sqlite3_exec(db, CREATE_TABLE_BLOCKS_H16_PROGRESS,
			   NULL, NULL, NULL);
	if (ret)
		goto out;

out:
	if (ret)
		perror_sqlite(ret, "creating database tables");

	return ret;
}

static int create_indexes(sqlite3 *db)
{
	int ret = 0;

#define CREATE_BLOCKS_DIGEST_INDEX					\
"create index if not exists idx_blocks_digest on blocks(digest);"
	if (!dbfile_skip_digest_index_creation) {
		ret = sqlite3_exec(db, CREATE_BLOCKS_DIGEST_INDEX,
				   NULL, NULL, NULL);
		if (ret)
			goto out;
	}

#define CREATE_BLOCKS_FILEID_INDEX					\
"create index if not exists idx_blocks_fileid on blocks(fileid);"
	ret = sqlite3_exec(db, CREATE_BLOCKS_FILEID_INDEX, NULL, NULL, NULL);
	if (ret)
		goto out;

	/*
	 * idx_blocks_fileid_loff is the composite point-lookup index for the
	 * Phase-4 (fileid, loff) selects in --lookup-only (resolve_canonical /
	 * srccount). A fileid-only index makes those O(blocks-per-file) on
	 * large images, so the (fileid, loff) prefix is required for lookup
	 * performance. The trailing digest makes it a COVERING index for the
	 * h16 build's "SELECT loff, digest ... WHERE fileid=? ORDER BY loff"
	 * (index-only scan: no external sort, no per-row table reads).
	 * Unlike idx_blocks_fileid it thrashes during the scan
	 * exactly like the digest index (loff is scrambled by the pre-INSERT
	 * digest sort), so it rides the same bulk-load skip flag: skipped here
	 * during the scan, dropped by dbfile_drop_bulk_load_indexes(), and
	 * rebuilt in one sorted pass by dbfile_create_bulk_load_indexes()
	 * after scan_files() completes.
	 */
#define CREATE_BLOCKS_FILEID_LOFF_INDEX \
"create index if not exists idx_blocks_fileid_loff on blocks(fileid, loff, digest);"
	if (!dbfile_skip_digest_index_creation) {
		ret = sqlite3_exec(db, CREATE_BLOCKS_FILEID_LOFF_INDEX,
				   NULL, NULL, NULL);
		if (ret)
			goto out;
	}

#define CREATE_EXTENTS_DIGEST_LEN_INDEX					\
"create index if not exists idx_extents_digest_len on extents(digest, len);"
	if (!dbfile_skip_digest_index_creation) {
		ret = sqlite3_exec(db, CREATE_EXTENTS_DIGEST_LEN_INDEX,
				   NULL, NULL, NULL);
		if (ret)
			goto out;
	}

#define CREATE_EXTENTS_FILEID_INDEX					\
"create index if not exists idx_extents_fileid on extents(fileid);"
	ret = sqlite3_exec(db, CREATE_EXTENTS_FILEID_INDEX, NULL, NULL, NULL);
	if (ret)
		goto out;

#define CREATE_FILES_INO_SUBVOL_INDEX					\
"create index if not exists idx_files_ino_subvol on files(ino, subvol);"
	ret = sqlite3_exec(db, CREATE_FILES_INO_SUBVOL_INDEX, NULL, NULL, NULL);
	if (ret)
		goto out;

#define CREATE_FILES_DEDUPESEQ_INDEX					\
"create index if not exists idx_files_dedupeseq on files(dedupe_seq);"
	ret = sqlite3_exec(db, CREATE_FILES_DEDUPESEQ_INDEX, NULL, NULL, NULL);
	if (ret)
		goto out;

#define CREATE_FILES_DIGEST_SIZE_INDEX					\
"create index if not exists idx_files_digest_size on files(digest, size);"
	ret = sqlite3_exec(db, CREATE_FILES_DIGEST_SIZE_INDEX, NULL, NULL, NULL);
	if (ret)
		goto out;

	/*
	 * idx_blocks_h16 is the covering lookup index for --lookup-only
	 * h16 queries. (h16, fileid, loff) order means a `WHERE h16 = ?`
	 * lookup is an index-only scan returning candidates in (fileid,
	 * loff) order without any blocks_h16 table reads.
	 *
	 * Created here so the index is present on every open of an
	 * already-migrated hashfile. The actual blocks_h16 rows are
	 * populated by --build-h16-index, which is a separate one-shot
	 * operation; this CREATE INDEX is a no-op on an empty table.
	 */
#define CREATE_BLOCKS_H16_INDEX						\
"create index if not exists idx_blocks_h16 on blocks_h16(h16, fileid, loff);"
	ret = sqlite3_exec(db, CREATE_BLOCKS_H16_INDEX, NULL, NULL, NULL);
	if (ret)
		goto out;

out:
	if (ret)
		perror_sqlite(ret, "creating database index");
	return ret;
}

/*
 * Standard SQLite bulk-load pattern, applied selectively. Only the
 * digest indexes (idx_blocks_digest, idx_extents_digest_len) are
 * dropped before the hash phase, because those are the ones that
 * thrash: digests are uniformly random, so each INSERT lands on a
 * random leaf page; once the digest index exceeds the page cache,
 * every insert becomes random I/O on the hashfile and throughput
 * collapses.
 *
 * The fileid indexes (idx_blocks_fileid, idx_extents_fileid) are
 * deliberately KEPT. They are required during the scan phase by:
 *   - REMOVE_BLOCK_HASHES / REMOVE_EXTENT_HASHES, which run from
 *     dbfile_remove_hashes() in __scan_file() for every file that
 *     already has prior hashes (the rescan-existing-file path).
 *     Without these indexes those DELETEs become full table scans.
 *   - The FK ON DELETE CASCADE from files -> blocks/extents fired
 *     by dbfile_prune_unscanned_files().
 * Fileid inserts during scan don't thrash because within a single
 * file's commit every row shares the same fileid, so all writes
 * land on one leaf page of the fileid index.
 *
 * The digest indexes are rebuilt from a sorted table scan in a
 * single pass after scan_files() completes. --lookup-only verifies
 * idx_blocks_digest is present at open time, so the rebuild must
 * succeed before the hashfile is considered written.
 */
int dbfile_drop_bulk_load_indexes(sqlite3 *db)
{
	int ret;

	/*
	 * Set the flag before issuing the DROPs so any worker thread
	 * that races our drop by opening its connection concurrently
	 * will skip digest-index creation in create_indexes(). Worker
	 * threads have not been spawned yet at the point this is
	 * called from main(), but the ordering keeps the invariant
	 * "flag set => digest indexes intentionally absent" simple to
	 * reason about during error recovery.
	 */
	dbfile_skip_digest_index_creation = true;

	ret = sqlite3_exec(db, "drop index if exists idx_blocks_digest;",
			   NULL, NULL, NULL);
	if (ret)
		goto out;

	ret = sqlite3_exec(db, "drop index if exists idx_blocks_fileid_loff;",
			   NULL, NULL, NULL);
	if (ret)
		goto out;

	ret = sqlite3_exec(db, "drop index if exists idx_extents_digest_len;",
			   NULL, NULL, NULL);
out:
	if (ret)
		perror_sqlite(ret, "dropping bulk-load indexes");
	return ret;
}

int dbfile_create_bulk_load_indexes(sqlite3 *db)
{
	int ret;

	ret = sqlite3_exec(db, CREATE_BLOCKS_DIGEST_INDEX, NULL, NULL, NULL);
	if (ret)
		goto out;

	ret = sqlite3_exec(db, CREATE_BLOCKS_FILEID_LOFF_INDEX, NULL, NULL, NULL);
	if (ret)
		goto out;

	ret = sqlite3_exec(db, CREATE_EXTENTS_DIGEST_LEN_INDEX,
			   NULL, NULL, NULL);
out:
	if (ret) {
		perror_sqlite(ret, "creating bulk-load indexes");
		/*
		 * Leave dbfile_skip_digest_index_creation set on
		 * failure: the digest indexes did not get rebuilt, so
		 * any subsequent worker connection that opens in this
		 * same process should still NOT recreate them
		 * (CREATE INDEX on a populated table is the very cost
		 * we are trying to avoid). The hashfile is in an
		 * incomplete state and will be repaired by the next
		 * full run's dbfile_prepare() -> create_indexes()
		 * call, which starts with the flag clear.
		 */
		return ret;
	}

	dbfile_skip_digest_index_creation = false;
	return 0;
}

static int dbfile_set_modes(sqlite3 *db)
{
	int ret;

	ret = sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "configuring database (sync pragma)");
		return ret;
	}

	ret = sqlite3_exec(db, "PRAGMA journal_mode = WAL", NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "configuring database (journal mode)");
		return ret;
	}

	/*
	 * 4 GiB SQLite page cache. The previous 256 MiB default was
	 * smaller than the per-file index working set at our scale
	 * (1 TB file -> ~8 GB digest index, ~2 GB even for 250 GB),
	 * which produced severe cache thrash and runaway WAL growth
	 * during the per-file commit. Kept modest (512 MiB) on purpose:
	 * the per-file block-digest array is sized to the whole file
	 * (~24 B/block => ~5.4 GiB for a ~1 TB image), so a large SQLite
	 * cache stacked on top of that array (times --io-threads) is what
	 * OOM-kills the build on multi-hundred-GB files. 512 MiB just spills
	 * the commit's working set to the WAL (paced by vm.dirty_bytes)
	 * instead of pinning it in anonymous RAM.
	 */
	ret = sqlite3_exec(db, "PRAGMA cache_size = -524288", NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "configuring database (cache size)");
		return ret;
	}

	ret = sqlite3_exec(db, "PRAGMA foreign_keys = ON", NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "enabling foreign keys");
		return ret;
	}

	return ret;
}

static int dbfile_prepare(sqlite3 *db)
{
	struct dbfile_config cfg;
	int ret;
	char dbpath[PATH_MAX + 1];

	ret = create_tables(db);
	if (ret) {
		perror_sqlite(ret, "creating tables");
		return ret;
	}

	ret = create_indexes(db);
	if (ret) {
		perror_sqlite(ret, "creating indexes");
		return ret;
	}

	/*
	 * Add the h16-pipeline columns to blocks (srccount,
	 * alias_root_fileid, alias_root_loff) if not already present.
	 * Idempotent: subsequent opens hit only the PRAGMA table_info
	 * probe. First open on an existing hashfile runs the actual
	 * ALTER TABLE statements.
	 */
	ret = dbfile_migrate_blocks_columns(db);
	if (ret) {
		perror_sqlite(ret, "migrating blocks columns");
		return ret;
	}

	ret = dbfile_get_dbpath(db, dbpath);
	if (ret)
		return ret;

	if (strcmp("(null)", dbpath) != 0) {
		ret = chmod(dbpath, S_IRUSR|S_IWUSR);
		if (ret) {
			perror("setting db file permissions");
			return ret;
		}
	}

	ret = dbfile_get_config(db, &cfg);
	if (ret) {
		perror_sqlite(ret, "reading initial db config");
		return ret;
	}

	ret = dbfile_check(db, &cfg);
	if (ret && strcmp("(null)", dbpath) != 0) {
		eprintf("Recreating hashfile ..\n");
		sqlite3_close(db);
		ret = unlink(dbpath);
		if ( ret && errno != ENOENT) {
			ret = errno;
			eprintf("Error %d while unlinking old "
				"db file \"%s\" : %s\n", ret, dbpath,
				strerror(ret));
			return ret;
		}

		db = __dbfile_open_handle(dbpath, false);
		return dbfile_prepare(db);
	}

	/* May store the default config, if fields were missing
	 * or if the database did not exist
	 */
	ret = __dbfile_sync_config(db, &cfg);
	if (ret) {
		perror_sqlite(ret, "sync db config");
		return ret;
	}

	return 0;
}


#define MEMDB_FILENAME		"file::memory:?cache=shared"
#define OPEN_FLAGS		(SQLITE_OPEN_READWRITE|SQLITE_OPEN_NOMUTEX|SQLITE_OPEN_URI)
#define OPEN_FLAGS_CREATE	(OPEN_FLAGS|SQLITE_OPEN_CREATE)
static sqlite3 *__dbfile_open_handle(char *filename, bool force_create)
{
	int ret;
	sqlite3 *db;

	if (!filename) {
		filename = MEMDB_FILENAME;
		force_create = true;
	}

	if (force_create)
		ret = sqlite3_open_v2(filename, &db, OPEN_FLAGS_CREATE, NULL);
	else
		ret = sqlite3_open_v2(filename, &db, OPEN_FLAGS, NULL);

	if (ret == SQLITE_CANTOPEN && !force_create) {
		vprintf("Cannot open an existing hashfile, retrying in create mode\n");
		sqlite3_close(db);
		return __dbfile_open_handle(filename, true);
	}

	if (ret) {
		perror_sqlite_open(db, filename);
		sqlite3_close(db);
		return NULL;
	}

	ret = dbfile_set_modes(db);
	if (ret) {
		sqlite3_close(db);
		return NULL;
	}

	ret = dbfile_prepare(db);
	if (ret) {
		sqlite3_close(db);
		return NULL;
	}

	return db;
}

#define dbfile_prepare_stmt(member, query) do {							\
	int ret = sqlite3_prepare_v2(result->db, query, -1, &(result->stmts.member), NULL);	\
	if (ret) {										\
		perror_sqlite(ret, "preparing stmt");						\
		goto err;									\
	}											\
} while (0)

struct dbhandle *dbfile_open_handle(char *filename)
{
	struct dbhandle *result = calloc(1, sizeof(struct dbhandle));
	result->db = __dbfile_open_handle(filename, false);

	if (!result->db)
		goto err;

#define	INSERT_BLOCK							\
"INSERT INTO blocks (fileid, loff, digest) VALUES (?1, ?2, ?3);"
	dbfile_prepare_stmt(insert_block, INSERT_BLOCK);

#define	INSERT_EXTENTS							\
"INSERT INTO extents (fileid, loff, poff, len, digest) "		\
"VALUES (?1, ?2, ?3, ?4, ?5);"
	dbfile_prepare_stmt(insert_extent, INSERT_EXTENTS);

#define UPDATE_SCANNED_FILE						\
"UPDATE files SET digest = ?1, flags = ?2 where id = ?3;"
	dbfile_prepare_stmt(update_scanned_file, UPDATE_SCANNED_FILE);

#define FIND_BLOCKS                                                     \
"select files.filename, blocks.loff from files "			\
"INNER JOIN blocks "							\
"on blocks.digest = ?1 AND files.id = blocks.fileid;"
	dbfile_prepare_stmt(find_blocks, FIND_BLOCKS);

#define FIND_TOP_B_HASHES						\
"select digest, count(digest) from blocks "				\
"group by digest having (count(digest) > 1) "				\
"order by (count(digest)) desc;"
	dbfile_prepare_stmt(find_top_b_hashes, FIND_TOP_B_HASHES);

#define FIND_TOP_E_HASHES						\
"select digest, count(digest) from extents "				\
"group by digest having (count(digest) > 1) "				\
"order by (count(digest)) desc;"
	dbfile_prepare_stmt(find_top_e_hashes, FIND_TOP_E_HASHES);

#define FIND_B_FILES_COUNT						\
"select count (distinct files.filename) from files "			\
"INNER JOIN blocks "							\
"on blocks.digest = ?1 AND files.id = blocks.fileid;"
	dbfile_prepare_stmt(find_b_files_count, FIND_B_FILES_COUNT);

#define FIND_E_FILES_COUNT						\
"select count (distinct files.filename) from files "			\
"INNER JOIN extents on extents.digest = ?1 "				\
"AND files.id = extents.fileid;"
	dbfile_prepare_stmt(find_e_files_count, FIND_E_FILES_COUNT);

#define	UPDATE_EXTENT_POFF						\
"update extents set poff = ?1 "						\
"where fileid = ?2 and loff = ?3;"
	dbfile_prepare_stmt(update_extent_poff, UPDATE_EXTENT_POFF);

#define	WRITE_FILE							\
"insert or replace into files (ino, subvol, filename, size, mtime, "	\
"dedupe_seq) VALUES (?1, ?2, ?3, ?4, ?5, ?6);"
	dbfile_prepare_stmt(write_file, WRITE_FILE);

#define REMOVE_BLOCK_HASHES						\
"delete from blocks where fileid = ?1;"
	dbfile_prepare_stmt(remove_block_hashes, REMOVE_BLOCK_HASHES);

#define REMOVE_EXTENT_HASHES						\
"delete from extents where fileid = ?1;"
	dbfile_prepare_stmt(remove_extent_hashes, REMOVE_EXTENT_HASHES);

#define LOAD_FILEREC							\
"select filename, size from files where id = ?1;"
	dbfile_prepare_stmt(load_filerec, LOAD_FILEREC);

#define GET_DUPLICATE_BLOCKS						\
"select blocks.digest, fileid, loff from blocks "			\
"join files on fileid = id "						\
"where dedupe_seq <= ?1 and blocks.digest in ( "			\
"	select blocks.digest from blocks "				\
"	join files on fileid = id "					\
"	where dedupe_seq <= ?1 and blocks.digest in ( "			\
"		select blocks.digest from blocks "			\
"		join files on fileid = id "				\
"		where dedupe_seq = ?1) "				\
"	group by blocks.digest having count(*) > 1);"
	dbfile_prepare_stmt(get_duplicate_blocks, GET_DUPLICATE_BLOCKS);

/*
 * We need to select on both digest and len, otherwise we
 * could run into a situation where a single extent with a
 * colliding hash but different length gets placed into the
 * results tree, which will get very angry when it has a
 * result of only one extent.
 */
#define GET_DUPLICATE_EXTENTS						\
"select extents.digest, fileid, loff, len, poff from extents "		\
"join files on fileid = id "						\
"where dedupe_seq <= ?1 and (extents.digest, len) in ( "		\
"	select extents.digest, len from extents "			\
"	join files on fileid = id "					\
"	where dedupe_seq <= ?1 and (extents.digest, len) in ( "		\
"		select extents.digest, len from extents "		\
"		join files on fileid = id "				\
"		where dedupe_seq = ?1) "				\
"	group by extents.digest, len having count(*) > 1);"
	dbfile_prepare_stmt(get_duplicate_extents, GET_DUPLICATE_EXTENTS);

/*
 * Select duplicates, excluding future files.
 * Then, only keep duplicates if at least one entry lives is related
 * to the current batch.
 */
#define GET_DUPLICATE_FILES							\
"select id, size, digest, filename from files "					\
"where dedupe_seq <= ?1 and not (flags & 1) and (digest, size) in ( "		\
"	select digest, size from files "					\
"	where dedupe_seq <= ?1 and not (flags & 1) and (digest, size) in ( "	\
"		select digest, size from files "				\
"		where dedupe_seq = ?1 and not (flags & 1)) "			\
"	group by digest, size having count(*) > 1);"
	dbfile_prepare_stmt(get_duplicate_files, GET_DUPLICATE_FILES);

#define GET_FILE_EXTENT							\
"select poff, loff, len from extents "					\
"join files on files.id = extents.fileid "				\
"where fileid = ?1 and loff <= ?2 and (loff + len) > ?2;"
	dbfile_prepare_stmt(get_file_extent, GET_FILE_EXTENT);

#define GET_NONDUPE_EXTENTS						\
"select extents.loff, len, poff "					\
"FROM extents join files on files.id = extents.fileid "			\
"where files.id = ?1 and "						\
"(1 = (SELECT COUNT(*) FROM extents as e where e.digest = extents.digest));"
	dbfile_prepare_stmt(get_nondupe_extents, GET_NONDUPE_EXTENTS);

#define DELETE_FILE "delete from files where filename = ?1;"
	dbfile_prepare_stmt(delete_file, DELETE_FILE);

#define SELECT_FILE_CHANGES						\
"select mtime, size, filename, id from files where ino = ?1 and subvol = ?2;"
	dbfile_prepare_stmt(select_file_changes, SELECT_FILE_CHANGES);

#define COUNT_B_HASHES "select COUNT(*) from blocks;"
	dbfile_prepare_stmt(count_b_hashes, COUNT_B_HASHES);

#define COUNT_E_HASHES "select COUNT(*) from extents;"
	dbfile_prepare_stmt(count_e_hashes, COUNT_E_HASHES);

#define COUNT_FILES "select COUNT(*) from files;"
	dbfile_prepare_stmt(count_files, COUNT_FILES);

#define GET_MAX_DEDUPE_SEQ "select max(dedupe_seq) from files;"
	dbfile_prepare_stmt(get_max_dedupe_seq, GET_MAX_DEDUPE_SEQ);

#define DELETE_UNSCANNED_FILES "delete from files where digest is NULL;"
	dbfile_prepare_stmt(delete_unscanned_files, DELETE_UNSCANNED_FILES);

#define RENAME_FILE							\
"update or replace files set filename = ?1 where id = ?2;"
	dbfile_prepare_stmt(rename_file, RENAME_FILE);
	return result;

err:
	dbfile_close_handle(result);
	return NULL;
}

/*
 * Return 0 if an index with the given name exists in the database,
 * ENOENT if not, or an sqlite error code on failure.
 */
static int dbfile_check_index(sqlite3 *db, const char *name)
{
	_cleanup_(sqlite3_stmt_cleanup) sqlite3_stmt *stmt = NULL;
	int ret;

	ret = sqlite3_prepare_v2(db,
		"select 1 from sqlite_master where type='index' and name=?1;",
		-1, &stmt, NULL);
	if (ret) {
		perror_sqlite(ret, "preparing index-check statement");
		return ret;
	}

	ret = sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
	if (ret) {
		perror_sqlite(ret, "binding index-check name");
		return ret;
	}

	ret = sqlite3_step(stmt);
	if (ret == SQLITE_ROW)
		return 0;
	if (ret == SQLITE_DONE)
		return ENOENT;

	perror_sqlite(ret, "stepping index-check statement");
	return ret;
}

/*
 * Open an existing hashfile strictly read-only for streaming
 * --lookup-only mode. Unlike dbfile_open_handle() this path performs
 * NO writes: no CREATE TABLE / CREATE INDEX, no chmod, no PRAGMA
 * journal_mode change, no sync_config. It also prepares only the
 * SELECT statements lookup mode needs (select_file_changes and
 * load_filerec). All write-side prepared statement slots remain NULL,
 * which dbfile_close_handle() handles safely.
 *
 * The database layer enforces the read-only guarantee: any accidental
 * INSERT / UPDATE / DELETE would fail at sqlite3_step() with
 * SQLITE_READONLY, not silently mutate the hashfile.
 */
struct dbhandle *dbfile_open_handle_readonly(char *filename)
{
	struct dbhandle *result;
	struct dbfile_config cfg;
	int ret;

	if (filename == NULL)
		return NULL;

	result = calloc(1, sizeof(struct dbhandle));
	if (result == NULL)
		return NULL;

	/*
	 * Despite the historical "readonly" name, --lookup-only now
	 * requires write access to maintain the srccount and
	 * alias_root_* columns introduced in Phase 4. The hashfile
	 * structure (blocks, blocks_h16, files, etc.) is still never
	 * modified - only the per-position bookkeeping columns are
	 * updated, and only via the specific prepared statements set
	 * up below. No INSERT/DELETE on the bulk data tables ever
	 * runs through this path.
	 */
	ret = sqlite3_open_v2(filename, &result->db,
			      SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX |
			      SQLITE_OPEN_URI, NULL);
	if (ret) {
		perror_sqlite_open(result->db, filename);
		sqlite3_close(result->db);
		free(result);
		return NULL;
	}

	/*
	 * --lookup-only now opens RW for srccount/alias_root
	 * maintenance, so synchronous = OFF here matches the
	 * full-RW path's tradeoff (small risk of losing the most
	 * recent srccount UPDATEs on power loss; the lazy
	 * LOGICAL_INO_V2 seed in Phase 5 catches the drift on the
	 * next run). WAL mode is already a property of the file
	 * from prior RW opens, so we do not need to re-establish it
	 * here.
	 */
	ret = sqlite3_exec(result->db, "PRAGMA synchronous = OFF",
			   NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "configuring database (synchronous)");
		goto err;
	}

	ret = sqlite3_exec(result->db, "PRAGMA cache_size = -256000",
			   NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret, "configuring database (cache size)");
		goto err;
	}

	/*
	 * Memory-map the hashfile. SQLite reads then come via
	 * demand-loaded memory accesses (no pread syscall, no copy
	 * from page cache to SQLite cache) which is a measurable win
	 * for the random-access pattern of idx_blocks_digest lookups
	 * during --lookup-only. 300 GB is sized to cover the whole
	 * post-h16 hashfile (~200 GB and growing); a too-small mmap_size
	 * leaves the tail of the file on the slower pread+copy path.
	 * On x64 Linux this only reserves ADDRESS SPACE, it does not pin
	 * physical RAM - the kernel pages in on demand and evicts clean
	 * file-backed pages first under pressure, so it cannot OOM and
	 * does not compete with the anonymous skip-caches for RSS.
	 *
	 * Non-fatal on failure: SQLite transparently falls back to
	 * pread per-file if mmap is unavailable or fails. We log the
	 * failure for diagnosis but don't abort the open.
	 */
	ret = sqlite3_exec(result->db,
			   "PRAGMA mmap_size = 300000000000",
			   NULL, NULL, NULL);
	if (ret) {
		perror_sqlite(ret,
			      "configuring database (mmap_size) - "
			      "continuing without mmap");
		ret = 0;	/* clear so we don't trip later checks */
	}

	ret = dbfile_get_config(result->db, &cfg);
	if (ret) {
		perror_sqlite(ret, "reading hashfile config");
		goto err;
	}

	if (cfg.major != DB_FILE_MAJOR || cfg.minor != DB_FILE_MINOR) {
		eprintf("Hashfile version mismatch (mine: %d.%d, file: %d.%d)\n",
			DB_FILE_MAJOR, DB_FILE_MINOR, cfg.major, cfg.minor);
		goto err;
	}

	if (strncasecmp(cfg.hash_type, HASH_TYPE, 8)) {
		eprintf("Hashfile uses hash \"%.*s\" but this build uses "
			"\"%.*s\".\n", 8, cfg.hash_type, 8, HASH_TYPE);
		goto err;
	}

	/*
	 * Run blocks-column migration even on the "readonly" open.
	 * The path is actually RW (see comment above) and the
	 * migration is idempotent - it ALTER TABLE ADD COLUMNs only
	 * when the column isn't already present. Without this, a
	 * lookup-only run against a hashfile that hasn't been opened
	 * with the new binary's full-RW path would be missing the
	 * srccount_gen column (added by this version's migration)
	 * and every SELECT would fail. Cost on a hashfile that
	 * already has the columns is one PRAGMA table_info per
	 * column (~µs total).
	 */
	ret = dbfile_migrate_blocks_columns(result->db);
	if (ret) {
		perror_sqlite(ret,
			      "migrating blocks columns (readonly open)");
		goto err;
	}

	/*
	 * --lookup-only now matches at 16 KB-window granularity via
	 * the blocks_h16 secondary index. Without idx_blocks_h16
	 * every h16 lookup would be a full-table scan of blocks_h16,
	 * which is the same row count as blocks. Refuse rather than
	 * silently performing pathologically.
	 */
	ret = dbfile_check_index(result->db, "idx_blocks_h16");
	if (ret == ENOENT) {
		eprintf("Hashfile is missing index 'idx_blocks_h16'. "
			"Build it with: duperemove --build-h16-index "
			"--hashfile=<hashfile>\n");
		goto err;
	}
	if (ret) {
		eprintf("Error checking hashfile indexes.\n");
		goto err;
	}

	/*
	 * --lookup-only matches at 16 KB rolling-window granularity
	 * against blocks_h16. That table is populated by
	 * --build-h16-index, which itself requires the underlying
	 * per-block hashes in `blocks` (populated by Phase 1 scan
	 * with --dedupe-options=partial). A non-empty blocks_h16 is
	 * the sufficient runtime precondition; if blocks_h16 is
	 * empty the hashfile either has not been h16-indexed yet or
	 * the Phase 1 blocks population was missing.
	 */
	{
		_cleanup_(sqlite3_stmt_cleanup) sqlite3_stmt *bstmt = NULL;
		int step;

		ret = sqlite3_prepare_v2(result->db,
			"select 1 from blocks_h16 limit 1;",
			-1, &bstmt, NULL);
		if (ret) {
			perror_sqlite(ret,
				"preparing blocks_h16-population check");
			goto err;
		}
		step = sqlite3_step(bstmt);
		if (step == SQLITE_DONE) {
			eprintf("Error: hashfile contains no h16 "
				"entries. --lookup-only matches via the "
				"blocks_h16 secondary index, which must "
				"first be populated by:\n"
				"  duperemove --build-h16-index "
				"--hashfile=<hashfile>\n"
				"(The underlying blocks table also needs "
				"per-block hashes - rebuild the hashfile "
				"with --dedupe-options=partial -b %u if "
				"it does not yet have them.)\n",
				blocksize);
			goto err;
		}
		if (step != SQLITE_ROW) {
			perror_sqlite(step,
				      "stepping blocks_h16-population check");
			goto err;
		}
	}

	/* Prepare only the SELECT statements lookup mode needs. */
	ret = sqlite3_prepare_v2(result->db,
		"select mtime, size, filename, id from files "
		"where ino = ?1 and subvol = ?2;",
		-1, &(result->stmts.select_file_changes), NULL);
	if (ret) {
		perror_sqlite(ret, "preparing select_file_changes");
		goto err;
	}

	ret = sqlite3_prepare_v2(result->db,
		"select filename, size from files where id = ?1;",
		-1, &(result->stmts.load_filerec), NULL);
	if (ret) {
		perror_sqlite(ret, "preparing load_filerec");
		goto err;
	}

	return result;

err:
	dbfile_close_handle(result);
	return NULL;
}

void dbfile_close_handle(struct dbhandle *db)
{
	if(db) {
		/* struct stmts is a named array of sqlite3_stmt*
		 * let's iterate over all unnamed elements and
		 * finalize each of them
		 */
		sqlite3_stmt **stmts = (sqlite3_stmt**)&(db->stmts);

		int len = sizeof(struct stmts) / sizeof(sqlite3_stmt*);
		for (int i = 0; i < len; i++) {
			sqlite3_finalize(stmts[i]);
		}

		sqlite3_close(db->db);
		free(db);
	}
}

/*
 * dbfile_close_handle takes struct dbhandle*.
 * we need a function that takes void* so we
 * can pass it to register_cleanup without
 * causing UB.
 */
static void cleanup_dbhandle(void *db)
{
	dbfile_close_handle(db);
}

struct dbhandle *dbfile_open_handle_thread(char *filename, struct threads_pool *pool)
{
	struct dbhandle *db;
	dbfile_lock();
	db = dbfile_open_handle(options.hashfile);
	dbfile_unlock();

	if (db)
		register_cleanup(pool, (void*)&cleanup_dbhandle, db);
	return db;
}

uint64_t count_file_by_digest(struct dbhandle *db, unsigned char *digest,
				bool show_block_hashes)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt;

	if (show_block_hashes)
		stmt = db->stmts.find_b_files_count;
	else
		stmt = db->stmts.find_e_files_count;

	ret = sqlite3_bind_blob(stmt, 1, digest, DIGEST_LEN, SQLITE_STATIC);
	if (ret) {
		eprintf("Error %d binding digest: %s\n", ret,
			sqlite3_errstr(ret));
		return 0;
	}

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_ROW && ret != SQLITE_DONE) {
		eprintf("error %d, file count search: %s\n",
			ret, sqlite3_errstr(ret));
		return 0;
	}

	return sqlite3_column_int64(stmt, 0);
}

int dbfile_begin_trans(sqlite3 *db)
{
	int ret;

	ret = sqlite3_exec(db, "begin transaction", NULL, NULL, NULL);
	if (ret)
		perror_sqlite(ret, "starting transaction");
	return ret;
}

int dbfile_update_extent_poff(struct dbhandle *db, int64_t fileid,
				uint64_t loff, uint64_t poff)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.update_extent_poff;

	ret = sqlite3_bind_int64(stmt, 1, poff);
	if (ret)
		goto bind_error;

	ret = sqlite3_bind_int64(stmt, 2, fileid);
	if (ret)
		goto bind_error;

	ret = sqlite3_bind_int64(stmt, 3, loff);
	if (ret)
		goto bind_error;

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "executing statement");
		return ret;
	}

	ret = 0;
bind_error:
	if (ret)
		perror_sqlite(ret, "binding values");

	return ret;
}

int dbfile_commit_trans(sqlite3 *db)
{
	int ret;

	ret = sqlite3_exec(db, "commit transaction", NULL, NULL, NULL);
	if (ret)
		perror_sqlite(ret, "committing transaction");
	return ret;
}

int dbfile_abort_trans(sqlite3 *db)
{
	int ret;

	ret = sqlite3_exec(db, "rollback transaction", NULL, NULL, NULL);
	if (ret)
		perror_sqlite(ret, "aborting transaction");
	return ret;
}

static int sync_config_text(sqlite3_stmt *stmt, const char *key, char *val,
			    int len)
{
	int ret;

	ret = sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	if (ret)
		goto out;
	ret = sqlite3_bind_text(stmt, 2, val, len, SQLITE_TRANSIENT);
	if (ret)
		goto out;
	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE)
		goto out;
	sqlite3_reset(stmt);

	ret = 0;
out:
	return ret;
}

static int sync_config_int(sqlite3_stmt *stmt, const char *key, int val)
{
	int ret;

	ret = sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	if (ret)
		goto out;
	ret = sqlite3_bind_int(stmt, 2, val);
	if (ret)
		goto out;
	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE)
		goto out;
	sqlite3_reset(stmt);

	ret = 0;
out:
	return ret;
}

int __dbfile_sync_config(sqlite3 *db, struct dbfile_config *cfg)
{
	int ret = 0;
	char uuid[37]; /* 36-bytes uuid + \0 */
	_cleanup_(sqlite3_stmt_cleanup) sqlite3_stmt *stmt = NULL;

	ret = sqlite3_prepare_v2(db,
				 "insert or replace into config VALUES (?1, ?2)", -1,
				 &stmt, NULL);
	if (ret) {
		perror_sqlite(ret, "preparing statement");
		return ret;
	}

	ret = sync_config_text(stmt, "hash_type", cfg->hash_type, 8);
	if (ret)
		goto out;

	ret = sync_config_int(stmt, "block_size", cfg->blocksize);
	if (ret)
		goto out;

	ret = sync_config_int(stmt, "dedupe_sequence", cfg->dedupe_seq);
	if (ret)
		goto out;

	ret = sync_config_int(stmt, "version_minor", cfg->minor);
	if (ret)
		goto out;

	uuid_unparse(cfg->fs_uuid, uuid);
	ret = sync_config_text(stmt, "fs_uuid", uuid, 36);
	if (ret)
		goto out;

	/*
	 * Always write version_major last so we have an easy check
	 * whether the config table was fully written.
	 */
	ret = sync_config_int(stmt, "version_major", cfg->major);
	if (ret)
		goto out;

out:
	if (ret) {
		perror_sqlite(ret, "binding");
	}

	return ret;
}

int dbfile_sync_config(struct dbhandle *db, struct dbfile_config *cfg)
{
	return __dbfile_sync_config(db->db, cfg);
}

static int __dbfile_count_rows(sqlite3_stmt *s, uint64_t *num)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = s;

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_ROW) {
		perror_sqlite(ret, "retrieving count from table (step)");
		return ret;
	}

	*num = sqlite3_column_int64(stmt, 0);
	return 0;
}

int dbfile_get_stats(struct dbhandle *db, struct dbfile_stats *stats)
{
	int ret = 0;
	ret = __dbfile_count_rows(db->stmts.count_b_hashes, &(stats->num_b_hashes));
	if (ret)
		return ret;

	ret = __dbfile_count_rows(db->stmts.count_e_hashes, &(stats->num_e_hashes));
	if (ret)
		return ret;

	ret = __dbfile_count_rows(db->stmts.count_files, &(stats->num_files));
	if (ret)
		return ret;

	return ret;
}

static int get_config_int(sqlite3_stmt *stmt, const char *name, int *val)
{
	int ret;

	if (!val)
		return 0;

	ret = sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
	if (ret) {
		perror_sqlite(ret, "retrieving row from config table (bind)");
		return ret;
	}

	while ((ret = sqlite3_step(stmt)) == SQLITE_ROW) {
		*val = sqlite3_column_int(stmt, 0);
	}

	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "retrieving row from config table (step)");
		return ret;
	}

	sqlite3_reset(stmt);

	return 0;
}

static int get_config_text(sqlite3_stmt *stmt, const char *name, char *val, int len)
{
	int ret;
	const unsigned char *local;

	if (!val)
		return 0;

	ret = sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
	if (ret) {
		perror_sqlite(ret, "retrieving row from config table (bind)");
		return ret;
	}

	while ((ret = sqlite3_step(stmt)) == SQLITE_ROW) {
		local = sqlite3_column_text(stmt, 0);
		memcpy(val, local, len);
	}

	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "retrieving row from config table (step)");
		return ret;
	}

	sqlite3_reset(stmt);

	return 0;
}

static int __dbfile_get_config(sqlite3 *db, struct dbfile_config *cfg)
{
	int ret;
	char uuid[37]; /* 36-bytes uuid + \0 */
	_cleanup_(sqlite3_stmt_cleanup) sqlite3_stmt *stmt = NULL;

#define SELECT_CONFIG "select keyval from config where keyname=?1;"
	ret = sqlite3_prepare_v2(db, SELECT_CONFIG, -1, &stmt, NULL);
	if (ret) {
		perror_sqlite(ret, "preparing statement");
		goto out;
	}

	ret = get_config_int(stmt, "block_size", (int *)&cfg->blocksize);
	if (ret)
		goto out;

	ret = get_config_text(stmt, "hash_type", cfg->hash_type, 8);
	if (ret)
		goto out;

	ret = get_config_int(stmt, "version_major", &cfg->major);
	if (ret)
		goto out;

	ret = get_config_int(stmt, "version_minor", &cfg->minor);
	if (ret)
		goto out;

	ret = get_config_int(stmt, "dedupe_sequence", (int *)&cfg->dedupe_seq);
	if (ret)
		goto out;

	ret = get_config_text(stmt, "fs_uuid", uuid, 36);
	if (ret)
		goto out;

	uuid_parse(uuid, cfg->fs_uuid);

out:
	if (ret != 0)
		perror_sqlite(ret, "__dbfile_get_config");
	return ret;
}

int dbfile_get_config(sqlite3 *db, struct dbfile_config *cfg)
{
	dbfile_config_defaults(cfg);
	return __dbfile_get_config(db, cfg);
}

/* Returns 0 on error, and the inserted rowid on success */
int64_t dbfile_store_file_info(struct dbhandle *db, struct file *dbfile)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.write_file;

	ret = sqlite3_bind_int64(stmt, 1, dbfile->ino);
	if (ret)
		goto bind_error;

	ret = sqlite3_bind_int64(stmt, 2, dbfile->subvol);
	if (ret)
		goto bind_error;

	ret = sqlite3_bind_text(stmt, 3, dbfile->filename, -1, SQLITE_STATIC);
	if (ret)
		goto bind_error;

	ret = sqlite3_bind_int64(stmt, 4, dbfile->size);
	if (ret)
		goto bind_error;

	ret = sqlite3_bind_int64(stmt, 5, dbfile->mtime);
	if (ret)
		goto bind_error;

	ret = sqlite3_bind_int(stmt, 6, dbfile->dedupe_seq);
	if (ret)
		goto bind_error;

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "executing sql");
		goto out_error;
	}

	return sqlite3_last_insert_rowid(db->db);

bind_error:
	if (ret)
		perror_sqlite(ret, "binding values");
out_error:
	return 0;
}

static int __dbfile_remove_file_hashes(sqlite3_stmt *stmt, int64_t fileid)
{
	int ret;

	ret = sqlite3_bind_int64(stmt, 1, fileid);
	if (ret) {
		perror_sqlite(ret, "binding fileid");
		goto out;
	}

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "removing hashes statement");
		goto out;
	}

	ret = 0;
out:
	return ret;
}

int dbfile_remove_extent_hashes(struct dbhandle *db, int64_t fileid)
{
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.remove_extent_hashes;
	return __dbfile_remove_file_hashes(stmt, fileid);
}

int dbfile_remove_hashes(struct dbhandle *db, int64_t fileid)
{
	int ret = 0;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *b_stmt = db->stmts.remove_block_hashes;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *e_stmt = db->stmts.remove_extent_hashes;

	ret += __dbfile_remove_file_hashes(b_stmt, fileid);
	ret += __dbfile_remove_file_hashes(e_stmt, fileid);

	return ret;
}

int dbfile_store_block_hashes(struct dbhandle *db, int64_t fileid,
				uint64_t nb_hash, struct block_csum *hashes)
{
	int ret;
	uint64_t i;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.insert_block;

	for (i = 0; i < nb_hash; i++) {
		ret = sqlite3_bind_int64(stmt, 1, fileid);
		if (ret)
			goto bind_error;

		ret = sqlite3_bind_int64(stmt, 2, hashes[i].loff);
		if (ret)
			goto bind_error;

		ret = sqlite3_bind_blob(stmt, 3, hashes[i].digest, DIGEST_LEN,
					SQLITE_STATIC);
		if (ret)
			goto bind_error;

		ret = sqlite3_step(stmt);
		if (ret != SQLITE_DONE) {
			perror_sqlite(ret, "executing statement");
			goto out_error;
		}

		sqlite3_reset(stmt);
	}

	ret = 0;
bind_error:
	if (ret)
		perror_sqlite(ret, "binding values");
out_error:

	return ret;
}

int dbfile_update_scanned_file(struct dbhandle *db, int64_t fileid,
				unsigned char *digest, unsigned int flags)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.update_scanned_file;

	ret = sqlite3_bind_blob(stmt, 1, digest, DIGEST_LEN,
				SQLITE_STATIC);
	if (ret)
		goto bind_error;

	ret = sqlite3_bind_int64(stmt, 2, flags);
	if (ret)
		goto bind_error;

	ret = sqlite3_bind_int64(stmt, 3, fileid);
	if (ret)
		goto bind_error;

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "executing statement");
		goto out_error;
	}

	ret = 0;
bind_error:
	if (ret)
		perror_sqlite(ret, "binding values");
out_error:
	return ret;
}

int dbfile_store_extent_hashes(struct dbhandle *db, int64_t fileid,
				uint64_t nb_hash, struct extent_csum *hashes)
{
	int ret;
	uint64_t i;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.insert_extent;

	for (i = 0; i < nb_hash; i++) {
		/*
		 * If len == 0, then this extent was never scanned and
		 * must be skipped.
		 */
		if (hashes[i].len == 0)
			continue;

		ret = sqlite3_bind_int64(stmt, 1, fileid);
		if (ret)
			goto bind_error;

		ret = sqlite3_bind_int64(stmt, 2, hashes[i].loff);
		if (ret)
			goto bind_error;

		ret = sqlite3_bind_int64(stmt, 3, hashes[i].poff);
		if (ret)
			goto bind_error;

		ret = sqlite3_bind_int64(stmt, 4, hashes[i].len);
		if (ret)
			goto bind_error;

		ret = sqlite3_bind_blob(stmt, 5, hashes[i].digest, DIGEST_LEN,
					SQLITE_STATIC);
		if (ret)
			goto bind_error;

		ret = sqlite3_step(stmt);
		if (ret != SQLITE_DONE) {
			perror_sqlite(ret, "executing statement");
			goto out_error;
		}

		sqlite3_reset(stmt);
	}

	ret = 0;
bind_error:
	if (ret)
		perror_sqlite(ret, "binding values");
out_error:

	return ret;
}

int dbfile_load_one_filerec(struct dbhandle *db, int64_t fileid,
				   struct filerec **file)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.load_filerec;
	const unsigned char *filename;
	uint64_t size;

	*file = NULL;

	ret = sqlite3_bind_int64(stmt, 1, fileid);
	if (ret) {
		perror_sqlite(ret, "binding fileid");
		return ret;
	}

	ret = sqlite3_step(stmt);
	if (ret == SQLITE_DONE) {
		dprintf("dbfile_load_one_filerec: no file found in hashdb: fileid = %lu\n", fileid);
		return 0;
	}

	if (ret != SQLITE_ROW) {
		perror_sqlite(ret, "executing statement");
		return ret;
	}

	filename = sqlite3_column_text(stmt, 0);
	size = sqlite3_column_int64(stmt, 1);

	*file = filerec_new((const char *)filename, fileid, size);
	if (!*file)
		return ENOMEM;

	return 0;
}

int dbfile_load_block_hashes(struct dbhandle *db, struct hash_tree *hash_tree,
			     unsigned int seq)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.get_duplicate_blocks;
	uint64_t loff;
	int64_t fileid;
	unsigned char *digest;
	struct filerec *file;

	ret = sqlite3_bind_int64(stmt, 1, seq);
	if (ret) {
		perror_sqlite(ret, "binding value");
		return ret;
	}

	while ((ret = sqlite3_step(stmt)) == SQLITE_ROW) {
		digest = (unsigned char *)sqlite3_column_blob(stmt, 0);
		fileid = sqlite3_column_int64(stmt, 1);
		loff = sqlite3_column_int64(stmt, 2);

		file = filerec_find(fileid);
		if (!file) {
			ret = dbfile_load_one_filerec(db, fileid, &file);
			if (ret) {
				eprintf("Error loading filerec (%"
					PRIu64") from db\n",
					fileid);
				return ret;
			}
		}

		ret = insert_hashed_block(hash_tree, digest, file, loff);
		if (ret)
			return ENOMEM;
	}
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "looking up hash");
		return ret;
	}

	sort_file_hash_heads(hash_tree);

	return 0;
}

int dbfile_load_extent_hashes(struct dbhandle *db, struct results_tree *res,
			      unsigned int seq)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.get_duplicate_extents;
	uint64_t loff, poff, len;
	int64_t fileid;
	unsigned char *digest;
	struct filerec *file;

	ret = sqlite3_bind_int64(stmt, 1, seq);
	if (ret) {
		perror_sqlite(ret, "binding value");
		return ret;
	}

	while ((ret = sqlite3_step(stmt)) == SQLITE_ROW) {
		digest = (unsigned char *)sqlite3_column_blob(stmt, 0);
		fileid = sqlite3_column_int64(stmt, 1);
		loff = sqlite3_column_int64(stmt, 2);
		len = sqlite3_column_int64(stmt, 3);
		poff = sqlite3_column_int64(stmt, 4);

		file = filerec_find(fileid);
		if (!file) {
			ret = dbfile_load_one_filerec(db, fileid, &file);
			if (ret) {
				eprintf("Error loading filerec (%"
					PRIu64") from db\n",
					fileid);
				return ret;
			}
		}

		ret = insert_one_result(res, digest, file, loff, len, poff);
		if (ret)
			return ENOMEM;
	}
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "looking up hash");
		return ret;
	}

	return 0;
}

int dbfile_load_one_file_extent(struct dbhandle *db, struct filerec *file,
				uint64_t loff, struct file_extent *extent)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.get_file_extent;

	ret = sqlite3_bind_int64(stmt, 1, file->fileid);
	if (ret) {
		perror_sqlite(ret, "binding value");
		return ret;
	}

	ret = sqlite3_bind_int64(stmt, 2, loff);
	if (ret) {
		perror_sqlite(ret, "binding value");
		return ret;
	}

	ret = sqlite3_step(stmt);

	/* TODO: drop me when extent_dedupe_worker() is fixed */
	if (ret == SQLITE_DONE)
		return 0;

	if (ret != SQLITE_ROW) {
		perror_sqlite(ret, "retrieving extent info");
		return ret;
	}

	extent->poff = sqlite3_column_int64(stmt, 0);
	extent->loff = sqlite3_column_int64(stmt, 1);
	extent->len = sqlite3_column_int64(stmt, 2);

	return 0;
}

int dbfile_load_nondupe_file_extents(struct dbhandle *db, struct filerec *file,
				     struct file_extent **ret_extents,
				     unsigned int *num_extents)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.get_nondupe_extents;
	uint64_t count = 0, i;
	struct file_extent *extents = NULL;

	ret = sqlite3_bind_int64(stmt, 1, file->fileid);
	if (ret) {
		perror_sqlite(ret, "binding values");
		goto out;
	}

	i = 0;
	while ((ret = sqlite3_step(stmt)) == SQLITE_ROW) {
		count++;

		extents = realloc(extents, count * sizeof(struct file_extent));
		if (!extents) {
			ret = ENOMEM;
			goto out;
		}

		extents[i].loff = sqlite3_column_int64(stmt, 0);
		extents[i].len = sqlite3_column_int64(stmt, 1);
		extents[i].poff = sqlite3_column_int64(stmt, 2);

		++i;
	}

	*num_extents = count;
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "stepping nondupe extents statement");
		goto out;
	}
	ret = 0;
out:
	*ret_extents = extents;
	if (ret && extents)
		free(extents);
	return ret;
}

static int iter_cb(void *priv, int argc, char **argv,
		char **column [[maybe_unused]])
{
	iter_files_func func = priv;

	abort_on(argc != 3);
	func(argv[0], argv[1], argv[2]);
	return 0;
}

int dbfile_iter_files(struct dbhandle *db, iter_files_func func)
{
	int ret;

#define	LIST_FILES	"select filename, ino, subvol from files;"
	ret = sqlite3_exec(db->db, LIST_FILES, iter_cb, func, NULL);
	if (ret) {
		perror_sqlite(ret, "Running sql to list files.");
		return ret;
	}

	return 0;
}

int dbfile_remove_file(struct dbhandle *db, const char *filename)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.delete_file;

	dprintf("Remove file \"%s\" from the db\n", filename);

	ret = sqlite3_bind_text(stmt, 1, filename, -1, SQLITE_STATIC);
	if (ret) {
		perror_sqlite(ret, "binding filename for sql");
		return ret;
	}

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "executing sql");
		return ret;
	}

	return 0;
}

void dbfile_list_files(struct dbhandle *db, int (*callback)(void*, int, char**, char**))
{
	int ret;
	char *err;

#define LIST_FILERECS							\
"select ino, subvol, size, filename from files;"

	ret = sqlite3_exec(db->db, LIST_FILERECS, callback, NULL, &err);
	if (ret) {
		eprintf("error %d, executing file search: %s\n", ret,
			err);
		return;
	}
	return;
}

/* Check if the data in the hashfile is in synced with the disk.
 * Returns false only if they match.
 * Returns true if not, or if there is not data found, or on error.
 */
int dbfile_describe_file(struct dbhandle *db, uint64_t ino, uint64_t subvol,
				struct file *dbfile)
{
	int ret;
	char *buf;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.select_file_changes;

	/* in-memory databases has no wal support,
	 * so we must do the lock by ourselves
	 */
	if (!options.hashfile)
		dbfile_lock();

	ret = sqlite3_bind_int64(stmt, 1, ino);
	if (ret) {
		perror_sqlite(ret, "binding values");
		goto out;
	}

	ret = sqlite3_bind_int64(stmt, 2, subvol);
	if (ret) {
		perror_sqlite(ret, "binding values");
		goto out;
	}

	ret = sqlite3_step(stmt);
	if (ret == SQLITE_DONE) {
		ret = 0;
		goto out;
	}

	if (ret != SQLITE_ROW) {
		perror_sqlite(ret, "fetching a file");
		goto out;
	}

	dbfile->mtime = sqlite3_column_int64(stmt, 0);
	dbfile->size = sqlite3_column_int64(stmt, 1);

	buf = (char *)sqlite3_column_text(stmt, 2);
	strncpy(dbfile->filename, buf, PATH_MAX);

	dbfile->id = sqlite3_column_int64(stmt, 3);

	ret = 0;

out:
	if (!options.hashfile)
		dbfile_unlock();
	return ret;
}

int dbfile_load_same_files(struct dbhandle *db, struct results_tree *res,
			   unsigned int seq)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.get_duplicate_files;
	uint64_t size;
	int64_t fileid;
	unsigned char *digest;
	struct filerec *file;
	const unsigned char *filename;

	ret = sqlite3_bind_int64(stmt, 1, seq);
	if (ret) {
		perror_sqlite(ret, "binding value");
		return ret;
	}

	while ((ret = sqlite3_step(stmt)) == SQLITE_ROW) {
		fileid = sqlite3_column_int64(stmt, 0);
		size = sqlite3_column_int64(stmt, 1);
		digest = (unsigned char *)sqlite3_column_blob(stmt, 2);
		filename = sqlite3_column_text(stmt, 3);

		file = filerec_find(fileid);
		if (!file) {
			file = filerec_new((const char *)filename, fileid, size);
			if (!file)
				return ENOMEM;
		}

		ret = insert_one_result(res, digest, file, 0, size, 0);
		if (ret)
			return ENOMEM;
	}
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "looking up hash");
		return ret;
	}

	return 0;
}

int dbfile_rename_file(struct dbhandle *db, int64_t fileid, char *path)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.rename_file;

	ret = sqlite3_bind_text(stmt, 1, path, -1, SQLITE_STATIC);
	if (ret) {
		perror_sqlite(ret, "binding values");
		return ret;
	}

	ret = sqlite3_bind_int64(stmt, 2, fileid);
	if (ret) {
		perror_sqlite(ret, "binding values");
		return ret;
	}

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "renaming a file");
		return ret;
	}

	return 0;
}

void dbfile_set_gdb(struct dbhandle *db)
{
	gdb = db;
}

unsigned int get_max_dedupe_seq(struct dbhandle *db)
{
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.get_max_dedupe_seq;

	int ret = sqlite3_step(stmt);
	if (ret != SQLITE_ROW) {
		eprintf("error %d, get max dedupe seq: %s\n",
			ret, sqlite3_errstr(ret));
		return 0;
	}

	return sqlite3_column_int64(stmt, 0);
}

/*
 * Remove entries from the files table that have no extents associated.
 * This can happen if duperemove is ctrl^C while scanning files
 */
int dbfile_prune_unscanned_files(struct dbhandle *db)
{
	int ret;
	_cleanup_(sqlite3_reset_stmt) sqlite3_stmt *stmt = db->stmts.delete_unscanned_files;

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE) {
		perror_sqlite(ret, "executing sql");
		return ret;
	}

	return 0;
}

/*
 * Read the global srccount generation from the config table.
 * Missing row is treated as 0 (the default at first migration).
 * Returns 0 on success and sets *out_gen; non-zero errno on SQL
 * failure (caller falls back to 0 in that case).
 */
int dbfile_get_srccount_gen(struct dbhandle *db, int64_t *out_gen)
{
	sqlite3_stmt *stmt = NULL;
	int rc;
	int64_t gen = 0;

	rc = sqlite3_prepare_v2(db->db,
		"SELECT keyval FROM config WHERE keyname = 'srccount_gen';",
		-1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		perror_sqlite(rc, "preparing srccount_gen SELECT");
		return EIO;
	}

	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		gen = sqlite3_column_int64(stmt, 0);
	} else if (rc != SQLITE_DONE) {
		perror_sqlite(rc, "stepping srccount_gen SELECT");
		sqlite3_finalize(stmt);
		return EIO;
	}
	sqlite3_finalize(stmt);

	*out_gen = gen;
	return 0;
}

/*
 * Increment the global srccount generation. Used by
 * --bump-srccount-gen to lazily invalidate all existing srccount
 * values: after the bump, every row with srccount_gen < new
 * current_gen is "stale" and will be re-seeded by Phase 5 on
 * next encounter as a candidate.
 *
 * Single INSERT OR REPLACE with COALESCE so the first-ever bump
 * correctly transitions from "no row" (treated as 0) to 1.
 */
int dbfile_bump_srccount_gen(struct dbhandle *db, int64_t *out_new_gen)
{
	int rc;
	char *err = NULL;
	int64_t new_gen = 0;

	rc = sqlite3_exec(db->db,
		"INSERT OR REPLACE INTO config(keyname, keyval) "
		"VALUES ('srccount_gen', "
		"COALESCE((SELECT keyval FROM config "
		"WHERE keyname = 'srccount_gen'), 0) + 1);",
		NULL, NULL, &err);
	if (rc != SQLITE_OK) {
		eprintf("bump-srccount-gen: %s (%d)\n",
			err ? err : "(no message)", rc);
		if (err)
			sqlite3_free(err);
		return EIO;
	}

	rc = dbfile_get_srccount_gen(db, &new_gen);
	if (rc)
		return rc;

	if (out_new_gen)
		*out_new_gen = new_gen;
	return 0;
}
