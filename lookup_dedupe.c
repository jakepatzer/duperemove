/*
 * lookup_dedupe.c
 *
 * Streaming --lookup-only dedupe path.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>
#include <inttypes.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <time.h>

#include <glib.h>
#include <sqlite3.h>

#include "rbtree.h"
#include "list.h"
#include "csum.h"
#include "filerec.h"
#include "results-tree.h"
#include "dedupe.h"
#include "dbfile.h"
#include "util.h"
#include "memstats.h"
#include "debug.h"
#include "opt.h"
#include "btrfs-util.h"
#include "file_scan.h"
#include "run_dedupe.h"

#include "lookup_dedupe.h"

extern unsigned int blocksize;

/*
 * Lookup-mode run state. Single-threaded by construction (see header),
 * so no locking is required around these counters or the open_once.
 */
struct lookup_state {
	struct dbhandle	*db;
	sqlite3_stmt	*find_block_stmt;
	struct open_once ref_opens;
	int64_t		next_synth_id;	/* counter for in-memory-only ids */
	uint64_t	n_lookup_files;
	uint64_t	n_ref_files;
	uint64_t	matches_deduped;
	uint64_t	bytes_deduped;
	uint64_t	seed_matches_found;	/* db rows matched, before
						 * any dedupe attempt */
	uint64_t	dedupe_attempts;
	uint64_t	n_too_small_skipped;	/* files skipped because
						 * size < min_dedupe_size */
	uint64_t	n_self_files;		/* reference files
						 * stream-processed under
						 * --lookup-self */

	/*
	 * Real-time progress tracking. Updated by stream_blocks; the
	 * print is throttled to one emit per ~2 seconds in
	 * print_progress(). bytes_scanned_total is the cumulative
	 * outer-loop advance across all files; bytes_scanned_at_last
	 * is the value at the previous emit (for short-window speed).
	 * progress_active is per-file: set true on first emit during
	 * the current file, reset to false at start of each new file
	 * so we can decide whether to emit a final per-file summary
	 * line.
	 */
	struct timespec	start_time;
	struct timespec	last_progress_time;
	uint64_t	bytes_scanned_total;
	uint64_t	bytes_scanned_at_last;
	int		is_tty;
	bool		progress_active;
};

static bool block_is_zero(const char *buf, size_t len)
{
	size_t i;

	/*
	 * len is at most a single block (default 128 KiB, minimum
	 * 4 KiB). A byte-wise loop with the optimizer is fast enough
	 * and avoids assumptions about buffer alignment. Bail out at
	 * the first non-zero byte.
	 */
	for (i = 0; i < len; i++) {
		if (buf[i] != 0)
			return false;
	}
	return true;
}

/*
 * Submit a single (source, destination) dedupe pair via a minimal
 * two-extent ctxt. Reuses the existing dedupe_extents() ioctl-chunking
 * path and process_dedupe_results() (which records the (src, dst)
 * high-water on success, gated on options.coalesce). Returns 0 on
 * success or an errno-style error code; *kern_bytes is set to the
 * number of bytes the kernel actually deduped (may be 0 on
 * SAME_DATA_DIFFERS, may be less than len on partial dedupes).
 */
static int submit_pair_dedupe(struct filerec *src, uint64_t src_off,
			      struct filerec *dst, uint64_t dst_off,
			      uint64_t len, uint64_t *kern_bytes)
{
	struct dedupe_ctxt *ctxt;
	int ret;

	*kern_bytes = 0;

	ctxt = new_dedupe_ctxt(2, src_off, len, src);
	if (ctxt == NULL)
		return ENOMEM;

	if (add_extent_to_dedupe(ctxt, dst_off, dst) < 0) {
		free_dedupe_ctxt(ctxt);
		return ENOMEM;
	}

	ret = dedupe_extents(ctxt);
	if (ret == 0) {
		process_dedupe_results(ctxt, kern_bytes);
	} else {
		ret = errno;
		eprintf("lookup: dedupe ioctl (%s @ %"PRIu64
			" -> %s @ %"PRIu64", %"PRIu64" bytes) "
			"returned %d: %s\n", src->filename, src_off,
			dst->filename, dst_off, len, ret, strerror(ret));
	}
	free_dedupe_ctxt(ctxt);
	return ret;
}

/*
 * Stream-process a single lookup file. The caller already opened fd
 * (for stat / subvol classification) and verified the file is not in
 * the hashfile. We adopt that fd into a transient in-memory-only
 * filerec, run the block hash + lookup + extend + submit loop, then
 * free the filerec so memory stays bounded as the file count grows.
 */
/*
 * Common streaming loop for both fresh "lookup" files (transient
 * filerec, negative synthetic id) and reference files re-entering
 * the pipeline under --lookup-self (positive id, filerec already in
 * st->ref_opens).
 *
 * The caller owns file_fr (which must have file_fr->fd set to a
 * valid descriptor open O_RDONLY) and is responsible for any
 * filerec / fd cleanup. stream_blocks itself allocates only the
 * per-call block buffer.
 *
 * Self-match filter: a row whose (fileid, loff) equals (file_fr's
 * fileid, current offset) is the block's identity in the hashfile
 * and is dropped. Same fileid with a different loff is a legitimate
 * within-file (e.g. within-image) candidate and is kept.
 *
 * Returns 0 on a normal completion (including when no matches were
 * found) or ENOMEM if the block buffer cannot be allocated.
 */
/*
 * Throttled real-time progress line for --lookup-only.
 *
 * Called once per outer-loop iteration in stream_blocks; emits at
 * most one line per ~2 seconds of wall-clock time per file. On a
 * TTY, the line is rewritten in place via \r + ANSI clear-to-EOL so
 * the terminal stays clean. Off-TTY (log files, pipes), each emit
 * is a separate \n-terminated line so the log is grep-friendly.
 *
 * `final` forces an emit regardless of the throttle and always uses
 * \n - used at the end of each file's stream_blocks to "commit" the
 * progress line before the next file's "scanning ..." qprintf
 * lands on stdout.
 *
 * Per-iteration cost when throttled-out: one clock_gettime via
 * vDSO (~20 ns). Negligible even at 1M iter/sec.
 */
/*
 * Always-human-readable size formatter for the progress line.
 * pretty_size() respects the global `human_readable` flag (set by -h),
 * but the streaming progress line is unreadable at multi-GB scale
 * without unit suffixes, so format unconditionally here.
 */
static void fmt_size_h(uint64_t size, char *str, size_t str_bytes)
{
	static const char *units[] = { "B", "K", "M", "G", "T", "P", "E" };
	unsigned int u = 0;
	double v = (double)size;

	while (v >= 1024.0 && u + 1 < sizeof(units) / sizeof(units[0])) {
		v /= 1024.0;
		u++;
	}
	snprintf(str, str_bytes, "%.1f%s", v, units[u]);
}

static void print_progress(struct lookup_state *st, const char *path,
			   uint64_t off, uint64_t size, bool final)
{
	struct timespec now;
	double elapsed_since, elapsed_total;
	uint64_t bytes_since;
	double speed_now_mbs, speed_avg_mbs;
	const char *name;
	int hrs, mins;
	char deduped_buf[16];

	clock_gettime(CLOCK_MONOTONIC, &now);
	elapsed_since = (now.tv_sec - st->last_progress_time.tv_sec) +
			(now.tv_nsec - st->last_progress_time.tv_nsec) / 1e9;

	if (!final && elapsed_since < 2.0)
		return;
	/*
	 * Final-emit is only useful if at least one in-progress line
	 * was already shown for this file. For files that complete in
	 * under one throttle window the file is fast enough that the
	 * end-of-run summary is sufficient; skipping avoids noise.
	 */
	if (final && !st->progress_active)
		return;

	elapsed_total = (now.tv_sec - st->start_time.tv_sec) +
			(now.tv_nsec - st->start_time.tv_nsec) / 1e9;
	bytes_since = st->bytes_scanned_total - st->bytes_scanned_at_last;

	speed_now_mbs = (elapsed_since > 0.001) ?
		(bytes_since / 1048576.0 / elapsed_since) : 0.0;
	speed_avg_mbs = (elapsed_total > 0.001) ?
		(st->bytes_scanned_total / 1048576.0 / elapsed_total) : 0.0;

	name = strrchr(path, '/');
	name = name ? name + 1 : path;

	hrs = (int)(elapsed_total / 3600);
	mins = (int)((elapsed_total - hrs * 3600) / 60);

	fmt_size_h(st->bytes_deduped, deduped_buf, sizeof(deduped_buf));

	if (st->is_tty && !final) {
		fprintf(stderr,
			"[lookup] file %"PRIu64" \"%s\" %5.1f%% | "
			"%5.0f MB/s now %5.0f avg | "
			"cand %"PRIu64" attempts %"PRIu64" ok %"PRIu64" | "
			"deduped %s | %dh%02dm\033[K\r",
			st->n_lookup_files + st->n_self_files + 1,
			name,
			size > 0 ? (100.0 * off / size) : 0.0,
			speed_now_mbs, speed_avg_mbs,
			st->seed_matches_found,
			st->dedupe_attempts, st->matches_deduped,
			deduped_buf,
			hrs, mins);
	} else {
		fprintf(stderr,
			"[lookup] file %"PRIu64" \"%s\" %5.1f%% | "
			"%5.0f MB/s now %5.0f avg | "
			"cand %"PRIu64" attempts %"PRIu64" ok %"PRIu64" | "
			"deduped %s | %dh%02dm\n",
			st->n_lookup_files + st->n_self_files + 1,
			name,
			size > 0 ? (100.0 * off / size) : 0.0,
			speed_now_mbs, speed_avg_mbs,
			st->seed_matches_found,
			st->dedupe_attempts, st->matches_deduped,
			deduped_buf,
			hrs, mins);
	}
	fflush(stderr);

	st->last_progress_time = now;
	st->bytes_scanned_at_last = st->bytes_scanned_total;
	st->progress_active = true;
}

static int stream_blocks(const char *path, struct filerec *file_fr,
			 uint64_t size, struct lookup_state *st)
{
	char *buf;
	sqlite3_stmt *stmt = st->find_block_stmt;
	uint64_t off = 0;
	/*
	 * Seed window size for the h16 lookup. Each seed covers
	 * 4 consecutive blocksize-byte ranges (the same 16 KB the
	 * blocks_h16 build pass anchored each h16 row over). The
	 * seed step itself stays at blocksize, so successive seeds
	 * overlap by 3 blocks. This is the "scan at every blocksize
	 * stride" semantics with the new 16 KB hash window.
	 */
	uint64_t window_bytes = (uint64_t)blocksize * 4;

	/*
	 * Outer-loop read buffering. The streaming scan walks the file
	 * blocksize at a time, but issuing a separate pread per block
	 * costs one syscall per block (~62.5 M for a 250 GB file at
	 * 4 KB blocks). Fold N consecutive blocks into one pread of
	 * size LOOKUP_READ_BUF_SIZE; subsequent blocks are served
	 * from the in-process buffer instead. The kernel's page cache
	 * already absorbs sequential preads, so this is a syscall +
	 * buffer-management cost reduction, not a disk-I/O reduction.
	 *
	 * The buffer is refilled from `off` whenever the current
	 * window (window_bytes starting at `off`) does not live
	 * entirely within it. That handles (1) the normal blocksize
	 * advance walking off the end after N iters, and (2) a
	 * successful dedupe causing `off` to jump forward by the
	 * deduped range's length, possibly past the buffer entirely.
	 *
	 * Buffer size must be >= window_bytes (we need all 4 blocks
	 * of the current seed window resident at the same time to
	 * compute h16). If a future caller runs with blocksize *
	 * 4 > LOOKUP_READ_BUF_SIZE we size up automatically; in that
	 * regime the buffering degrades to one pread per window which
	 * is correct but offers no syscall amortization.
	 */
#define LOOKUP_READ_BUF_SIZE (64 * 1024)
	size_t buf_size = LOOKUP_READ_BUF_SIZE;
	uint64_t buf_file_off = 0;
	size_t buf_valid = 0;
	int rc = 0;

	if (buf_size < window_bytes)
		buf_size = window_bytes;

	buf = malloc(buf_size);
	if (buf == NULL)
		return ENOMEM;

	/*
	 * Per-file progress reset. progress_active being false means
	 * "no in-progress line has been emitted for THIS file yet" and
	 * gates the final-emit at end-of-file. start_time and
	 * bytes_scanned_total are corpus-wide and not reset here.
	 */
	st->progress_active = false;

	qprintf("lookup: scanning \"%s\" (%s, file %"PRIu64")\n",
		path, pretty_size(size),
		st->n_lookup_files + st->n_self_files + 1);

	while (off + window_bytes <= size) {
		unsigned char digests[4][DIGEST_LEN];
		unsigned char h16[DIGEST_LEN];
		unsigned char concat[4 * DIGEST_LEN];
		uint64_t advance = blocksize;
		char *block0;

		/*
		 * Ensure the current 16 KB seed window lives entirely
		 * within the buffer. If the buffer is empty, exhausted,
		 * or behind us (large advance after a successful
		 * dedupe), refill from `off`. We never need to look
		 * backward because the outer loop is monotonic in `off`.
		 */
		if (off < buf_file_off ||
		    off + window_bytes > buf_file_off + buf_valid) {
			ssize_t rd;

			buf_file_off = off;
			rd = pread(file_fr->fd, buf, buf_size, off);
			if (rd < (ssize_t)window_bytes) {
				if (rd < 0)
					eprintf("lookup: pread \"%s\" @ "
						"%"PRIu64": %s\n",
						path, off, strerror(errno));
				/*
				 * Either a hard error or a short read
				 * that can't form a full seed window -
				 * treat as EOF and stop. The outer-loop
				 * guard (off + window_bytes <= size)
				 * normally keeps us in-bounds, but the
				 * file may have shrunk between scan and
				 * now.
				 */
				break;
			}
			buf_valid = (size_t)rd;
		}

		block0 = buf + (off - buf_file_off);

		/*
		 * Zero-skip is on the FIRST block of the seed window
		 * only. The blocks_h16 build also skipped windows that
		 * contained an interior zero-block gap, so windows
		 * with a zero leading block could not have entries in
		 * blocks_h16 anyway. Skipping at the seed side mirrors
		 * the build-side semantics.
		 */
		if (options.skip_zeroes && block_is_zero(block0, blocksize)) {
			off += blocksize;
			/*
			 * Zero-skip bypasses the bottom-of-loop
			 * bytes_scanned_total update and print_progress
			 * call, so account for the advance here.
			 * Without this, files with large zero regions
			 * (typical for raw disk images) show 0 MB/s and
			 * the throttled progress line never fires, even
			 * though `off` is racing forward.
			 */
			st->bytes_scanned_total += blocksize;
			continue;
		}

		/*
		 * Compute 4 block digests then h16 = XXH128 of their
		 * concatenation. This matches the construction used by
		 * h16_build_index() exactly so an image position whose
		 * 16 KB content matches a corpus reference at any
		 * blocksize-aligned position will produce the same h16
		 * value.
		 */
		checksum_block(block0, (int)blocksize, digests[0]);
		checksum_block(block0 + blocksize, (int)blocksize, digests[1]);
		checksum_block(block0 + 2 * blocksize, (int)blocksize,
			       digests[2]);
		checksum_block(block0 + 3 * blocksize, (int)blocksize,
			       digests[3]);

		memcpy(concat,                     digests[0], DIGEST_LEN);
		memcpy(concat +     DIGEST_LEN,    digests[1], DIGEST_LEN);
		memcpy(concat + 2 * DIGEST_LEN,    digests[2], DIGEST_LEN);
		memcpy(concat + 3 * DIGEST_LEN,    digests[3], DIGEST_LEN);
		checksum_block((char *)concat, sizeof(concat), h16);

		rc = sqlite3_bind_blob(stmt, 1, h16, DIGEST_LEN,
				       SQLITE_STATIC);
		if (rc) {
			eprintf("lookup: sqlite3_bind_blob failed: %d\n", rc);
			sqlite3_reset(stmt);
			off += blocksize;
			st->bytes_scanned_total += blocksize;
			continue;
		}

		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
			int64_t ref_id;
			uint64_t ref_loff;
			struct filerec *ref;
			uint64_t ext_len;
			uint64_t kern_bytes = 0;
			int sub_rc, oo;

			ref_id = sqlite3_column_int64(stmt, 0);
			ref_loff = sqlite3_column_int64(stmt, 1);

			/*
			 * Self-identity filter: same file AND same
			 * logical offset is the seed's own row in
			 * blocks_h16. For transient (negative)
			 * fileids this never fires; for reference
			 * files re-entering under --lookup-self this
			 * is what prevents a position from "matching"
			 * itself.
			 */
			if (ref_id == file_fr->fileid && ref_loff == off)
				continue;

			st->seed_matches_found++;

			/*
			 * Per (src, dst) high-water suppression. Done
			 * BEFORE filerec_find / load / open because the
			 * check needs only the two fileids and the dst
			 * offset - no filerec object required. Saves a
			 * rbtree walk, a possible SQL query, and a
			 * possible open(2) for every match that lands
			 * inside a previously-deduped range, which on
			 * --lookup-self workloads can be the majority
			 * of matches in the tail of a long contiguous
			 * dedupe.
			 */
			if (options.coalesce &&
			    coalesce_covered(ref_id, file_fr->fileid, off))
				continue;

			/*
			 * Same-file gap pre-check. For a within-file
			 * match, the dedupe range cannot exceed the
			 * gap between source and destination offsets
			 * (the existing same-file overlap cap below
			 * enforces this after extend_match). If that
			 * gap is already smaller than --min-dedupe-size
			 * the match cannot possibly qualify, so skip
			 * it now and avoid paying extend_match's
			 * random-seek cost on the ref_loff side. The
			 * formula matches the post-extend cap exactly
			 * so there is no risk of accepting a match here
			 * that the post-cap would have rejected (or
			 * vice versa). Only ref_id is needed - filerec
			 * is not yet resolved, which is the point.
			 */
			if (options.min_dedupe_size &&
			    ref_id == file_fr->fileid) {
				uint64_t gap = (ref_loff > off) ?
					(ref_loff - off) : (off - ref_loff);
				if (gap < options.min_dedupe_size)
					continue;
			}

			ref = filerec_find(ref_id);
			if (ref == NULL) {
				if (dbfile_load_one_filerec(st->db, ref_id,
							    &ref) || ref == NULL) {
					vprintf("lookup: cannot load "
						"reference filerec %"PRId64
						"; skipping match.\n", ref_id);
					continue;
				}
			}

			oo = filerec_open_once(ref, &st->ref_opens);
			if (oo) {
				vprintf("lookup: cannot open reference "
					"\"%s\" (was the images directory "
					"passed on the command line?); "
					"skipping match.\n", ref->filename);
				continue;
			}

			/*
			 * h16 guarantees a 16 KB byte match, so start
			 * extend_match with the full window already
			 * matched - it just needs to extend forward
			 * beyond the seed. Without --coalesce we
			 * submit the bare 16 KB.
			 */
			ext_len = window_bytes;
			if (options.coalesce)
				ext_len = extend_match(ref, ref_loff,
						       file_fr, off,
						       window_bytes);

			/*
			 * Same-file dedup safety: cap to the gap
			 * between source and destination offsets so
			 * the [ref_loff, ref_loff+L) and [off, off+L)
			 * ranges do not overlap. The kernel returns
			 * EINVAL on overlapping ranges; without the
			 * cap, periodic content within a file would
			 * trigger this for every seed in the periodic
			 * region.
			 */
			if (ref->fileid == file_fr->fileid) {
				uint64_t gap;

				gap = (ref_loff > off) ? (ref_loff - off)
						       : (off - ref_loff);
				if (ext_len > gap)
					ext_len = gap;
			}

			if (options.min_dedupe_size &&
			    ext_len < options.min_dedupe_size)
				continue;

			st->dedupe_attempts++;
			sub_rc = submit_pair_dedupe(ref, ref_loff,
						    file_fr, off, ext_len,
						    &kern_bytes);
			if (sub_rc == 0 && kern_bytes > 0) {
				uint64_t step;

				st->matches_deduped++;
				st->bytes_deduped += kern_bytes;

				/*
				 * High-water skip: advance past the
				 * range the kernel actually deduped.
				 * Round down to blocksize to keep
				 * subsequent seed offsets block-aligned
				 * (otherwise their h16 values cannot
				 * match any entry in blocks_h16).
				 */
				step = (kern_bytes / blocksize) * blocksize;
				if (step >= blocksize)
					advance = step;
				break;	/* this seed is satisfied */
			}
		}
		sqlite3_reset(stmt);

		off += advance;
		st->bytes_scanned_total += advance;
		print_progress(st, path, off, size, false);
	}

	/*
	 * Commit the in-flight progress line (if any) with a \n so the
	 * next file's qprintf "scanning" message on stdout does not
	 * land on top of it.
	 */
	print_progress(st, path, off, size, true);

	free(buf);
	return 0;
}

static int process_lookup_file(const char *path, int fd, uint64_t size,
			       struct lookup_state *st)
{
	struct filerec *lookup_fr;
	int64_t synth_id;
	int rc;

	/*
	 * Optimization: a contiguous match in this file can never
	 * exceed the file's own size, so any file smaller than
	 * --min-dedupe-size cannot produce a qualifying submission.
	 * Skip before any pread / hashing / sqlite / filerec work.
	 */
	if (options.min_dedupe_size && size < options.min_dedupe_size) {
		vprintf("lookup: skipping \"%s\" (%s < "
			"--min-dedupe-size); no possible qualifying "
			"match.\n", path, pretty_size(size));
		close(fd);
		st->n_too_small_skipped++;
		return 0;
	}

	/*
	 * Synthetic, in-memory-only fileid. Negative so it can never
	 * collide with positive ids assigned by the hashfile. The
	 * self-identity filter in stream_blocks compares the SQL
	 * fileid against file_fr->fileid; for synthetic ids it never
	 * matches.
	 */
	synth_id = st->next_synth_id--;

	lookup_fr = filerec_new(path, synth_id, size);
	if (lookup_fr == NULL) {
		eprintf("lookup: out of memory creating filerec for \"%s\"\n",
			path);
		close(fd);
		st->next_synth_id++;
		return ENOMEM;
	}

	/* Adopt the open fd; we close it manually below. */
	lookup_fr->fd = fd;
	lookup_fr->fd_refs = 1;

	rc = stream_blocks(path, lookup_fr, size, st);

	/*
	 * Detach and close the lookup file's fd, then drop the
	 * transient filerec so memory does not grow with the number
	 * of lookup files processed.
	 */
	if (lookup_fr->fd != -1) {
		close(lookup_fr->fd);
		lookup_fr->fd = -1;
		lookup_fr->fd_refs = 0;
	}
	filerec_free(lookup_fr);
	st->n_lookup_files++;
	return rc;
}

/*
 * --lookup-self: stream-process a file that is already in the
 * hashfile (a "reference" file). The filerec is already pinned in
 * st->ref_opens by the caller; we reuse its fd and its real
 * (positive) fileid so the self-identity filter in stream_blocks
 * can correctly distinguish "block matching itself" from
 * "block matching another block at a different offset in the same
 * file" (within-image dedup).
 *
 * Does not allocate or free the filerec, does not touch the fd
 * lifetime: ref_opens owns both.
 */
static int process_reference_self(const char *path, struct filerec *ref_fr,
				  struct lookup_state *st)
{
	int rc;

	if (options.min_dedupe_size &&
	    ref_fr->size < options.min_dedupe_size) {
		vprintf("lookup: skipping reference \"%s\" (%s < "
			"--min-dedupe-size); no possible qualifying "
			"match.\n", path, pretty_size(ref_fr->size));
		st->n_too_small_skipped++;
		return 0;
	}

	rc = stream_blocks(path, ref_fr, ref_fr->size, st);
	st->n_self_files++;
	return rc;
}

/*
 * Open one cmdline file, classify it as reference (skip) or lookup
 * (stream-process). Reference files are pinned open in st->ref_opens
 * for the duration of the run so subsequent matches can pread/ioctl
 * against them without re-opening. Lookup files take their fd into
 * process_lookup_file() which closes it on return.
 */
static int process_one_file(const char *path, struct stat *sb,
			    struct lookup_state *st)
{
	int fd;
	uint64_t subvol = 0;
	int rc;
	struct file dbfile;

	fd = open(path, O_RDONLY);
	if (fd < 0) {
		eprintf("lookup: open \"%s\": %s\n", path, strerror(errno));
		return errno;
	}

	/*
	 * On non-btrfs filesystems lookup_btrfs_subvol returns an
	 * errno (typically ENOTTY); fall back to subvol=0, matching
	 * how duperemove treats non-btrfs storage. On btrfs the
	 * subvol id is needed for correct hard-link / inode disambig.
	 */
	if (lookup_btrfs_subvol(fd, &subvol) != 0)
		subvol = 0;

	memset(&dbfile, 0, sizeof(dbfile));
	rc = dbfile_describe_file(st->db, sb->st_ino, subvol, &dbfile);
	if (rc) {
		/*
		 * SQL error during classification. Already logged by
		 * dbfile_describe_file. Be conservative: don't treat
		 * an unclassifiable file as a lookup target (we could
		 * end up rehashing a known reference); skip it.
		 */
		eprintf("lookup: classification failed for \"%s\"; "
			"skipping.\n", path);
		close(fd);
		return rc;
	}

	if (dbfile.id != 0) {
		/* Reference file: pin into the long-lived open set. */
		struct filerec *f;

		close(fd);	/* re-open via filerec for refcounted lifetime */

		f = filerec_find(dbfile.id);
		if (f == NULL) {
			if (dbfile_load_one_filerec(st->db, dbfile.id, &f) ||
			    f == NULL) {
				eprintf("lookup: failed to load reference "
					"filerec for \"%s\" (id %"PRId64").\n",
					path, dbfile.id);
				return ENOENT;
			}
		}
		if (filerec_open_once(f, &st->ref_opens) != 0) {
			eprintf("lookup: cannot open reference \"%s\".\n",
				path);
			return errno;
		}
		st->n_ref_files++;
		vprintf("lookup: reference \"%s\" pinned (id %"PRId64").\n",
			path, dbfile.id);

		/*
		 * Under --lookup-self, also stream-process this
		 * reference file against the hashfile to discover
		 * cross-file and within-file (e.g. cross-image and
		 * within-image) matches. Self-identity matches are
		 * filtered inside stream_blocks.
		 */
		if (options.lookup_self)
			return process_reference_self(path, f, st);
		return 0;
	}

	/* Lookup file: hand the open fd off to the streaming loop. */
	return process_lookup_file(path, fd, (uint64_t)sb->st_size, st);
}

/*
 * Walk a single cmdline path. Directories are entered if
 * options.recurse_dirs is set; otherwise their direct regular-file
 * children are processed (matching the non-recursive behaviour of
 * the normal hash phase). Symlinks, special files, mount points
 * crossed only when the kernel/glibc follows them naturally for the
 * caller; we use lstat to avoid silently following symlinks.
 */
static int walk_path(const char *path, struct lookup_state *st)
{
	struct stat sb;
	DIR *d;
	struct dirent *e;
	int rc;

	if (lstat(path, &sb) < 0) {
		eprintf("lookup: lstat \"%s\": %s\n", path, strerror(errno));
		return errno;
	}

	if (S_ISREG(sb.st_mode))
		return process_one_file(path, &sb, st);

	if (!S_ISDIR(sb.st_mode)) {
		vprintf("lookup: skipping non-regular non-directory "
			"\"%s\".\n", path);
		return 0;
	}

	d = opendir(path);
	if (d == NULL) {
		eprintf("lookup: opendir \"%s\": %s\n", path, strerror(errno));
		return errno;
	}

	while ((e = readdir(d)) != NULL) {
		char child[PATH_MAX];

		if (e->d_name[0] == '.' &&
		    (e->d_name[1] == 0 ||
		     (e->d_name[1] == '.' && e->d_name[2] == 0)))
			continue;

		if ((size_t)snprintf(child, sizeof(child), "%s/%s",
				     path, e->d_name) >= sizeof(child)) {
			eprintf("lookup: path too long: \"%s/%s\"; "
				"skipping.\n", path, e->d_name);
			continue;
		}

		if (options.recurse_dirs) {
			rc = walk_path(child, st);
			if (rc == ENOMEM)
				goto out_oom;
		} else {
			struct stat csb;

			if (lstat(child, &csb) < 0) {
				eprintf("lookup: lstat \"%s\": %s\n",
					child, strerror(errno));
				continue;
			}
			if (S_ISREG(csb.st_mode)) {
				rc = process_one_file(child, &csb, st);
				if (rc == ENOMEM)
					goto out_oom;
			}
		}
	}

	closedir(d);
	return 0;

out_oom:
	closedir(d);
	return ENOMEM;
}

int lookup_dedupe_main(struct dbhandle *db, int argc, char **argv,
		       int filelist_idx)
{
	struct lookup_state st;
	int rc;
	int i;

	memset(&st, 0, sizeof(st));
	st.db = db;
	st.ref_opens = OPEN_ONCE_INIT;
	/*
	 * Start synthetic fileids one less than zero and walk down.
	 * Positive ids belong to the hashfile; negative ids are
	 * strictly in-memory and never persisted (--lookup-only never
	 * writes).
	 */
	st.next_synth_id = -1;

	/*
	 * Real-time progress baseline. start_time anchors elapsed and
	 * average-throughput math; last_progress_time starts equal to
	 * start_time so the very first iter's elapsed_since is ~0 and
	 * the throttle correctly skips it (no spurious 0-data emit).
	 * is_tty is decided once here so we don't re-isatty per emit.
	 */
	clock_gettime(CLOCK_MONOTONIC, &st.start_time);
	st.last_progress_time = st.start_time;
	st.is_tty = isatty(STDERR_FILENO);

	/*
	 * Prepare the h16-based lookup statement. Keyed by the 16 KB
	 * rolling-window hash; the covering idx_blocks_h16 index
	 * makes this an O(log n) index-only scan, no blocks_h16
	 * table reads needed. We deliberately avoid joining files
	 * here: the (fileid, loff) pair is enough to drive
	 * filerec_find / dbfile_load_one_filerec lazily and skip the
	 * join cost on every call.
	 *
	 * The key insight that makes this much faster than the
	 * previous per-block-digest lookup: every row this query
	 * returns is guaranteed to be a 16 KB byte-match against the
	 * seed (modulo cryptographic XXH128 collision, negligible at
	 * any realistic scale). The old per-digest path returned
	 * thousands of "candidates" per seed for hot 4 KB patterns,
	 * most of which diverged within the first few KB of the
	 * surrounding bytes; extend_match burned ~50-100 KB of disk
	 * reads per candidate to discover this. With h16, every
	 * candidate is already known to be a valid 16 KB match and
	 * no wasted extend_match cycles happen.
	 */
	rc = sqlite3_prepare_v2(db->db,
		"select fileid, loff from blocks_h16 where h16 = ?1;",
		-1, &st.find_block_stmt, NULL);
	if (rc) {
		eprintf("lookup: preparing h16-lookup statement: %s\n",
			sqlite3_errstr(rc));
		return EIO;
	}

	/*
	 * Initialize the coalesce high-water map even though our
	 * streaming loop also performs a within-file high-water skip
	 * by advancing the offset directly. coalesce_record() is
	 * called from process_dedupe_results() when --coalesce is on
	 * and uniquely identifies the (src, dst) pair, which the
	 * within-file skip cannot do on its own. The init is a no-op
	 * when --coalesce is off.
	 */
	coalesce_map_init();

	for (i = filelist_idx; i < argc; i++) {
		rc = walk_path(argv[i], &st);
		if (rc == ENOMEM) {
			eprintf("lookup: out of memory walking \"%s\"; "
				"aborting.\n", argv[i]);
			break;
		}
	}

	filerec_close_open_list(&st.ref_opens);
	sqlite3_finalize(st.find_block_stmt);
	coalesce_map_destroy();

	qprintf("lookup: %"PRIu64" lookup file(s) processed, "
		"%"PRIu64" reference file(s) self-processed, "
		"%"PRIu64" skipped (< --min-dedupe-size), "
		"%"PRIu64" reference file(s) pinned, "
		"%"PRIu64" seed hash match(es), "
		"%"PRIu64" dedupe attempt(s), "
		"%"PRIu64" succeeded, %s deduped.\n",
		st.n_lookup_files, st.n_self_files,
		st.n_too_small_skipped,
		st.n_ref_files, st.seed_matches_found,
		st.dedupe_attempts, st.matches_deduped,
		pretty_size(st.bytes_deduped));

	if ((st.n_lookup_files + st.n_self_files) > 0 &&
	    st.matches_deduped == 0) {
		if (st.seed_matches_found == 0)
			eprintf("Warning: --lookup-only processed "
				"%"PRIu64" file(s) but found no hash "
				"matches against the hashfile. Verify "
				"--block-size and that the hashfile was "
				"built from the intended reference "
				"files.\n",
				st.n_lookup_files + st.n_self_files);
		else
			eprintf("Warning: --lookup-only found "
				"%"PRIu64" hash match(es) but no dedupe "
				"succeeded. Check that the filesystem "
				"is btrfs (or xfs with reflink) and that "
				"FIDEDUPERANGE is supported on this "
				"kernel.\n", st.seed_matches_found);
	}

	return rc == ENOMEM ? ENOMEM : 0;
}
