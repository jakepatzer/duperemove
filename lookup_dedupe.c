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
static int process_lookup_file(const char *path, int fd, uint64_t size,
			       struct lookup_state *st)
{
	struct filerec *lookup_fr;
	char *buf = NULL;
	sqlite3_stmt *stmt = st->find_block_stmt;
	uint64_t off = 0;
	int rc = 0;
	int64_t synth_id;

	/*
	 * Synthetic, in-memory-only fileid. Negative so it can never
	 * collide with positive ids assigned by the hashfile.
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

	/*
	 * Adopt our already-open fd directly. We bypass filerec_open()
	 * to keep the same fd we used for classification, and to keep
	 * the lifetime explicit (we close it manually below).
	 */
	lookup_fr->fd = fd;
	lookup_fr->fd_refs = 1;

	buf = malloc(blocksize);
	if (buf == NULL) {
		rc = ENOMEM;
		goto out;
	}

	qprintf("lookup: scanning \"%s\" (%s, file %"PRIu64")\n",
		path, pretty_size(size), st->n_lookup_files + 1);

	/*
	 * Iterate full blocks. The trailing partial block (if any) is
	 * intentionally ignored: it cannot match any hash in the
	 * hashfile (which only stores full-block hashes).
	 */
	while (off + blocksize <= size) {
		ssize_t rd;
		unsigned char digest[DIGEST_LEN];
		uint64_t advance = blocksize;
		bool advanced = false;

		rd = pread(fd, buf, blocksize, off);
		if (rd != (ssize_t)blocksize) {
			if (rd < 0)
				eprintf("lookup: pread \"%s\" @ %"PRIu64
					": %s\n", path, off, strerror(errno));
			/* short read: treat as EOF and stop */
			break;
		}

		if (options.skip_zeroes && block_is_zero(buf, rd)) {
			off += blocksize;
			continue;
		}

		checksum_block(buf, (int)rd, digest);

		rc = sqlite3_bind_blob(stmt, 1, digest, DIGEST_LEN,
				       SQLITE_STATIC);
		if (rc) {
			eprintf("lookup: sqlite3_bind_blob failed: %d\n", rc);
			sqlite3_reset(stmt);
			off += blocksize;
			continue;
		}

		/*
		 * Try matches in the order returned by the index. As
		 * soon as one match yields an actual dedupe, advance
		 * the outer offset past the deduped range and move on
		 * (no point deduping the same destination region a
		 * second time against a different source).
		 */
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
			int64_t ref_id;
			uint64_t ref_loff;
			struct filerec *ref;
			uint64_t ext_len;
			uint64_t kern_bytes = 0;
			int sub_rc, oo;

			ref_id = sqlite3_column_int64(stmt, 0);
			ref_loff = sqlite3_column_int64(stmt, 1);

			/* Self-match guard (synthetic id is negative). */
			if (ref_id == synth_id)
				continue;

			st->seed_matches_found++;

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

			/*
			 * filerec_open_once is refcounted and idempotent
			 * for files already in st->ref_opens. Returns 0
			 * on success or an errno on failure. Counted at
			 * classification time, not here, so this counter
			 * stays a count of files and not match attempts.
			 */
			oo = filerec_open_once(ref, &st->ref_opens);
			if (oo) {
				vprintf("lookup: cannot open reference "
					"\"%s\" (was the images directory "
					"passed on the command line?); "
					"skipping match.\n", ref->filename);
				continue;
			}

			/* Per (src, dst) high-water suppression. */
			if (options.coalesce &&
			    coalesce_covered(ref_id, synth_id, off))
				continue;

			ext_len = blocksize;
			if (options.coalesce)
				ext_len = extend_match(ref, ref_loff,
						       lookup_fr, off,
						       blocksize);

			if (options.min_dedupe_size &&
			    ext_len < options.min_dedupe_size)
				continue;

			st->dedupe_attempts++;
			sub_rc = submit_pair_dedupe(ref, ref_loff,
						    lookup_fr, off, ext_len,
						    &kern_bytes);
			if (sub_rc == 0 && kern_bytes > 0) {
				uint64_t step;

				st->matches_deduped++;
				st->bytes_deduped += kern_bytes;

				/*
				 * High-water skip: advance past the
				 * range the kernel actually deduped.
				 * Round down to a multiple of blocksize
				 * to keep subsequent seed offsets
				 * block-aligned (otherwise their
				 * hashes cannot match any block hash in
				 * the hashfile). kern_bytes is normally
				 * already blocksize-aligned; this is
				 * defensive.
				 */
				step = (kern_bytes / blocksize) * blocksize;
				if (step >= blocksize) {
					advance = step;
					advanced = true;
				}
				break;	/* this seed is satisfied */
			}
		}
		sqlite3_reset(stmt);

		off += advance;
		(void)advanced;
	}

	rc = 0;

out:
	free(buf);

	/*
	 * Detach and close the lookup file's fd, then drop the
	 * transient filerec so memory does not grow with the number of
	 * lookup files processed.
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
	 * Prepare the per-block lookup statement. Keyed by digest
	 * only; the idx_blocks_digest index makes this an O(log n)
	 * lookup per call. We deliberately avoid joining files here:
	 * the (fileid, loff) pair is enough to drive filerec_find /
	 * dbfile_load_one_filerec lazily and skip the join cost on
	 * every call.
	 */
	rc = sqlite3_prepare_v2(db->db,
		"select fileid, loff from blocks where digest = ?1;",
		-1, &st.find_block_stmt, NULL);
	if (rc) {
		eprintf("lookup: preparing block-lookup statement: %s\n",
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
		"%"PRIu64" reference file(s) pinned, "
		"%"PRIu64" seed hash match(es), "
		"%"PRIu64" dedupe attempt(s), "
		"%"PRIu64" succeeded, %s deduped.\n",
		st.n_lookup_files, st.n_ref_files,
		st.seed_matches_found, st.dedupe_attempts,
		st.matches_deduped, pretty_size(st.bytes_deduped));

	if (st.n_lookup_files > 0 && st.matches_deduped == 0) {
		if (st.seed_matches_found == 0)
			eprintf("Warning: --lookup-only processed "
				"%"PRIu64" file(s) but found no hash "
				"matches against the hashfile. Verify "
				"--block-size and that the hashfile was "
				"built from the intended reference "
				"files.\n", st.n_lookup_files);
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
