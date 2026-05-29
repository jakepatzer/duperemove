/*
 * srccount_seed.c
 *
 * Phase 5: lazy LOGICAL_INO_V2 seed for srccount.
 *
 * Design overview is in srccount_seed.h. Key choices:
 *
 *   - Bounded buffer. We size the result buffer for cap + 10
 *     entries and pass that size to LOGICAL_INO_V2. When the
 *     buffer fills, the kernel stops walking the back-reference
 *     list, so the worst-case ioctl cost is bounded by (cap+10)
 *     references rather than the unbounded "walk every ref" cost
 *     that triggers the slow-backref kernel pathology. This is
 *     exactly the workaround bees uses.
 *
 *   - One call per canonical per run. Result is written back to
 *     blocks.srccount; subsequent uses of the same canonical see
 *     the cached value via select_alias_root_stmt / select_srccount_stmt.
 *     Across a typical workload this is hundreds of thousands of
 *     ioctls total - hours of cumulative kernel-side work, dwarfed
 *     by FIDEDUPERANGE wall time elsewhere.
 *
 *   - Single-threaded interaction. We never call LOGICAL_INO_V2
 *     concurrently with FIDEDUPERANGE within our process (the
 *     stream_blocks loop is strictly serial), which avoids the
 *     add_all_parents() infinite-loop bug triggered by concurrent
 *     LOGICAL_INO + dedupe on the same extent. External processes
 *     are the user's responsibility to coordinate.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <linux/btrfs.h>
#include <linux/fiemap.h>
#include <linux/fs.h>		/* FS_IOC_FIEMAP */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*
 * Some kernel uapi headers (including Synology's DSM 7 4.4-based
 * tree) ship the legacy BTRFS_IOC_LOGICAL_INO but not the V2 macro
 * added in upstream 4.15. Define both ourselves if missing so the
 * code below compiles uniformly. Both ioctls use the same arg type
 * (struct btrfs_ioctl_logical_ino_args); V2 just additionally honors
 * the .flags and .reserved fields.
 */
#ifndef BTRFS_IOC_LOGICAL_INO
#define BTRFS_IOC_LOGICAL_INO	_IOWR(BTRFS_IOCTL_MAGIC, 36, \
				      struct btrfs_ioctl_logical_ino_args)
#endif
#ifndef BTRFS_IOC_LOGICAL_INO_V2
#define BTRFS_IOC_LOGICAL_INO_V2 _IOWR(BTRFS_IOCTL_MAGIC, 59, \
				       struct btrfs_ioctl_logical_ino_args)
#endif

#include <sqlite3.h>

#include "csum.h"
#include "dbfile.h"
#include "debug.h"
#include "filerec.h"
#include "lookup_dedupe_internal.h"
#include "srccount_seed.h"

/*
 * Get the physical extent address (devid + physical offset) of the
 * extent containing (fd, loff). One FIEMAP ioctl with a single-extent
 * buffer is enough: we only need the leading extent, the rest of the
 * file's layout is irrelevant.
 *
 * Returns 0 and writes *out_phys on success, errno on failure.
 */
static int fiemap_physical_addr(int fd, uint64_t loff, uint64_t *out_phys)
{
	struct {
		struct fiemap fm;
		struct fiemap_extent fe[1];
	} req;
	int ret;

	memset(&req, 0, sizeof(req));
	req.fm.fm_start = loff;
	req.fm.fm_length = 1;	/* Bytes-of-interest. The kernel returns
				 * the containing extent regardless. */
	req.fm.fm_flags = FIEMAP_FLAG_SYNC;
	req.fm.fm_extent_count = 1;

	ret = ioctl(fd, FS_IOC_FIEMAP, &req);
	if (ret < 0)
		return errno;

	if (req.fm.fm_mapped_extents != 1)
		return ENOENT;

	if (req.fe[0].fe_flags & FIEMAP_EXTENT_UNKNOWN)
		return ENOENT;

	*out_phys = req.fe[0].fe_physical;
	return 0;
}

int srccount_lazy_seed(struct lookup_state *st,
		       int64_t canon_fileid, uint64_t canon_loff,
		       uint32_t cap, int64_t *out_srccount)
{
	struct filerec *ref = NULL;
	int oo;
	uint64_t phys = 0;
	int ret;
	size_t buf_size;
	struct btrfs_data_container *container = NULL;
	struct btrfs_ioctl_logical_ino_args args;
	uint64_t count;

	/* Resolve the canonical's file. Load lazily if not in cache. */
	ref = filerec_find(canon_fileid);
	if (ref == NULL) {
		if (dbfile_load_one_filerec(st->db, canon_fileid, &ref) ||
		    ref == NULL) {
			return ENOENT;
		}
	}

	oo = filerec_open_once(ref, &st->ref_opens);
	if (oo)
		return oo;

	ret = fiemap_physical_addr(ref->fd, canon_loff, &phys);
	if (ret)
		return ret;

	/*
	 * Allocate a btrfs_data_container sized for (cap+10) (root,
	 * inode, offset) triples plus the container header. Each
	 * triple is 3 * sizeof(uint64_t) = 24 B (the kernel writes
	 * them out as bare uint64_t arrays inside the container's
	 * `val` field). The +10 cushion handles the edge case where
	 * the kernel returns slightly more than cap entries before
	 * stopping (it walks in batches).
	 */
	{
		size_t triples = (size_t)cap + 10;
		size_t triple_bytes = triples * 3 * sizeof(uint64_t);
		buf_size = sizeof(struct btrfs_data_container) +
			   triple_bytes;
	}

	container = calloc(1, buf_size);
	if (container == NULL)
		return ENOMEM;

	memset(&args, 0, sizeof(args));
	args.logical = phys;
	args.size = buf_size;
	args.inodes = (uintptr_t)container;
	/*
	 * V2-specific: with no flags set, we ask the kernel to
	 * resolve the extent's leading offset only, which is what
	 * we want for reflink counting. BTRFS_LOGICAL_INO_ARGS_IGNORE_OFFSET
	 * would resolve every reference within the extent regardless
	 * of offset - more work for the same answer for our purposes.
	 */

	/*
	 * Kernel-version-aware dispatch. V2 (4.15+) is preferred
	 * because it stops walking back-references when the buffer
	 * fills, bounding worst-case ioctl cost. V1 (3.7+) walks all
	 * parents regardless of buffer size and is slower on heavily
	 * shared extents, but works on kernels too old for V2 (e.g.
	 * Synology DSM 7's 4.4 base).
	 *
	 * We try V2 first per call until we see ENOTTY / EOPNOTSUPP,
	 * then latch v2_unsupported = true for the rest of the
	 * process so every subsequent call goes straight to V1 with
	 * no wasted syscall. First failure of each kind is logged
	 * once so the user can tell which path is active without
	 * having to strace.
	 */
	{
		static bool v2_unsupported = false;
		static bool v2_unsupported_logged = false;
		static bool first_real_error_logged = false;
		bool tried_v2 = false;

		if (!v2_unsupported) {
			tried_v2 = true;
			ret = ioctl(ref->fd, BTRFS_IOC_LOGICAL_INO_V2,
				    &args);
			if (ret < 0 && (errno == ENOTTY ||
					errno == EOPNOTSUPP)) {
				if (!v2_unsupported_logged) {
					eprintf("lookup: LOGICAL_INO_V2 "
						"not supported on this "
						"kernel (errno=%d); falling "
						"back to V1 for the rest of "
						"this run\n", errno);
					v2_unsupported_logged = true;
				}
				v2_unsupported = true;
				/* Reset args; V2 may have written status
				 * fields even on early failure. */
				memset(&args, 0, sizeof(args));
				args.logical = phys;
				args.size = buf_size;
				args.inodes = (uintptr_t)container;
				memset(container, 0, buf_size);
			}
		}

		if (v2_unsupported) {
			ret = ioctl(ref->fd, BTRFS_IOC_LOGICAL_INO, &args);
		}

		if (ret < 0) {
			ret = errno;
			if (!first_real_error_logged) {
				eprintf("lookup: LOGICAL_INO%s seed failed "
					"(errno=%d: %s) for canonical "
					"(%"PRId64", %"PRIu64") phys=%"PRIu64
					"; further seed errors will be "
					"silent\n",
					tried_v2 && !v2_unsupported ?
						"_V2" : " (V1)",
					ret, strerror(ret),
					canon_fileid, canon_loff, phys);
				first_real_error_logged = true;
			}
			free(container);
			return ret;
		}
	}

	/*
	 * elem_cnt is the number of uint64_t values written (3 per
	 * (root, inode, offset) triple). bytes_left tells us how
	 * many bytes of the buffer were not used; bytes_missing is
	 * how many MORE bytes would have been needed to fit all
	 * entries (non-zero means the buffer truncated).
	 *
	 * What we want is the reflink count, which is the number of
	 * triples = elem_cnt / 3.
	 */
	count = container->elem_cnt / 3;
	if (container->bytes_missing > 0) {
		/*
		 * Truncated buffer. True count is >= count + (bytes_missing /
		 * 24). Cap at cap+10 for our purposes since past that we
		 * know the canonical is over-saturated anyway.
		 */
		uint64_t extra = container->bytes_missing /
				 (3 * sizeof(uint64_t));
		count += extra;
	}

	free(container);

	/* Cap the recorded value at cap+10 to bound future SQL int
	 * values cleanly; any value at or above the cap is treated
	 * identically (skip) by the spillover logic. */
	{
		uint64_t cap_bound = (uint64_t)cap + 10;
		if (count > cap_bound)
			count = cap_bound;
	}

	*out_srccount = (int64_t)count;

	/* Persist to blocks.srccount so future encounters of this
	 * canonical use the seeded value instead of re-running the
	 * ioctl. */
	{
		sqlite3_stmt *upd = st->set_srccount_stmt;
		int rc;

		sqlite3_reset(upd);
		sqlite3_bind_int64(upd, 1, (int64_t)count);
		sqlite3_bind_int64(upd, 2, canon_fileid);
		sqlite3_bind_int64(upd, 3, (int64_t)canon_loff);

		rc = sqlite3_step(upd);
		if (rc != SQLITE_DONE) {
			/*
			 * SQL update failed but we already have the
			 * value in *out_srccount. Caller can still do
			 * the cap check; the only cost is re-running
			 * LOGICAL_INO_V2 next time we encounter this
			 * canonical.
			 */
			eprintf("lookup: srccount UPDATE failed for "
				"canonical (%"PRId64", %"PRIu64"): %d\n",
				canon_fileid, canon_loff, rc);
			return EIO;
		}
	}

	return 0;
}
