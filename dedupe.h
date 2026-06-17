/*
 * dedupe.h
 *
 * Copyright (C) 2016 SUSE.  All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 */

#ifndef	__DEDUPE_H__
#define	__DEDUPE_H__

#include "list.h"
#include "ioctl.h"

#define MAX_DEDUPES_PER_IOCTL	120

struct dedupe_ctxt {

	/*
	 * Starting len/file off saved for the callers convenience -
	 * the ones below can change during dedupe operations.
	 */
	uint64_t	orig_len;
	uint64_t	orig_file_off;

	uint64_t	len;
	struct filerec	*ioctl_file;
	uint64_t	ioctl_file_off;

	/* Next two are used for sanity checking */
	unsigned int		max_queable;
	unsigned int		num_queued;

	unsigned int		same_size;

	/*
	 * request tracking.
	 *	queued: request is awaiting dedupe
	 *	in_progress: currently undergoing dedupe operations
	 *	completed: results of dedupe for this request are available
	 */
	struct list_head	queued;
	struct list_head	in_progress;
	struct list_head	completed;

	struct file_dedupe_range *same;
};

struct dedupe_ctxt *new_dedupe_ctxt(unsigned int max_extents, uint64_t loff,
				    uint64_t elen, struct filerec *ioctl_file);
void free_dedupe_ctxt(struct dedupe_ctxt *ctxt);

/*
 * add_extent_to_dedupe returns:
 *  < 0: error
 * == 0: no more extents after this one
 *  > 0: ok, can accept more extents
 */
int add_extent_to_dedupe(struct dedupe_ctxt *ctxt, uint64_t loff,
			 struct filerec *file);
int dedupe_extents(struct dedupe_ctxt *ctxt);
int pop_one_dedupe_result(struct dedupe_ctxt *ctxt, int *status,
			  uint64_t *off, uint64_t *bytes_deduped,
			  struct filerec **file);

/*
 * BTRFS_IOC_SYNO_EXTENT_SAME submission (Synology DSM 7+ native dedupe).
 *
 * Pairwise: one src filerec + one dst filerec per call (the SYNO ioctl
 * has no multi-dst batching, unlike FIDEDUPERANGE). Lookup-mode and
 * zero-dedupe paths use this when options.use_syno_dedupe != SYNO_OFF.
 *
 * Both src->fd and dst->fd MUST be open before calling. The function
 * looks up (rootid, objectid) via filerec_get_btrfs_ids() (cached after
 * first lookup) and constructs the ioctl args.
 *
 * Outputs:
 *   *out_bytes_deduped — bytes successfully deduped by the kernel.
 *     For SUCCESS: equals `length`. For DITTO/DIFF: equals the matching
 *     prefix `failed_dst_offset - dst_off` (may be 0 if cap/mismatch
 *     hit at the very start). Caller uses this to decide how far to
 *     advance and whether to do srccount/alias_root bookkeeping.
 *   *out_syno_status — the SYNO enum value (SUCCESS/DITTO/DIFF/...).
 *     Used by the caller to bump the right diagnostic counter and to
 *     decide whether already-shared paths warrant special handling.
 *   *out_release_size (optional, NULL OK) — bytes freed from disk by
 *     the kernel. release_size==0 with SUCCESS hints at already-shared
 *     or other-references; diagnostic-only.
 *
 * Returns:
 *    0       — ioctl completed; check *out_syno_status for outcome
 *    ENOTSUP — SYNO ioctl not available on this kernel; g_syno_unavailable
 *              is set as a side effect so the caller can fall back globally
 *    EBADF   — filerec_get_btrfs_ids failed (fd not held)
 *    other   — errno from the ioctl (real failure)
 *
 * The caller is responsible for all post-success bookkeeping (srccount,
 * alias_root, coalesce_record, progress counters). This function only
 * issues the ioctl and translates the result into a kern_bytes/status
 * pair that the existing post-success branch knows how to handle.
 */
int syno_dedupe_pair(struct filerec *src, uint64_t src_off,
		     struct filerec *dst, uint64_t dst_off,
		     uint64_t length, uint32_t backref_limit,
		     uint32_t min_dedupe_length,
		     uint64_t *out_bytes_deduped,
		     int *out_syno_status,
		     uint64_t *out_release_size);

/*
 * Lower-level SYNO_EXTENT_SAME submission that takes pre-resolved
 * (rootid, objectid) tuples rather than filerecs. Used by zero_dedupe
 * which manages its own fds outside the filerec abstraction.
 *
 *   ioctl_fd: any open fd on the same filesystem (used by kernel for
 *             fs_info context; not the src or dst itself necessarily)
 *
 * Other arguments and return semantics are identical to syno_dedupe_pair.
 *
 * Diagnostic logging uses generic identifiers (rootid/objectid pairs)
 * rather than filenames since we don't have filerec context.
 */
int syno_dedupe_ids(int ioctl_fd,
		    uint64_t src_rootid, uint64_t src_objectid, uint64_t src_off,
		    uint64_t dst_rootid, uint64_t dst_objectid, uint64_t dst_off,
		    uint64_t length, uint32_t backref_limit,
		    uint32_t min_dedupe_length,
		    uint64_t *out_bytes_deduped,
		    int *out_syno_status,
		    uint64_t *out_release_size);

/*
 * Process-global flag set when SYNO_EXTENT_SAME is detected as
 * unsupported (ioctl returns -ENOTTY or -EOPNOTSUPP). Once set, callers
 * should bypass syno_dedupe_pair and use the FIDEDUPERANGE ctxt path.
 * Read-only after the first SYNO call; safe to read lockless.
 */
extern volatile int g_syno_unavailable;

#endif	/* __DEDUPE_H__ */
