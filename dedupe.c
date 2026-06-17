/*
 * dedupe.c
 *
 * Copyright (C) 2013 SUSE.  All rights reserved.
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
 * Authors: Mark Fasheh <mfasheh@suse.de>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/vfs.h>
#include <sys/ioctl.h>

#include <errno.h>

#include "kernel.h"
#include "list.h"
#include "filerec.h"
#include "dedupe.h"
#include "debug.h"
#include "btrfs-syno.h"

/*
 * Used to determine if requests must be aligned with the underlying block size
 * If 0, there is no need to align requests
 */
static unsigned int fs_blocksize = 0;

struct dedupe_req {
	struct filerec		*req_file;
	struct list_head	req_list; /* see comment in dedupe.h */

	uint64_t		req_loff;
	uint64_t		req_total; /* total bytes processed by kernel */
	int			req_status;
	int			req_idx; /* index into same->info */
};

static struct dedupe_req *new_dedupe_req(struct filerec *file, uint64_t loff)
{
	struct dedupe_req *req = calloc(1, sizeof(*req));

	if (req) {
		INIT_LIST_HEAD(&req->req_list);
		req->req_file = file;
		req->req_loff = loff;
	}
	return req;
}

static void free_dedupe_req(struct dedupe_req *req)
{
	if (req) {
		if (!list_empty(&req->req_list)) {
			struct filerec *file = req->req_file;

			eprintf("%s: freeing request with nonempty list\n",
				file ? file->filename : "(null)");
			list_del(&req->req_list);
		}
		free(req);
	}
}

static struct dedupe_req *same_idx_to_request(struct dedupe_ctxt *ctxt, int idx)
{
	int i;
	struct dedupe_req *req;
	struct list_head *lists[3] = { &ctxt->queued,
				      &ctxt->in_progress,
				      &ctxt->completed, };

	for (i = 0; i < 3; i++) {
		list_for_each_entry(req, lists[i], req_list) {
			if (req->req_idx == idx)
				return req;
		}
	}

	return NULL;
}

#define _PRE	"(dedupe) "
static void print_btrfs_same_info(struct dedupe_ctxt *ctxt)
{
	int i;
	struct filerec *file = ctxt->ioctl_file;
	struct file_dedupe_range *same = ctxt->same;
	struct file_dedupe_range_info *info;
	struct dedupe_req *req;

	dprintf(_PRE"btrfs same info: ioctl_file: \"%s\"\n",
		file ? file->filename : "(null)");
	dprintf(_PRE"logical_offset: %llu, length: %llu, dest_count: %u\n",
		(unsigned long long)same->src_offset,
		(unsigned long long)same->src_length, same->dest_count);

	for (i = 0; i < same->dest_count; i++) {
		info = &same->info[i];
		req = same_idx_to_request(ctxt, i);
		file = req->req_file;
		dprintf(_PRE"info[%d]: name: \"%s\", fd: %lld, logical_offset: "
			"%llu, bytes_deduped: %llu, status: %d\n",
			i, file ? file->filename : "(null)", (long long)info->dest_fd,
			(unsigned long long)info->dest_offset,
			(unsigned long long)info->bytes_deduped, info->status);
	}
}

static void clear_lists(struct dedupe_ctxt *ctxt)
{
	int i;
	struct list_head *lists[3] = { &ctxt->queued,
				      &ctxt->in_progress,
				      &ctxt->completed, };
	struct dedupe_req *req, *tmp;

	for (i = 0; i < 3; i++) {
		list_for_each_entry_safe(req, tmp, lists[i], req_list) {
			list_del_init(&req->req_list);
			free_dedupe_req(req);
		}
	}
}

void free_dedupe_ctxt(struct dedupe_ctxt *ctxt)
{
	if (ctxt) {
		clear_lists(ctxt);
		if (ctxt->same)
			free(ctxt->same);
		free(ctxt);
	}
}

static unsigned int get_fs_blocksize(int fd)
{
	int ret;
	struct statfs fs;

	ret = fstatfs(fd, &fs);
	if (ret) {
		eprintf("Error %d (\"%s\") while getting fs "
			"blocksize, defaulting to 4096 bytes for this "
			"dedupe.\n", errno, strerror(errno));
		return 4096;
	}
	return fs.f_bsize;
}

struct dedupe_ctxt *new_dedupe_ctxt(unsigned int max_extents, uint64_t loff,
				    uint64_t elen, struct filerec *ioctl_file)
{
	struct dedupe_ctxt *ctxt = calloc(1, sizeof(*ctxt));
	struct file_dedupe_range *same;
	unsigned int same_size;
	unsigned int max_dest_files;

	if (ctxt == NULL)
		return NULL;

	if (max_extents > MAX_DEDUPES_PER_IOCTL)
		max_extents = MAX_DEDUPES_PER_IOCTL;

	max_dest_files = max_extents - 1;

	same_size = sizeof(*same) +
		max_dest_files * sizeof(struct file_dedupe_range_info);
	same = calloc(1, same_size);
	if (same == NULL) {
		free(same);
		free(ctxt);
		return NULL;
	}

	ctxt->same = same;
	ctxt->same_size = same_size;

	ctxt->max_queable = max_dest_files;
	ctxt->len = ctxt->orig_len = elen;
	ctxt->ioctl_file = ioctl_file;
	ctxt->ioctl_file_off = ctxt->orig_file_off = loff;
	INIT_LIST_HEAD(&ctxt->queued);
	INIT_LIST_HEAD(&ctxt->in_progress);
	INIT_LIST_HEAD(&ctxt->completed);

	return ctxt;
}

int add_extent_to_dedupe(struct dedupe_ctxt *ctxt, uint64_t loff,
			 struct filerec *file)
{
	struct dedupe_req *req = new_dedupe_req(file, loff);

	abort_on(ctxt->num_queued >= ctxt->max_queable);

	if (req == NULL)
		return -1;

	list_add_tail(&req->req_list, &ctxt->queued);
	ctxt->num_queued++;

	return ctxt->max_queable - ctxt->num_queued;
}

static void add_dedupe_request(struct dedupe_ctxt *ctxt,
			       struct file_dedupe_range *same,
			       struct dedupe_req *req)
{
	int same_idx = same->dest_count;
	struct file_dedupe_range_info *info;
	struct filerec *file = req->req_file;

	abort_on(same->dest_count >= ctxt->max_queable);

	req->req_idx = same_idx;
	info = &same->info[same_idx];
	info->dest_fd = file->fd;
	info->dest_offset = req->req_loff;
	info->bytes_deduped = 0;
	same->dest_count++;

	dprintf("add ioctl request %s, off: %llu, dest: %d\n", file->filename,
		(unsigned long long)req->req_loff, same->dest_count);
}

static void set_aligned_same_length(struct dedupe_ctxt *ctxt,
				    struct file_dedupe_range *same)
{
	same->src_length = ctxt->len;
	if (fs_blocksize != 0 && ctxt->len > fs_blocksize)
		same->src_length = ctxt->len & ~(fs_blocksize - 1);
}

static void populate_dedupe_request(struct dedupe_ctxt *ctxt,
				    struct file_dedupe_range *same)
{
	struct dedupe_req *req, *tmp;

	memset(same, 0, ctxt->same_size);

	set_aligned_same_length(ctxt, same);
	same->src_offset = ctxt->ioctl_file_off;

	list_for_each_entry_safe(req, tmp, &ctxt->queued, req_list) {
		add_dedupe_request(ctxt, same, req);

		list_move_tail(&req->req_list, &ctxt->in_progress);
		ctxt->num_queued--;
	}
}

/* Returns 1 when there are no more dedupes to process. */
static void process_dedupes(struct dedupe_ctxt *ctxt,
			    struct file_dedupe_range *same)
{
	int same_idx;
	uint64_t max_deduped = 0;
	struct file_dedupe_range_info *info;
	struct dedupe_req *req, *tmp;

	list_for_each_entry_safe(req, tmp, &ctxt->in_progress, req_list) {
		same_idx = req->req_idx;
		info = &same->info[same_idx];

		if (info->bytes_deduped > max_deduped)
			max_deduped = info->bytes_deduped;

		req->req_loff += info->bytes_deduped;
		req->req_total += info->bytes_deduped;

		if (info->status || req->req_total >= ctxt->orig_len) {
			/*
			 * Only bother taking the final status (the
			 * rest will be 0)
			 */
			req->req_status = info->status;
			list_move_tail(&req->req_list, &ctxt->completed);
		} else {
			/*
			 * put us back on the queued list for another
			 * go around
			 */
			list_move_tail(&req->req_list, &ctxt->queued);
			ctxt->num_queued++;
		}
	}

	/* Increment our ioctl file pointers */
	ctxt->len -= max_deduped;
	ctxt->ioctl_file_off += max_deduped;

	if (fs_blocksize != 0 && ctxt->len < fs_blocksize) {
		/*
		 * If we go around again in this situation, we'll just
		 * get -EINVAL on all the fds. Short circuit this then
		 * by moving everything off the queued list.
		 */
		list_splice_init(&ctxt->queued, &ctxt->completed);
	}
}

int dedupe_extents(struct dedupe_ctxt *ctxt)
{
	int ret = 0;

	while (!list_empty(&ctxt->queued)) {
		/* Convert the queued list into an actual request */
		populate_dedupe_request(ctxt, ctxt->same);

retry:
		ret = ioctl(ctxt->ioctl_file->fd, FIDEDUPERANGE, ctxt->same);
		if (ret)
			break;

		if (debug)
			print_btrfs_same_info(ctxt);

		if (ctxt->same->info[0].status == -EINVAL && !fs_blocksize) {
			fs_blocksize = get_fs_blocksize(ctxt->ioctl_file->fd);
			set_aligned_same_length(ctxt, ctxt->same);
			goto retry;
		}

		process_dedupes(ctxt, ctxt->same);
	}

	return ret;
}

/*
 * Returns 1 when we have no more items.
 */
int pop_one_dedupe_result(struct dedupe_ctxt *ctxt, int *status,
			  uint64_t *off, uint64_t *bytes_deduped,
			  struct filerec **file)
{
	struct dedupe_req *req;

	/*
	 * We should not be called if dedupe_extents wasn't called or if
	 * we already passed back all the results..
	 */
	abort_on(list_empty(&ctxt->completed));

	req = list_entry(ctxt->completed.next, struct dedupe_req, req_list);
	list_del_init(&req->req_list);

	*status = req->req_status;
	*off = req->req_loff - req->req_total;
	*bytes_deduped = req->req_total;
	*file = req->req_file;

	free_dedupe_req(req);

	return !!list_empty(&ctxt->completed);
}

/*
 * =============================================================
 * BTRFS_IOC_SYNO_EXTENT_SAME path (Synology-native pairwise dedupe)
 * =============================================================
 *
 * Separate from the FIDEDUPERANGE dedupe_ctxt machinery above
 * because SYNO has a fundamentally different API shape:
 *
 *   - Pairwise only (1 src + 1 dst per call)
 *   - Uses (rootid, objectid) not fds
 *   - Status codes via args.status, signalled via -EMLINK
 *   - DITTO short-circuit on cap-saturated extents
 *   - No queue, no batching, no result drain via process_dedupes
 *
 * See btrfs-syno.h for the protocol details. Phase 0 smoke test
 * (tools/syno_smoke.c) verified ABI and behavior on the user's kernel.
 */

volatile int g_syno_unavailable = 0;

/*
 * Rate-limited unexpected-status logger. log_count grows monotonically;
 * we emit when crossing the powers of 10 thresholds (1, 10, 100, 1000...)
 * so the user sees the first occurrence of each unexpected condition but
 * isn't drowned by repeats.
 *
 * Categories are keyed by status enum value so each unexpected status
 * gets its own counter and threshold ladder.
 */
static unsigned long syno_log_counts[SYNO_EXTENT_SAME_MAX];

static bool syno_should_log(uint8_t status)
{
	unsigned long c, n;

	if (status >= SYNO_EXTENT_SAME_MAX)
		return true;	/* truly weird; log every time */

	c = ++syno_log_counts[status];
	/* Log at counts that are powers of 10 (1, 10, 100, 1000, ...). */
	n = c;
	while (n % 10 == 0)
		n /= 10;
	return n == 1;
}

int syno_dedupe_ids(int ioctl_fd,
		    uint64_t src_rootid, uint64_t src_objectid, uint64_t src_off,
		    uint64_t dst_rootid, uint64_t dst_objectid, uint64_t dst_off,
		    uint64_t length, uint32_t backref_limit,
		    uint32_t min_dedupe_length,
		    uint64_t *out_bytes_deduped,
		    int *out_syno_status,
		    uint64_t *out_release_size)
{
	struct btrfs_ioctl_syno_extent_same_args args;
	uint64_t kern_bytes = 0;
	int ret;

	*out_bytes_deduped = 0;
	*out_syno_status = SYNO_EXTENT_SAME_MAX;	/* sentinel = "no result" */
	if (out_release_size)
		*out_release_size = 0;

	if (g_syno_unavailable)
		return ENOTSUP;

	memset(&args, 0, sizeof(args));
	args.src_rootid        = src_rootid;
	args.src_objectid      = src_objectid;
	args.src_offset        = src_off;
	args.dst_rootid        = dst_rootid;
	args.dst_objectid      = dst_objectid;
	args.dst_offset        = dst_off;
	args.length            = length;
	args.min_dedupe_length = min_dedupe_length;
	args.backref_limit     = backref_limit;
	/* failed_dst_*, release_size, status zeroed by memset */

	/*
	 * Issue the ioctl. The kernel uses ioctl_fd only for fs_info
	 * context and permission checks; src/dst inodes are looked up
	 * via the (rootid, objectid) tuples in args.
	 */
	ret = ioctl(ioctl_fd, BTRFS_IOC_SYNO_EXTENT_SAME, &args);

	if (ret < 0) {
		int saved_errno = errno;

		/* EMLINK = "args.status holds outcome." NOT a failure. */
		if (saved_errno == EMLINK)
			goto interpret_status;

		/* ENOTTY/EOPNOTSUPP = kernel doesn't support this ioctl.
		 * Flip g_syno_unavailable so subsequent calls short-circuit. */
		if (saved_errno == ENOTTY || saved_errno == EOPNOTSUPP) {
			if (!g_syno_unavailable) {
				g_syno_unavailable = 1;
				eprintf("syno_dedupe_ids: kernel does not support "
					"BTRFS_IOC_SYNO_EXTENT_SAME (errno=%d %s); "
					"falling back to FIDEDUPERANGE for the rest "
					"of this run.\n",
					saved_errno, strerror(saved_errno));
			}
			return ENOTSUP;
		}

		eprintf("syno_dedupe_ids: ioctl (src %llu/%llu @ %llu -> "
			"dst %llu/%llu @ %llu, len %llu): %s (errno %d)\n",
			(unsigned long long)src_rootid,
			(unsigned long long)src_objectid,
			(unsigned long long)src_off,
			(unsigned long long)dst_rootid,
			(unsigned long long)dst_objectid,
			(unsigned long long)dst_off,
			(unsigned long long)length,
			strerror(saved_errno), saved_errno);
		return saved_errno;
	}

interpret_status:
	*out_syno_status = args.status;
	if (out_release_size)
		*out_release_size = (uint64_t)args.release_size;

	switch (args.status) {
	case SYNO_EXTENT_SAME_SUCCESS:
		kern_bytes = length;
		break;

	case SYNO_EXTENT_SAME_DITTO:
	case SYNO_EXTENT_SAME_DIFF:
		if ((uint64_t)args.failed_dst_offset >= dst_off)
			kern_bytes = (uint64_t)args.failed_dst_offset - dst_off;
		else
			kern_bytes = 0;
		if (args.status == SYNO_EXTENT_SAME_DIFF && kern_bytes == 0 &&
		    syno_should_log(args.status))
			eprintf("syno_dedupe_ids: DIFF (mismatch from byte 0) "
				"src=%llu/%llu@%llu dst=%llu/%llu@%llu len=%llu "
				"— possible hashfile drift or bitrot.\n",
				(unsigned long long)src_rootid,
				(unsigned long long)src_objectid,
				(unsigned long long)src_off,
				(unsigned long long)dst_rootid,
				(unsigned long long)dst_objectid,
				(unsigned long long)dst_off,
				(unsigned long long)length);
		break;

	case SYNO_EXTENT_SAME_SRC_NOT_FOUND:
	case SYNO_EXTENT_SAME_DST_NOT_FOUND:
		kern_bytes = 0;
		if (syno_should_log(args.status))
			eprintf("syno_dedupe_ids: %s — src=%llu/%llu dst=%llu/%llu\n",
				args.status == SYNO_EXTENT_SAME_SRC_NOT_FOUND ?
					"SRC_NOT_FOUND" : "DST_NOT_FOUND",
				(unsigned long long)src_rootid,
				(unsigned long long)src_objectid,
				(unsigned long long)dst_rootid,
				(unsigned long long)dst_objectid);
		break;

	default:
		kern_bytes = 0;
		eprintf("syno_dedupe_ids: unknown status %u from kernel "
			"(args.status field corrupted or kernel ABI mismatch)\n",
			args.status);
		break;
	}

	*out_bytes_deduped = kern_bytes;
	return 0;
}

int syno_dedupe_pair(struct filerec *src, uint64_t src_off,
		     struct filerec *dst, uint64_t dst_off,
		     uint64_t length, uint32_t backref_limit,
		     uint32_t min_dedupe_length,
		     uint64_t *out_bytes_deduped,
		     int *out_syno_status,
		     uint64_t *out_release_size)
{
	uint64_t src_rootid = 0, src_objectid = 0;
	uint64_t dst_rootid = 0, dst_objectid = 0;
	int ret;

	*out_bytes_deduped = 0;
	*out_syno_status = SYNO_EXTENT_SAME_MAX;
	if (out_release_size)
		*out_release_size = 0;

	if (g_syno_unavailable)
		return ENOTSUP;

	ret = filerec_get_btrfs_ids(src, &src_rootid, &src_objectid);
	if (ret) {
		eprintf("syno_dedupe_pair: filerec_get_btrfs_ids(src=%s): %s\n",
			src->filename, strerror(ret));
		return ret;
	}
	ret = filerec_get_btrfs_ids(dst, &dst_rootid, &dst_objectid);
	if (ret) {
		eprintf("syno_dedupe_pair: filerec_get_btrfs_ids(dst=%s): %s\n",
			dst->filename, strerror(ret));
		return ret;
	}

	return syno_dedupe_ids(src->fd,
			       src_rootid, src_objectid, src_off,
			       dst_rootid, dst_objectid, dst_off,
			       length, backref_limit, min_dedupe_length,
			       out_bytes_deduped, out_syno_status,
			       out_release_size);
}
