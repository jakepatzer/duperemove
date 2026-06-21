/*
 * srccount_seed.h
 *
 * Phase 5 helper: lazy LOGICAL_INO_V2 seed for srccount.
 *
 * When a canonical position's srccount is the "not seeded" sentinel
 * (-1), we ask the kernel for the actual reflink count via FIEMAP +
 * LOGICAL_INO_V2 and write it back to the hashfile. Subsequent uses
 * of the same canonical hit the cached value with no kernel calls.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */

#ifndef __SRCCOUNT_SEED_H__
#define __SRCCOUNT_SEED_H__

#include <stdint.h>

struct lookup_state;

/*
 * Resolve the kernel-truth reflink count for the canonical at
 * (canon_fileid, canon_loff). On success writes the count to
 * *out_srccount (capped at "cap+10" for the bounded-buffer
 * LOGICAL_INO_V2 call) and writes the same value back to
 * blocks.srccount via st->inc_srccount_stmt-style UPDATE. Returns
 * 0 on success, errno-style code on failure.
 *
 * On any error (filerec_find/load failure, file open failure,
 * FIEMAP error, LOGICAL_INO_V2 error) the function returns
 * non-zero and *out_srccount is left at the caller's initial
 * value (typically 0, "treat as fresh"). The cap check in
 * stream_blocks degrades gracefully: a failed seed just means we
 * don't get the kernel-truth count, but srccount continues to
 * track from 0 going forward.
 */
int srccount_lazy_seed(struct lookup_state *st,
		       int64_t canon_fileid, uint64_t canon_loff,
		       uint32_t cap, int64_t *out_srccount);

/*
 * Resolve the physical (devid-relative) byte address of the extent
 * containing (fd, loff) via a single FIEMAP ioctl. Returns 0 and writes
 * *out_phys on success, errno on failure. Exposed for the Fix B
 * physical-extent saturation cache populate path in lookup_dedupe.c.
 */
int fiemap_physical_addr(int fd, uint64_t loff, uint64_t *out_phys);

#endif	/* __SRCCOUNT_SEED_H__ */
