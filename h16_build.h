/*
 * h16_build.h
 *
 * One-shot build operation that populates the blocks_h16 secondary
 * index from an already-populated blocks table. Each h16 entry covers
 * a 16 KB rolling window starting at a (fileid, loff) anchor; h16 is
 * XXH128 of the four consecutive 4 KB block digests.
 *
 * Resumable across crashes via blocks_h16_build_progress. Periodic
 * WAL checkpointing keeps disk usage bounded; aborts cleanly if free
 * space on the hashfile's volume drops below a safety threshold.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */

#ifndef __H16_BUILD_H__
#define __H16_BUILD_H__

struct dbhandle;

/*
 * Build (or resume) the blocks_h16 index for an open RW hashfile.
 * Reads blocksize from the hashfile config. Walks files.id in
 * ascending order; skips fileids already present in
 * blocks_h16_build_progress. Returns 0 on success, errno-style code
 * on error.
 */
int h16_build_index(struct dbhandle *db);

#endif	/* __H16_BUILD_H__ */
