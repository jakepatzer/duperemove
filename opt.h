/*
 * opt.h
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 */

#ifndef	__OPT_H__
#define	__OPT_H__

#include <stdbool.h>
#include <stdint.h>

struct options {
	int run_dedupe;
	bool recurse_dirs : 1;
	unsigned int io_threads;
	unsigned int cpu_threads;
	bool skip_zeroes : 1;
	bool only_whole_files : 1;
	bool do_block_hash : 1;
	bool dedupe_same_file : 1;
	unsigned int batch_size;
	bool fdupes_mode : 1;
	char *hashfile;

	/*
	 * Contiguous range coalescing (off by default; --coalesce).
	 * When enabled, a seed block match is extended forward by
	 * direct byte comparison and submitted as a single large
	 * FIDEDUPERANGE range instead of many per-block ioctls.
	 */
	bool coalesce : 1;
	uint64_t min_dedupe_size;	/* skip coalesced ranges shorter
					 * than this (0 = no minimum) */
	char *dedupe_target_priority;	/* path prefix that, if matched,
					 * forces an extent to be the
					 * dedupe target */

	/*
	 * Streaming lookup mode (--lookup-only). The hashfile is
	 * treated as a read-only reference dictionary; files already
	 * present in the hashfile are skipped, files not present are
	 * stream-hashed block-by-block and each digest is looked up
	 * against the hashfile in memory. No writes to the hashfile.
	 */
	bool lookup_only : 1;

	/*
	 * --lookup-self: requires --lookup-only. Causes files that
	 * are already in the hashfile (the "reference" set) to also
	 * be stream-processed, enabling within-image and cross-image
	 * dedupe of the reference corpus itself. Self-identity
	 * matches (a block matching its own row in the hashfile) are
	 * filtered out; legitimate same-fileid-different-loff matches
	 * are kept.
	 */
	bool lookup_self : 1;

	/*
	 * Cap on reflinks added to any single canonical position by
	 * --lookup-only during a run. When a candidate's canonical
	 * srccount reaches this value the candidate is skipped and
	 * we try the next h16 hit ("cap-aware spillover"). Prevents
	 * accumulating thousands of reflinks on a single physical
	 * extent, which on btrfs causes pathological metadata growth
	 * and (on older kernels) performance cliffs.
	 *
	 * Set with --lookup-max-reflinks=N (default 500).
	 */
	uint32_t lookup_max_reflinks;

	/*
	 * --no-seed-srccount: disable the lazy LOGICAL_INO_V2 seed
	 * pass that initializes srccount from kernel-truth on first
	 * use of a canonical position. Useful for "pristine" hashfiles
	 * with no pre-existing reflinks (no rmlint, no snapshots, no
	 * prior dedupe runs) where srccount starting at 0 is correct
	 * and the LOGICAL_INO_V2 calls are pure overhead.
	 */
	bool no_seed_srccount : 1;

	/*
	 * --lookup-fd-cache=N: cap on simultaneously-open reference
	 * filerec FDs during --lookup-only / --lookup-self. Bounded
	 * LRU eviction prevents EMFILE on hashfiles containing many
	 * small files. Default 2048; sits comfortably below typical
	 * NOFILE hard limits (e.g., Synology DSM ships 4096).
	 *
	 * Higher values reduce re-open churn if the working set is
	 * larger than 2048 files; raise the process NOFILE limit via
	 * `prlimit --nofile=N` or equivalent before bumping this.
	 */
	uint32_t lookup_fd_cache;

	/*
	 * --lookup-progress-interval=N: seconds between throttled
	 * progress emits. Default 10. Lower for live monitoring,
	 * higher to keep run logs compact (each line is ~250 bytes;
	 * 2s emits add up to MB of log per hour).
	 */
	uint32_t lookup_progress_interval;
};

extern struct options options;

#endif	/* __OPT_H__ */
