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
};

extern struct options options;

#endif	/* __OPT_H__ */
