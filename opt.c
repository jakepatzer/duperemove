/*
 * opt.c
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

#include "opt.h"

struct options options = {
	.run_dedupe = 0,
	.recurse_dirs = false,
	.io_threads = 0,
	.cpu_threads = 0,
	.skip_zeroes = false,
	.only_whole_files = false,
	.do_block_hash = false,
	.dedupe_same_file = true,
	.batch_size = 1024,
	.fdupes_mode = false,
	.lookup_max_reflinks = 500,
	.no_seed_srccount = false,
	.lookup_fd_cache = 2048,
	.lookup_progress_interval = 10,
	.lookup_start_from = 0,
	.zero_only_dedupe = false,
};
