/*
 * duperemove.c
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

#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>
#include <getopt.h>
#include <inttypes.h>

#include <glib.h>

#include "list.h"
#include "csum.h"
#include "filerec.h"
#include "hash-tree.h"
#include "results-tree.h"
#include "dedupe.h"
#include "util.h"
#include "btrfs-util.h"
#include "dbfile.h"
#include "memstats.h"
#include "debug.h"
#include "progress.h"
#include "file_scan.h"
#include "find_dupes.h"
#include "run_dedupe.h"
#include "lookup_dedupe.h"
#include "h16_build.h"

#include "opt.h"

unsigned int blocksize = DEFAULT_BLOCKSIZE;

static int stdin_filelist = 0;
static unsigned int list_only_opt = 0;
static unsigned int rm_only_opt = 0;
static unsigned int build_h16_index_opt = 0;
static unsigned int reset_lookup_state_opt = 0;
struct dbfile_config dbfile_cfg;

static enum {
	H_READ,
	H_WRITE,
	H_UPDATE,
} use_hashfile = H_UPDATE;

static void print_file(char *filename, char *ino, char *subvol)
{
	if (verbose)
		printf("%s\t%s\t%s\n", filename, ino, subvol);
	else
		printf("%s\n", filename);
}

static int list_db_files(char *filename)
{
	int ret;

	_cleanup_(sqlite3_close_cleanup) struct dbhandle *db = dbfile_open_handle(filename);
	if (!db) {
		eprintf("Error: Could not open \"%s\"\n", filename);
		return -1;
	}

	ret = dbfile_iter_files(db, &print_file);
	return ret;
}

static void rm_db_files_from_stdin(struct dbhandle *db)
{
	_cleanup_(freep) char *path = NULL;
	size_t pathlen = 0;
	ssize_t readlen;

	while ((readlen = getline(&path, &pathlen, stdin)) != -1) {
		if (readlen == 0)
			continue;

		if (readlen > 0 && path[readlen - 1] == '\n') {
			path[--readlen] = '\0';
		}

		if (readlen > PATH_MAX - 1) {
			eprintf("Path max exceeded: %s\n", path);
			continue;
		}

		dbfile_remove_file(db, path);
	}
}

static int rm_db_files(int numfiles, char **files)
{
	int i, ret;
	_cleanup_(sqlite3_close_cleanup) struct dbhandle *db = dbfile_open_handle(options.hashfile);
	if (!db) {
		eprintf("Error: Could not open \"%s\"\n", options.hashfile);
		return -1;
	}

	for (i = 0; i < numfiles; i++) {
		const char *name = files[i];

		if (strlen(name) == 1 && name[0] == '-')
			rm_db_files_from_stdin(db);

		ret = dbfile_remove_file(db, name);
		if (ret == 0)
			vprintf("Removed \"%s\" from hashfile.\n", name);

		if (ret)
			printf("ret ?\n");
	}
	return 0;
}

static void print_version(void)
{
	char *s = NULL;
#ifdef	DEBUG_BUILD
	s = " (debug build)";
#endif
	printf("duperemove %s%s\n", VERSTRING, s ? s : "");
}

/* adapted from ocfs2-tools */
static int parse_dedupe_opts(const char *raw_opts)
{
	_cleanup_(freep) char *opts;
	char *token, *next, *p, *arg;
	int print_usage = false;
	int invert, ret = 0;

	opts = strdup(raw_opts);

	for (token = opts; token && *token; token = next) {
		p = strchr(token, ',');
		next = NULL;
		invert = 0;

		if (p) {
			*p = '\0';
			next = p + 1;
		}

		arg = strstr(token, "no");
		if (arg == token) {
			invert = 1;
			token += strlen("no");
		}

		if (strcmp(token, "same") == 0) {
			options.dedupe_same_file = !invert;
		} else if (strcmp(token, "partial") == 0) {
			options.do_block_hash = !invert;
		} else if (strcmp(token, "only_whole_files") == 0) {
			options.only_whole_files = !invert;
		} else {
			print_usage = true;
			break;
		}
	}

	if (print_usage) {
		eprintf("Bad dedupe options specified. Valid dedupe "
			"options are:\n"
			"\t[no]same\n"
			"\t[no]only_whole_files\n"
			"\t[no]partial\n");
		ret = EINVAL;
	}

	return ret;
}

enum {
	DEBUG_OPTION = CHAR_MAX + 1,
	HELP_OPTION,
	VERSION_OPTION,
	WRITE_HASHES_OPTION,
	READ_HASHES_OPTION,
	HASHFILE_OPTION,
	IO_THREADS_OPTION,
	CPU_THREADS_OPTION,
	SKIP_ZEROES_OPTION,
	FDUPES_OPTION,
	DEDUPE_OPTS_OPTION,
	QUIET_OPTION,
	EXCLUDE_OPTION,
	BATCH_SIZE_OPTION,
	COALESCE_OPTION,
	MIN_DEDUPE_SIZE_OPTION,
	DEDUPE_TARGET_PRIORITY_OPTION,
	LOOKUP_ONLY_OPTION,
	LOOKUP_SELF_OPTION,
	BUILD_H16_INDEX_OPTION,
	LOOKUP_MAX_REFLINKS_OPTION,
	NO_SEED_SRCCOUNT_OPTION,
	RESET_LOOKUP_STATE_OPTION,
};

static int process_fdupes(void)
{
	int ret = 0;
	_cleanup_(freep) char *path = NULL;
	size_t pathlen = 0;
	ssize_t readlen;

	while ((readlen = getline(&path, &pathlen, stdin)) != -1) {
		if (readlen == 0)
			continue;

		if (readlen == 1 && path[0] == '\n') {
			ret = fdupes_dedupe();
			if (ret)
				return ret;
			free_all_filerecs();
			continue;
		}

		if (readlen > 0 && path[readlen - 1] == '\n') {
			path[--readlen] = '\0';
		}

		if (readlen > PATH_MAX - 1) {
			eprintf("Path max exceeded: %s\n", path);
			continue;
		}

		add_file_fdupes(path);
	}

	return 0;
}

static int add_files_from_stdin(struct dbhandle *db)
{
	_cleanup_(freep) char *path = NULL;
	size_t pathlen = 0;
	ssize_t readlen;

	while ((readlen = getline(&path, &pathlen, stdin)) != -1) {
		if (readlen == 0)
			continue;

		if (readlen > 0 && path[readlen - 1] == '\n') {
			path[--readlen] = '\0';
		}

		if (readlen > PATH_MAX - 1) {
			eprintf("Path max exceeded: %s\n", path);
			continue;
		}

		if (scan_file(path, db)) {
			eprintf("Error: cannot add %s into the lookup list\n",
				path);
			return 1;
		}
	}

	return 0;
}

static int scan_files_from_cmdline(int numfiles, char **files, struct dbhandle *db)
{
	int i;

	for (i = 0; i < numfiles; i++) {
		char *name = files[i];

		if (scan_file(name, db)) {
			eprintf("Error: cannot scan %s\n", name);
			return 1;
		}
	}

	return 0;
}

static void help(void)
{
	execlp("man", "man", "8", "duperemove", NULL);
}

/*
 * Ok this is doing more than just parsing options.
 */
static int parse_options(int argc, char **argv, int *filelist_idx)
{
	int c, numfiles;
	bool read_hashes = false;
	bool write_hashes = false;
	bool update_hashes = false;

	static struct option long_ops[] = {
		{ "debug", 0, NULL, DEBUG_OPTION },
		{ "help", 0, NULL, HELP_OPTION },
		{ "version", 0, NULL, VERSION_OPTION },
		{ "write-hashes", 1, NULL, WRITE_HASHES_OPTION },
		{ "read-hashes", 1, NULL, READ_HASHES_OPTION },
		{ "hashfile", 1, NULL, HASHFILE_OPTION },
		{ "io-threads", 1, NULL, IO_THREADS_OPTION },
		{ "hash-threads", 1, NULL, IO_THREADS_OPTION },
		{ "cpu-threads", 1, NULL, CPU_THREADS_OPTION },
		{ "skip-zeroes", 0, NULL, SKIP_ZEROES_OPTION },
		{ "fdupes", 0, NULL, FDUPES_OPTION },
		{ "dedupe-options=", 1, NULL, DEDUPE_OPTS_OPTION },
		{ "quiet", 0, NULL, QUIET_OPTION },
		{ "exclude", 1, NULL, EXCLUDE_OPTION },
		{ "batchsize", 1, NULL, BATCH_SIZE_OPTION },
		{ "coalesce", 0, NULL, COALESCE_OPTION },
		{ "min-dedupe-size", 1, NULL, MIN_DEDUPE_SIZE_OPTION },
		{ "dedupe-target-priority", 1, NULL,
		  DEDUPE_TARGET_PRIORITY_OPTION },
		{ "lookup-only", 0, NULL, LOOKUP_ONLY_OPTION },
		{ "lookup-self", 0, NULL, LOOKUP_SELF_OPTION },
		{ "build-h16-index", 0, NULL, BUILD_H16_INDEX_OPTION },
		{ "lookup-max-reflinks", 1, NULL, LOOKUP_MAX_REFLINKS_OPTION },
		{ "no-seed-srccount", 0, NULL, NO_SEED_SRCCOUNT_OPTION },
		{ "reset-lookup-state", 0, NULL, RESET_LOOKUP_STATE_OPTION },
		{ NULL, 0, NULL, 0}
	};

	if (argc < 2) {
		help(); /* Never returns */
	}

	while ((c = getopt_long(argc, argv, "b:vdDrh?LRqB:", long_ops, NULL))
	       != -1) {
		switch (c) {
		case 'b':
			blocksize = parse_size(optarg);
			if (blocksize < MIN_BLOCKSIZE ||
			    blocksize > MAX_BLOCKSIZE){
				eprintf("Error: Blocksize is bounded by %u and %u, %u found\n",
					MIN_BLOCKSIZE, MAX_BLOCKSIZE, blocksize);
				return EINVAL;
			}
			break;
		case 'd':
		case 'D':
			options.run_dedupe += 1;
			break;
		case 'r':
			options.recurse_dirs = true;
			break;
		case VERSION_OPTION:
			print_version();
			exit(0);
		case DEBUG_OPTION:
			debug = 1;
			verbose = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		case 'h':
			human_readable = 1;
			break;
		case WRITE_HASHES_OPTION:
			write_hashes = true;
			options.hashfile = strdup(optarg);
			break;
		case READ_HASHES_OPTION:
			read_hashes = true;
			options.hashfile = strdup(optarg);
			break;
		case HASHFILE_OPTION:
			update_hashes = true;
			options.hashfile = strdup(optarg);
			break;
		case IO_THREADS_OPTION:
			options.io_threads = strtoul(optarg, NULL, 10);
			if (!options.io_threads){
				eprintf("Error: --io-threads must be "
					"an integer, %s found\n", optarg);
				return EINVAL;
			}
			break;
		case CPU_THREADS_OPTION:
			options.cpu_threads = strtoul(optarg, NULL, 10);
			if (!options.cpu_threads){
				eprintf("Error: --cpu-threads must be "
					"an integer, %s found\n", optarg);
				return EINVAL;
			}
			break;
		case SKIP_ZEROES_OPTION:
			options.skip_zeroes = true;
			break;
		case FDUPES_OPTION:
			options.fdupes_mode = 1;
			break;
		case DEDUPE_OPTS_OPTION:
			if (parse_dedupe_opts(optarg))
				return EINVAL;
			break;
		case 'L':
			list_only_opt = 1;
			break;
		case 'R':
			rm_only_opt = 1;
			break;
		case QUIET_OPTION:
		case 'q':
			quiet = 1;
			break;
		case EXCLUDE_OPTION:
			if (add_exclude_pattern(optarg))
				eprintf("Error: cannot exclude %s\n", optarg);
			break;
		case BATCH_SIZE_OPTION:
		case 'B':
			options.batch_size = parse_size(optarg);
			break;
		case COALESCE_OPTION:
			options.coalesce = true;
			break;
		case MIN_DEDUPE_SIZE_OPTION:
			options.min_dedupe_size = parse_size(optarg);
			break;
		case DEDUPE_TARGET_PRIORITY_OPTION:
			options.dedupe_target_priority = strdup(optarg);
			if (options.dedupe_target_priority == NULL) {
				eprintf("Error: out of memory parsing "
					"--dedupe-target-priority\n");
				return ENOMEM;
			}
			break;
		case LOOKUP_ONLY_OPTION:
			options.lookup_only = true;
			break;
		case LOOKUP_SELF_OPTION:
			options.lookup_self = true;
			break;
		case BUILD_H16_INDEX_OPTION:
			build_h16_index_opt = 1;
			break;
		case LOOKUP_MAX_REFLINKS_OPTION: {
			unsigned long v;
			char *endp = NULL;
			v = strtoul(optarg, &endp, 10);
			if (endp == optarg || *endp != '\0' || v == 0 ||
			    v > UINT32_MAX) {
				eprintf("Error: --lookup-max-reflinks "
					"must be a positive integer "
					"(got \"%s\").\n", optarg);
				return EINVAL;
			}
			options.lookup_max_reflinks = (uint32_t)v;
			break;
		}
		case NO_SEED_SRCCOUNT_OPTION:
			options.no_seed_srccount = true;
			break;
		case RESET_LOOKUP_STATE_OPTION:
			reset_lookup_state_opt = 1;
			break;
		case HELP_OPTION:
			help();
			break;
		case '?':
		default:
			return 1;
		}
	}

	numfiles = argc - optind;

	if (options.only_whole_files && options.do_block_hash) {
		eprintf("Error: using both only_whole_files and partial "
			"options have no meaning\n");
		return 1;
	}

	if (options.min_dedupe_size && !options.coalesce)
		eprintf("Warning: --min-dedupe-size has no effect without "
			"--coalesce; ignoring.\n");

	if (options.lookup_only) {
		if (options.hashfile == NULL) {
			eprintf("Error: --lookup-only requires --hashfile.\n");
			return EINVAL;
		}
		if (options.fdupes_mode) {
			eprintf("Error: --lookup-only is incompatible with "
				"--fdupes.\n");
			return EINVAL;
		}
		if (list_only_opt || rm_only_opt) {
			eprintf("Error: --lookup-only is incompatible with "
				"-L and -R.\n");
			return EINVAL;
		}
		if (options.batch_size)
			eprintf("Warning: --batchsize has no meaning in "
				"--lookup-only mode (no batching); "
				"ignoring.\n");
	}

	if (options.lookup_self && !options.lookup_only) {
		eprintf("Error: --lookup-self requires --lookup-only.\n");
		return EINVAL;
	}

	if (build_h16_index_opt) {
		if (options.hashfile == NULL) {
			eprintf("Error: --build-h16-index requires "
				"--hashfile.\n");
			return EINVAL;
		}
		if (options.lookup_only || options.fdupes_mode ||
		    list_only_opt || rm_only_opt) {
			eprintf("Error: --build-h16-index is a standalone "
				"operation; it cannot be combined with "
				"--lookup-only, --fdupes, -L, or -R.\n");
			return EINVAL;
		}
		if (write_hashes || read_hashes) {
			eprintf("Error: --build-h16-index is incompatible "
				"with --write-hashes and --read-hashes. "
				"Build h16 against an already-populated "
				"hashfile (use --hashfile= to specify it).\n");
			return EINVAL;
		}
		/*
		 * --hashfile= sets update_hashes=true earlier in the
		 * parse, which would normally route to the scan + dedupe
		 * pipeline in main(). Clear it so we don't accidentally
		 * follow that path; --build-h16-index is its own branch
		 * in main() and takes precedence over use_hashfile.
		 */
		update_hashes = false;
	}

	if (reset_lookup_state_opt) {
		if (options.hashfile == NULL) {
			eprintf("Error: --reset-lookup-state requires "
				"--hashfile.\n");
			return EINVAL;
		}
		if (options.lookup_only || options.fdupes_mode ||
		    list_only_opt || rm_only_opt || build_h16_index_opt) {
			eprintf("Error: --reset-lookup-state is a "
				"standalone operation; it cannot be "
				"combined with --lookup-only, --fdupes, "
				"-L, -R, or --build-h16-index.\n");
			return EINVAL;
		}
		if (write_hashes || read_hashes) {
			eprintf("Error: --reset-lookup-state is "
				"incompatible with --write-hashes and "
				"--read-hashes.\n");
			return EINVAL;
		}
		/* Same rationale as --build-h16-index above: clear
		 * update_hashes so we don't fall into the scan +
		 * dedupe pipeline in main(). */
		update_hashes = false;
	}

	/* Filter out option combinations that don't make sense. */
	if ((write_hashes + read_hashes + update_hashes) > 1) {
		eprintf("Error: Specify only one hashfile option.\n");
		return 1;
	}

	if (read_hashes)
		use_hashfile = H_READ;
	else if (write_hashes)
		use_hashfile = H_WRITE;
	else if (update_hashes)
		use_hashfile = H_UPDATE;

	/*
	 * Always add the hashfile and its wal etc to the exclude list
	 * A wildcard would be easier but may exclude extra files silently,
	 * this would be confusing for the user.
	 */
	if (options.hashfile != NULL) {
		char tmp[PATH_MAX + 10 ] = {0,};
		add_exclude_pattern(options.hashfile);
		snprintf(tmp, PATH_MAX + 9, "%s-wal", options.hashfile);
		add_exclude_pattern(tmp);
		snprintf(tmp, PATH_MAX + 9, "%s-shm", options.hashfile);
		add_exclude_pattern(tmp);
	}

	if (read_hashes) {
		if (numfiles) {
			eprintf("Error: --read-hashes option does not take a "
				"file list argument\n");
			return 1;
		}
		goto out_nofiles;
	}

	if (options.fdupes_mode) {
		if (read_hashes || write_hashes || update_hashes) {
			eprintf("Error: cannot mix hashfile option with "
				"--fdupes option\n");
			return 1;
		}

		if (numfiles) {
			eprintf("Error: fdupes option does not take a file "
				"list argument\n");
			return 1;
		}
		/* rest of fdupes mode is implemented in main() */
		return 0;
	}

	*filelist_idx = optind;
	if (numfiles == 1 && strcmp(argv[optind], "-") == 0)
		stdin_filelist = 1;

	if (list_only_opt && rm_only_opt) {
		eprintf("Error: Can not mix '-L' and '-R' options.\n");
		return 1;
	}

	if (list_only_opt || rm_only_opt) {
		if (!options.hashfile || use_hashfile == H_WRITE) {
			eprintf("Error: --hashfile= option is required "
				"with '-L' or -R.\n");
			return 1;
		}

		if (list_only_opt && numfiles) {
			eprintf("Error: -L option do not take "
				"a file list argument\n");
			return 1;
		}
	}

	if (!(options.fdupes_mode || list_only_opt ||
	      build_h16_index_opt || reset_lookup_state_opt)
			&& numfiles == 0) {
		eprintf("Error: a file list argument is required.\n");
		return 1;
	}

	if (reset_lookup_state_opt && numfiles > 0) {
		eprintf("Warning: --reset-lookup-state does not take a "
			"file list argument; ignoring %d argument(s).\n",
			numfiles);
	}

	if (build_h16_index_opt && numfiles > 0) {
		eprintf("Warning: --build-h16-index does not take a file "
			"list argument; ignoring.\n");
	}

out_nofiles:
	return 0;
}

static void print_header(void)
{
	vprintf("Using %uK blocks\n", blocksize / 1024);
	vprintf("Using %s hashing\n", options.do_block_hash ? "block+extent" : "extent");
#ifdef	DEBUG_BUILD
	printf("Debug build, performance may be impacted.\n");
#endif
	qprintf("Gathering file list...\n");
}

static void __process_duplicates(struct dbhandle *db, unsigned int seq)
{
	int ret;
	struct results_tree res;
	struct hash_tree dups_tree;

	init_results_tree(&res);
	init_hash_tree(&dups_tree);

	qprintf("Loading only identical files from hashfile.\n");
	ret = dbfile_load_same_files(db, &res, seq + 1);
	if (ret)
		goto out;

	if (options.run_dedupe)
		dedupe_results(&res, true);
	else
		print_dupes_table(&res, true);

	/* Reset the results_tree before loading extents or blocks */
	free_results_tree(&res);

	if (!options.only_whole_files) {
		init_results_tree(&res);

		qprintf("Loading only duplicated hashes from hashfile.\n");

		ret = dbfile_load_extent_hashes(db, &res, seq + 1);
		if (ret)
			goto out;

		printf("Found %llu identical extents.\n", res.num_extents);
		if (options.do_block_hash) {
			ret = dbfile_load_block_hashes(db, &dups_tree, seq + 1);
			if (ret)
				goto out;

			ret = find_additional_dedupe(&res);
			if (ret)
				goto out;
		}

		if (options.run_dedupe)
			dedupe_results(&res, false);
		else
			print_dupes_table(&res, false);
	}

out:
	free_results_tree(&res);
	free_hash_tree(&dups_tree);
}

static void process_duplicates(struct dbhandle *db)
{
	unsigned int max = get_max_dedupe_seq(db);

	/* Spawn a dedicated thread pool to block-based lookup */
	if (options.do_block_hash)
		extents_search_init();

	for (unsigned int i = dedupe_seq; i < max; i++) {
		/* Drop all filerecs from the previous iteration. Needed filerecs will be
		 * recreated by __process_duplicates()
		 */
		free_all_filerecs();
		__process_duplicates(db, i);

		if (options.run_dedupe) {
			/*
			 * Bump dedupe_seq, this effectively marks the files
			 * in our hashfile as having been through dedupe.
			 */
			dedupe_seq++;
			dbfile_cfg.dedupe_seq = dedupe_seq;
			dbfile_cfg.blocksize = blocksize;
			dbfile_sync_config(db, &dbfile_cfg);
		}
	}

	if (options.do_block_hash)
		extents_search_free();
}

static int scan_files(int argc, char **argv, int filelist_idx, struct dbhandle *db)
{
	int ret;

	filescan_init();
	if (!quiet)
		pscan_run();

	if (stdin_filelist)
		ret = add_files_from_stdin(db);
	else
		ret = scan_files_from_cmdline(argc - filelist_idx,
					     &argv[filelist_idx], db);

	pscan_finish_listing();
	filescan_free();
	if (!quiet)
		pscan_join();

	if (ret)
		return ret;

	/*
	 * Sync the locked filesystem informations in the hashfile
	 */
	fs_get_locked_uuid(&(dbfile_cfg.fs_uuid));
	ret = dbfile_sync_config(db, &dbfile_cfg);
	if (ret)
		return ret;

	return 0;
}

int main(int argc, char **argv)
{
	int ret, filelist_idx = 0;
	_cleanup_(sqlite3_close_cleanup) struct dbhandle *db = NULL;

	char stdbuf[BUFSIZ];
	setvbuf(stdout, stdbuf, _IOLBF, BUFSIZ);

	init_filerec();

	/* Set the default CPU limits before parsing the user options */
	get_num_cpus(&(options.cpu_threads), &(options.io_threads));

	ret = parse_options(argc, argv, &filelist_idx);
	if (ret) {
		exit(1);
	}

	/* Allow larger than unusal amount of open files. On linux
	 * this should bw increase form 1K to 512K open files
	 * simultaneously.
	 *
	 * On multicore SSD machines it's not hard to get to 1K open
	 * files.
	 */
	increase_limits();

	if (options.fdupes_mode)
		return process_fdupes();

	if (list_only_opt)
		return list_db_files(options.hashfile);
	else if (rm_only_opt)
		return rm_db_files(argc - filelist_idx, &argv[filelist_idx]);

	if (build_h16_index_opt) {
		/*
		 * --build-h16-index is a standalone one-shot operation
		 * that builds the secondary h16 lookup index on an
		 * already-populated hashfile. Opens RW (the schema
		 * migration in dbfile_prepare adds the new tables and
		 * columns if they aren't already present) and exits
		 * when the build completes or fails. Resumable: a
		 * crashed or aborted build leaves blocks_h16 and
		 * blocks_h16_build_progress in a consistent state and
		 * a subsequent invocation picks up where the previous
		 * one stopped.
		 */
		db = dbfile_open_handle(options.hashfile);
		if (!db)
			goto out;

		dbfile_set_gdb(db);

		ret = h16_build_index(db);
		goto out;
	}

	if (reset_lookup_state_opt) {
		/*
		 * --reset-lookup-state is a standalone one-shot
		 * operation that wipes Phase 4/5 per-position state
		 * (srccount, alias_root_*) on the blocks table.
		 * Opens RW; runs one UPDATE; commits + truncates the
		 * WAL; exits. Use after fixing a seed-side bug to
		 * force fresh kernel-truth seeds on the next
		 * --lookup-only run.
		 */
		db = dbfile_open_handle(options.hashfile);
		if (!db)
			goto out;

		dbfile_set_gdb(db);

		ret = lookup_reset_state(db);
		goto out;
	}

	if (options.lookup_only) {
		/*
		 * --lookup-only opens the hashfile strictly read-only
		 * and bypasses the hash + find-dupes + batch dedupe
		 * pipeline entirely. The hashfile is treated as a
		 * pre-built reference dictionary; lookup files are
		 * stream-processed against it.
		 */
		db = dbfile_open_handle_readonly(options.hashfile);
		if (!db)
			goto out;

		dbfile_set_gdb(db);

		ret = dbfile_get_config(db->db, &dbfile_cfg);
		if (ret)
			goto out;

		if (blocksize != dbfile_cfg.blocksize) {
			eprintf("Error: block size (%u) does not match the "
				"hashfile (%u). Pass -b %u to match.\n",
				blocksize, dbfile_cfg.blocksize,
				dbfile_cfg.blocksize);
			ret = EINVAL;
			goto out;
		}

		print_header();
		ret = lookup_dedupe_main(db, argc, argv, filelist_idx);
		goto out;
	}

	db = dbfile_open_handle(options.hashfile);
	if (!db)
		goto out;

	dbfile_set_gdb(db);

	ret = dbfile_get_config(db->db, &dbfile_cfg);
	if (ret)
		goto out;

	dedupe_seq = dbfile_cfg.dedupe_seq;

	print_header();

	if (use_hashfile == H_WRITE || use_hashfile == H_UPDATE) {
		/*
		 * Bulk-load pattern for --write-hashes: drop the digest
		 * indexes on blocks/extents up front, rebuild them once
		 * at the end. Digests are uniformly random, so without
		 * this each INSERT during scan thrashes random leaf
		 * pages of a multi-GB index that doesn't fit in the
		 * SQLite page cache, and per-file commit time grows
		 * unboundedly with hashfile size.
		 *
		 * The drop runs BEFORE dbfile_prune_unscanned_files so
		 * the prune's FK cascade through blocks/extents (rows
		 * deleted for files with digest IS NULL, e.g. files
		 * left half-scanned by a prior kill) is not paying
		 * per-row digest-index maintenance. The cascade still
		 * uses idx_blocks_fileid / idx_extents_fileid, which
		 * are deliberately NOT dropped (see dbfile.c).
		 *
		 * Restricted to H_WRITE: H_UPDATE is the incremental
		 * path where dropping/rebuilding indexes on an
		 * already-populated hashfile may be a net loss versus
		 * letting the existing indexes absorb the smaller set
		 * of new inserts.
		 *
		 * The rebuild is attempted unconditionally after
		 * scan_files() so the hashfile is left in a state
		 * --lookup-only accepts even when scan fails partway
		 * through. The original scan error code is preserved.
		 */
		if (use_hashfile == H_WRITE) {
			ret = dbfile_drop_bulk_load_indexes(db->db);
			if (ret)
				goto out;
		}

		ret = dbfile_prune_unscanned_files(db);
		if (ret) {
			eprintf("Unable to prune unscanned files\n");
			if (use_hashfile == H_WRITE) {
				int idx_ret = dbfile_create_bulk_load_indexes(db->db);
				if (idx_ret)
					eprintf("Index rebuild also failed; "
						"hashfile left without "
						"digest indexes.\n");
			}
			goto out;
		}

		ret = scan_files(argc, argv, filelist_idx, db);

		if (use_hashfile == H_WRITE) {
			int idx_ret = dbfile_create_bulk_load_indexes(db->db);
			if (!ret)
				ret = idx_ret;
		}

		if (ret)
			goto out;

		qprintf("Hashfile \"%s\" written\n",
			options.hashfile);
	}

	if (use_hashfile == H_READ || use_hashfile == H_UPDATE)
		process_duplicates(db);

out:
	free_all_filerecs();

#ifdef DEBUG_BUILD
	print_mem_stats();
#else
	if (ret == ENOMEM || debug)
		print_mem_stats();
#endif

	return ret;
}
