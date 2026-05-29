/*
 * filerec.h
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
 */

#ifndef __FILEREC__
#define __FILEREC__

#include <stdint.h>
#include <time.h>
#include <glib.h>
#include <bsd/sys/queue.h>
#include "list.h"
#include "rbtree.h"
#include "results-tree.h"

SLIST_HEAD(filerec_list, filerec);
extern struct filerec_list filerec_head;

extern unsigned long long num_filerecs;
extern unsigned int dedupe_seq; /* This is incremented on every dedupe pass */

struct filerec {
	int		fd;			/* file descriptor */
	unsigned int	fd_refs;			/* fd refcount */

	char	*filename;		/* path to file */
	int64_t fileid;

	struct rb_node		fileid_node;

	uint64_t		size;
	struct rb_root		block_tree;	/* root for hash blocks tree */

	SLIST_ENTRY(filerec)	rec_list;	/* all filerecs */
};

void init_filerec(void);
void free_all_filerecs(void);

struct filerec *filerec_new(const char *filename, int64_t fileid,
			    uint64_t size);
void filerec_free(struct filerec *file);
struct filerec *filerec_find(int64_t fileid);

int filerec_open(struct filerec *file, bool quiet);
void filerec_close(struct filerec *file);

struct open_once {
	struct rb_root		root;
	/*
	 * Optional LRU FD eviction. When max_open > 0, filerec_open_once
	 * keeps no more than max_open files opened at a time, evicting
	 * the least-recently-used token when the cap would be exceeded.
	 * When max_open == 0 (the default), no eviction happens and the
	 * struct behaves exactly like the original "open everything,
	 * close at end" cache - which is what run_dedupe and friends
	 * want. The LRU list, open_count, and per-token t_lru node are
	 * unused in that mode.
	 *
	 * Use open_once_set_max(&o, N) after OPEN_ONCE_INIT to enable
	 * LRU eviction with cap N for a particular open_once instance.
	 */
	struct list_head	lru;
	unsigned int		open_count;
	unsigned int		max_open;
};
#define	OPEN_ONCE_INIT	(struct open_once) { RB_ROOT, { 0 }, 0, 0 }
#define OPEN_ONCE(name)	struct open_once name = OPEN_ONCE_INIT

int filerec_open_once(struct filerec *file,
		      struct open_once *open_files);
void filerec_close_open_list(struct open_once *open_files);
void open_once_set_max(struct open_once *open_files, unsigned int max);

/*
 * Track unique filerecs in a tree. Two places in the code use this:
 *	- filerec comparison tracking in filerec.c
 *	- conversion of large dupe lists in hash-tree.c
 * User has to define an rb_root, and a "free all" function.
 */
struct filerec_token {
	struct filerec	*t_file;
	struct rb_node	t_node;
	/*
	 * LRU list node, used only when this token is inserted into
	 * an open_once with max_open > 0. Initialized to a self-empty
	 * list_head by filerec_token_new so list_empty() can detect
	 * "not in any LRU list" cleanly.
	 */
	struct list_head t_lru;
};
struct filerec_token *find_filerec_token_rb(struct rb_root *root,
					    struct filerec *val);
void insert_filerec_token_rb(struct rb_root *root,
			     struct filerec_token *token);
void filerec_token_free(struct filerec_token *token);
struct filerec_token *filerec_token_new(struct filerec *file);

int fiemap_scan_extent(struct extent *extent);
#endif /* __FILEREC__ */
