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

	/*
	 * Cached BTRFS subvolume root id (treeid from BTRFS_IOC_INO_LOOKUP)
	 * and inode number (objectid = st_ino). Populated lazily by
	 * filerec_get_btrfs_ids() on first use. 0 means "not yet computed";
	 * any real subvolume's treeid is >= 5 (FS_TREE = 5,
	 * BTRFS_FIRST_FREE_OBJECTID = 256), so 0 is a safe sentinel.
	 * Persists across fd open/close cycles - the (treeid, ino) pair is a
	 * stable per-file identifier as long as the file isn't moved between
	 * subvolumes (which we don't expect during a duperemove run).
	 */
	uint64_t		btrfs_rootid;
	uint64_t		btrfs_objectid;

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

/*
 * Look up (BTRFS subvolume rootid, inode objectid) for a filerec.
 *
 * Requires file->fd to be valid (caller must filerec_open() first, or
 * be inside a path that holds the fd via open_once). Result is cached
 * in file->btrfs_rootid / file->btrfs_objectid for subsequent calls.
 *
 * On success, writes the IDs to *rootid and *objectid and returns 0.
 * On failure, returns an errno value (typically from BTRFS_IOC_INO_LOOKUP
 * or fstat) and leaves the cache unmodified.
 *
 * Intended for the SYNO_EXTENT_SAME ioctl path which needs (rootid,
 * objectid) rather than file descriptors.
 */
int filerec_get_btrfs_ids(struct filerec *file,
			  uint64_t *rootid, uint64_t *objectid);

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
