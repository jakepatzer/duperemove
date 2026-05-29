/*
 * lookup_dedupe.h
 *
 * Streaming --lookup-only dedupe path. Files passed on the command
 * line that are already present in a (read-only) hashfile are skipped
 * as reference files; files not in the hashfile are stream-hashed
 * block by block and each digest is looked up against the hashfile in
 * memory, with seed matches fed directly into the coalesce extension
 * and dedupe submission used by the normal pipeline.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */

#ifndef __LOOKUP_DEDUPE_H__
#define __LOOKUP_DEDUPE_H__

struct dbhandle;

/*
 * Entry point for --lookup-only mode. The database must have been
 * opened with dbfile_open_handle_readonly() and dbfile_set_gdb()
 * already called. The global blocksize must match the hashfile's
 * blocksize (the caller checks).
 *
 * argv/argc/filelist_idx describe the cmdline file/directory list to
 * walk. Reference files (those present in the hashfile by ino,subvol)
 * are opened and pinned; lookup files (not in the hashfile) are
 * stream-processed.
 *
 * Returns 0 on success or an errno-style code on fatal error.
 */
int lookup_dedupe_main(struct dbhandle *db, int argc, char **argv,
		       int filelist_idx);

/*
 * Reset Phase 4/5 per-position state on the blocks table:
 * srccount -> -1 (the "not seeded" sentinel) and alias_root_fileid /
 * alias_root_loff -> NULL. Used as a one-shot before re-running
 * --lookup-only when prior runs left stale cap accounting or alias
 * pointers (e.g. after fixing the V1-fallback bug in the LOGICAL_INO
 * seed and wanting fresh kernel-truth counts on next encounter).
 *
 * Returns 0 on success or an errno-style code on failure. The DB
 * must be opened read-write.
 */
int lookup_reset_state(struct dbhandle *db);

#endif	/* __LOOKUP_DEDUPE_H__ */
