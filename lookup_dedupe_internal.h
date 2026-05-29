/*
 * lookup_dedupe_internal.h
 *
 * Definitions shared between lookup_dedupe.c and helper files like
 * srccount_seed.c that need direct access to struct lookup_state.
 * Not for use outside the lookup_dedupe family of files.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */

#ifndef __LOOKUP_DEDUPE_INTERNAL_H__
#define __LOOKUP_DEDUPE_INTERNAL_H__

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

#include <sqlite3.h>

#include "filerec.h"

struct dbhandle;

struct lookup_state {
	struct dbhandle	*db;
	sqlite3_stmt	*find_block_stmt;

	/*
	 * Phase 4 per-position bookkeeping. All five statements are
	 * keyed by (fileid, loff). They modify only the srccount and
	 * alias_root_* columns added by dbfile_migrate_blocks_columns;
	 * the bulk-data tables are never written through these.
	 */
	sqlite3_stmt	*select_alias_root_stmt;
	sqlite3_stmt	*select_srccount_stmt;
	sqlite3_stmt	*inc_srccount_stmt;
	sqlite3_stmt	*set_alias_root_stmt;
	/*
	 * Phase 5: set (not increment) srccount to a specific value.
	 * Used by the lazy LOGICAL_INO_V2 seed when transitioning a
	 * canonical from the "not seeded" sentinel (-1) to the
	 * kernel-truth reflink count.
	 */
	sqlite3_stmt	*set_srccount_stmt;

	struct open_once ref_opens;
	int64_t		next_synth_id;	/* counter for in-memory-only ids */
	uint64_t	n_lookup_files;
	uint64_t	n_ref_files;
	uint64_t	matches_deduped;
	uint64_t	bytes_deduped;
	uint64_t	seed_matches_found;
	uint64_t	dedupe_attempts;
	uint64_t	n_too_small_skipped;
	uint64_t	n_self_files;

	/* Phase 4/5 counters surfaced in the summary. */
	uint64_t	cap_skipped;
	uint64_t	alias_already_same;
	uint64_t	srccount_seeded;	/* canonicals where the
						 * lazy LOGICAL_INO_V2 seed
						 * fired and wrote a value
						 * back */

	struct timespec	start_time;
	struct timespec	last_progress_time;
	/*
	 * Periodic in-process WAL checkpoint. UPDATEs to blocks
	 * (srccount + alias_root_*) accumulate every-version page
	 * frames in the WAL until something forces a checkpoint;
	 * without this throttle the WAL grows ~170 MB per 1 GB
	 * scanned on heavy --lookup-self workloads. PRAGMA
	 * wal_checkpoint(PASSIVE) collapses each modified page to its
	 * final version in the main DB and frees the WAL frames.
	 */
	struct timespec	last_checkpoint_time;

	/*
	 * Cached global srccount generation. Loaded once at
	 * lookup_dedupe_main entry from the config table; immutable
	 * for the lifetime of the run. Phase 5 writes this value to
	 * the per-row srccount_gen column when it fires; the outer-
	 * skip rule uses this to detect stale srccount values.
	 */
	int64_t		current_srccount_gen;
	uint64_t	bytes_scanned_total;
	uint64_t	bytes_scanned_at_last;
	int		is_tty;
	bool		progress_active;
};

#endif	/* __LOOKUP_DEDUPE_INTERNAL_H__ */
