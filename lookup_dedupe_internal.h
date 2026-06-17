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

#include <glib.h>
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

	/*
	 * Tier 2 alias_root coverage fix.
	 *
	 * harvest_max_srccount_stmt: SELECT MAX(srccount) over a (fileid,
	 * loff) range, gated on srccount_gen = current_gen. Returns the
	 * largest CURRENT-GEN srccount among dst blocks in the deduped
	 * range. This recovers V1-truth that prior Phase 5 calls deposited
	 * on individual blocks when they were candidates of someone else's
	 * scan. Old-gen values are excluded to avoid stamping a stale
	 * number as fresh on the canonical.
	 *
	 * bulk_set_alias_root_stmt: UPDATE alias_root_fileid/loff for ALL
	 * blocks in the (fileid, loff) range. Without this, extend_match
	 * dedupes covering N blocks only marked the seed block as aliased,
	 * leaving the other N-1 blocks looking canonical-unseeded forever.
	 */
	sqlite3_stmt	*harvest_max_srccount_stmt;
	sqlite3_stmt	*bulk_set_alias_root_stmt;

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

	/*
	 * SYNO_EXTENT_SAME diagnostic counters. Only bumped when
	 * options.use_syno_dedupe != SYNO_OFF and the SYNO path was used.
	 *   ditto_partial: DITTO returns where the kernel processed some
	 *     prefix bytes before hitting a backref-saturated src extent.
	 *     The prefix is counted toward matches_deduped/bytes_deduped
	 *     normally; ditto_partial is a separate informational count.
	 *   ditto_skipped: DITTO returns where the cap was hit at the very
	 *     first byte, so no bytes were deduped in this call. The outer
	 *     loop re-evaluates (and userspace cap_skip should catch on
	 *     next iteration once Phase 5 reseeds srccount).
	 */
	uint64_t	ditto_partial;
	uint64_t	ditto_skipped;

	/*
	 * Per-run h16 saturation blacklist. An h16 enters the blacklist
	 * when its candidate list returns 100% cap_skip with zero
	 * successful dedupes in a seed - i.e., every position in the
	 * corpus matching that 16 KB content pattern resolves to a
	 * cap-saturated canonical. Future seeds whose h16 matches one
	 * already in the blacklist skip the SQL lookup + inner loop
	 * entirely.
	 *
	 * Keyed by 16-byte h16 bytes; values are unused (presence-only).
	 * The blacklist is in-memory only because cap state can change
	 * across runs (via --bump-srccount-gen, --reset-lookup-state,
	 * cap config changes) and we don't want stale entries.
	 *
	 * h16_blacklist_skipped counts whole-seed skips via this fast
	 * path - displayed on the progress line for visibility.
	 */
	GHashTable	*h16_blacklist;
	uint64_t	h16_blacklist_skipped;

	/*
	 * Bytes skipped by --skip-zeroes. Each time the leading
	 * blocksize of a seed window is all-zero and we skip the h16
	 * compute + SQL lookup, this counter advances by blocksize.
	 * Surfaced on the progress line so the user can quantify the
	 * data left on the table by the flag (zero blocks are
	 * trivially dedupable across the entire corpus to a single
	 * shared extent; --skip-zeroes trades that gain for csum +
	 * SQL overhead avoidance).
	 */
	uint64_t	zero_bytes_skipped;

	/*
	 * Walk position counter. Incremented once for every regular
	 * file walk_path passes to process_one_file, regardless of
	 * whether the file ends up processed, skipped as too-small,
	 * or skipped via --lookup-start-from. The progress line's
	 * "file N" display uses this rather than the (processed-only)
	 * sum of n_lookup_files + n_self_files, so the N reflects
	 * walk position and is stable across runs (assuming directory
	 * contents and readdir order are stable).
	 */
	uint64_t	files_visited;

	/*
	 * Corpus totals discovered by the pre-walk. lookup_dedupe_main
	 * walks each cmdline path once with no opens/no processing
	 * before starting real work, just lstat + readdir, to count
	 * regular files and sum sizes. Used by the progress line to
	 * show "% of total files" and "% of total bytes".
	 *
	 * Memory cost: just two scalars (no per-file storage).
	 * Time cost: one lstat per file, no file opens, no reads.
	 * For a 10M-file corpus on warm fs cache this is well under
	 * a minute; cold cache may be a few minutes.
	 *
	 * bytes_skipped_start_from accumulates the sizes of files
	 * skipped via --lookup-start-from. Added to bytes_scanned_total
	 * when computing the bytes-% display so resumed runs report
	 * total-corpus progress, not just bytes-since-resume progress.
	 * Kept separate from bytes_scanned_total so MB/s rate stays
	 * based on actual processing throughput, not a jump at the
	 * skip boundary.
	 */
	uint64_t	total_files_in_walk;
	uint64_t	total_bytes_to_scan;
	uint64_t	bytes_skipped_start_from;

	/*
	 * bytes_skipped_too_small accumulates the sizes of files that
	 * reached process_lookup_file / process_reference_self but were
	 * rejected by the size < min_dedupe_size guard. Files smaller
	 * than the minimum dedupe window can never contribute to a
	 * dedupe (the seed would never reach min_dedupe_size after
	 * extend_match). Without this counter, the bytes-% display
	 * would plateau short of 100% by the cumulative size of all
	 * too-small files; with it, the display converges to 100% as
	 * intended.
	 */
	uint64_t	bytes_skipped_too_small;

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
