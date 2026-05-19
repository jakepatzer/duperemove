/*
 * run_dedupe.c
 *
 * Implements dedupe of duplicate extents from our results tree
 *
 * Copyright (C) 2014 SUSE.  All rights reserved.
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

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>
#include <inttypes.h>

#include <glib.h>

#include "rbtree.h"
#include "list.h"
#include "csum.h"
#include "filerec.h"
#include "hash-tree.h"
#include "results-tree.h"
#include "dedupe.h"
#include "util.h"
#include "memstats.h"
#include "debug.h"
#include "dbfile.h"
#include "fiemap.h"

#include "run_dedupe.h"

static GMutex mutex;
static GMutex console_mutex;
static struct results_tree *results_tree;
static volatile unsigned long long total_dedupe_passes;
static volatile unsigned long long curr_dedupe_pass;
static unsigned int leading_spaces;
static bool whole_file_dedup;

/*
 * Contiguous range coalescing support (enabled via --coalesce).
 *
 * Safety note: duperemove submits ranges via FIDEDUPERANGE, which
 * performs its own byte-for-byte comparison inside the kernel before
 * sharing any data. The extension logic below is therefore only an
 * optimization hint: an over-long range can never corrupt data, it can
 * at worst be rejected or partially deduped by the kernel.
 */
#define COALESCE_ALIGN		4096ULL		/* btrfs min dedup unit */
#define COALESCE_CHUNK		(128ULL * 1024)	/* per-pread compare size */

/*
 * Per-thread extension buffers, allocated once on first use and reused
 * for the lifetime of the thread (bounded: io_threads * 2 * 128 KiB).
 */
static __thread char *coalesce_buf_a;
static __thread char *coalesce_buf_b;

struct coalesce_key {
	int64_t	src_id;
	int64_t	dst_id;
};

static GHashTable *coalesce_map;
static GMutex coalesce_map_mutex;

static guint coalesce_key_hash(gconstpointer p)
{
	const struct coalesce_key *k = p;

	return g_int64_hash(&k->src_id) ^ g_int64_hash(&k->dst_id);
}

static gboolean coalesce_key_equal(gconstpointer a, gconstpointer b)
{
	const struct coalesce_key *ka = a;
	const struct coalesce_key *kb = b;

	return ka->src_id == kb->src_id && ka->dst_id == kb->dst_id;
}

void coalesce_map_init(void)
{
	if (!options.coalesce)
		return;

	g_mutex_lock(&coalesce_map_mutex);
	coalesce_map = g_hash_table_new_full(coalesce_key_hash,
					     coalesce_key_equal,
					     g_free, g_free);
	g_mutex_unlock(&coalesce_map_mutex);
}

void coalesce_map_destroy(void)
{
	g_mutex_lock(&coalesce_map_mutex);
	if (coalesce_map) {
		g_hash_table_destroy(coalesce_map);
		coalesce_map = NULL;
	}
	g_mutex_unlock(&coalesce_map_mutex);
}

/*
 * True if the destination region at dst_off for the (src_id, dst_id)
 * file pair was already covered by a previously submitted coalesced
 * range and should be skipped. Keyed by both file ids so that the same
 * destination file matched against different source files at different
 * offsets is tracked independently.
 */
bool coalesce_covered(int64_t src_id, int64_t dst_id,
		      uint64_t dst_off)
{
	struct coalesce_key key = { .src_id = src_id, .dst_id = dst_id };
	uint64_t *hw;
	bool covered = false;

	if (!coalesce_map)
		return false;

	g_mutex_lock(&coalesce_map_mutex);
	hw = g_hash_table_lookup(coalesce_map, &key);
	if (hw && dst_off < *hw)
		covered = true;
	g_mutex_unlock(&coalesce_map_mutex);

	return covered;
}

/*
 * Record that the (src_id, dst_id) pair has been deduped in the
 * destination up to dst_end. The maximum is kept so that out-of-order
 * entries cannot lower the high-water mark.
 */
void coalesce_record(int64_t src_id, int64_t dst_id,
		     uint64_t dst_end)
{
	struct coalesce_key lookup = { .src_id = src_id, .dst_id = dst_id };
	struct coalesce_key *key;
	uint64_t *hw;

	if (!coalesce_map)
		return;

	g_mutex_lock(&coalesce_map_mutex);
	hw = g_hash_table_lookup(coalesce_map, &lookup);
	if (hw) {
		if (dst_end > *hw)
			*hw = dst_end;
	} else {
		key = g_malloc(sizeof(*key));
		*key = lookup;
		hw = g_malloc(sizeof(*hw));
		*hw = dst_end;
		g_hash_table_insert(coalesce_map, key, hw);
	}
	g_mutex_unlock(&coalesce_map_mutex);
}

static int coalesce_get_bufs(char **a, char **b)
{
	if (coalesce_buf_a == NULL) {
		coalesce_buf_a = malloc(COALESCE_CHUNK);
		coalesce_buf_b = malloc(COALESCE_CHUNK);
		if (coalesce_buf_a == NULL || coalesce_buf_b == NULL) {
			free(coalesce_buf_a);
			free(coalesce_buf_b);
			coalesce_buf_a = coalesce_buf_b = NULL;
			return -ENOMEM;
		}
	}
	*a = coalesce_buf_a;
	*b = coalesce_buf_b;
	return 0;
}

/*
 * Starting from a seed match of seed_len bytes at (tgt, tgt_off) and
 * (dst, dst_off), extend the match forward by direct byte comparison.
 * Returns the total contiguous match length, aligned down to the btrfs
 * dedup unit and never below seed_len. Reads only; never aborts. On any
 * pread error or short read the extension stops at the confirmed point
 * and whatever has been verified so far is returned.
 */
uint64_t extend_match(struct filerec *tgt, uint64_t tgt_off,
		      struct filerec *dst, uint64_t dst_off,
		      uint64_t seed_len)
{
	char *buf_a, *buf_b;
	uint64_t len = seed_len;
	uint64_t tgt_max, dst_max, ceiling;

	if (coalesce_get_bufs(&buf_a, &buf_b)) {
		eprintf("Warning: out of memory for coalesce buffers; "
			"submitting seed length only.\n");
		return seed_len;
	}

	/*
	 * Never read or submit past either file's scanned size. The
	 * kernel re-verifies regardless, but staying in bounds avoids
	 * pointless short reads and EINVAL.
	 */
	if (tgt_off >= tgt->size || dst_off >= dst->size)
		return seed_len;

	tgt_max = tgt->size - tgt_off;
	dst_max = dst->size - dst_off;
	ceiling = tgt_max < dst_max ? tgt_max : dst_max;

	while (len < ceiling) {
		uint64_t want = ceiling - len;
		ssize_t ra, rb, cmp;

		if (want > COALESCE_CHUNK)
			want = COALESCE_CHUNK;

		ra = pread(tgt->fd, buf_a, want, tgt_off + len);
		if (ra <= 0) {
			if (ra < 0)
				vprintf("coalesce: pread(\"%s\", %"PRIu64
					") failed: %s; stopping extension\n",
					tgt->filename, tgt_off + len,
					strerror(errno));
			break;
		}

		rb = pread(dst->fd, buf_b, ra, dst_off + len);
		if (rb <= 0) {
			if (rb < 0)
				vprintf("coalesce: pread(\"%s\", %"PRIu64
					") failed: %s; stopping extension\n",
					dst->filename, dst_off + len,
					strerror(errno));
			break;
		}

		cmp = ra < rb ? ra : rb;
		if (memcmp(buf_a, buf_b, cmp) != 0) {
			ssize_t i = 0;

			while (i < cmp && buf_a[i] == buf_b[i])
				i++;
			len += i;
			break;
		}

		len += cmp;
		if (cmp < (ssize_t)want)
			break;	/* short read - treat as EOF */
	}

	/* Align down to the btrfs dedup unit; never below seed_len. */
	len &= ~(COALESCE_ALIGN - 1);
	if (len < seed_len)
		len = seed_len;
	return len;
}

/*
 * If --dedupe-target-priority is set, return the first extent in the
 * group whose file path begins with the configured prefix, else NULL.
 */
static struct extent *find_priority_target(struct dupe_extents *dext)
{
	struct extent *extent;
	size_t plen;

	if (options.dedupe_target_priority == NULL)
		return NULL;

	plen = strlen(options.dedupe_target_priority);
	list_for_each_entry(extent, &dext->de_extents, e_list) {
		if (strncmp(extent->e_file->filename,
			    options.dedupe_target_priority, plen) == 0)
			return extent;
	}
	return NULL;
}

void print_dupes_table(struct results_tree *res, bool whole_file)
{
	struct rb_root *root = &res->root;
	struct rb_node *node = rb_first(root);
	struct dupe_extents *dext;
	struct extent *extent;
	char *kind;

	if (whole_file)
		kind = "files";
	else
		kind = "extents";

	printf("Simple read and compare of file data found %u instances of "
	       "%s that might benefit from deduplication.\n",
	       res->num_dupes, kind);

	if (quiet || res->num_dupes == 0)
		return;

	while (1) {
		if (node == NULL)
			break;

		dext = rb_entry(node, struct dupe_extents, de_node);

		printf("Showing %u identical %s of length %s with id ",
		       dext->de_num_dupes, kind, pretty_size(dext->de_len));
		debug_print_digest_short(stdout, dext->de_hash);
		printf("\n");
		printf("Start\t\tFilename\n");
		list_for_each_entry(extent, &dext->de_extents, e_list) {
			printf("%s\t\"%s\"\n",
			       pretty_size(extent->e_loff),
			       extent->e_file->filename);
		}

		node = rb_next(node);
	}
}

void process_dedupe_results(struct dedupe_ctxt *ctxt,
			    uint64_t *kern_bytes)
{
	int done = 0;
	int target_status;
	uint64_t target_loff, target_bytes;
	struct filerec *f;
	const char *status_str = "[unknown status]";

	while (!done) {
		done = pop_one_dedupe_result(ctxt, &target_status, &target_loff,
					     &target_bytes, &f);
		if (kern_bytes)
			*kern_bytes += target_bytes;

		/*
		 * Coalesce: on a successful dedupe of a destination,
		 * advance the (source, destination) high-water mark so
		 * subsequent per-block entries that fall inside this
		 * range are skipped instead of generating redundant
		 * ioctls. Only real, kernel-confirmed bytes are
		 * recorded, so a SAME_DATA_DIFFERS / partial result
		 * does not over-claim coverage.
		 */
		if (options.coalesce && target_status == 0 &&
		    f != ctxt->ioctl_file && target_bytes)
			coalesce_record(ctxt->ioctl_file->fileid,
					f->fileid,
					target_loff + target_bytes);

		/*
		 * Only print in case of error.
		 *
		 * Kernels older than 4.2 can't handle the target and
		 * dedupe files being the same and -EINVAL in that
		 * case. Don't bubble it up so as to avoid user
		 * confusion.
		 */
		if (target_status == 0 ||
		    (target_status == -EINVAL && f == ctxt->ioctl_file))
			continue;

		if (target_status == FILE_DEDUPE_RANGE_DIFFERS)
			status_str = "data changed";
		else if (target_status < 0)
			status_str = strerror(-target_status);
		printf("[%p] Dedupe for file \"%s\" had status (%d) "
		       "\"%s\" at offset %s, %s bytes.\n",
		       g_thread_self(), f->filename, target_status,
		       status_str, pretty_size(target_loff),
		       pretty_size(target_bytes));
	}
}

static void add_shared_extents(struct dupe_extents *dext, uint64_t *shared)
{
	struct extent *extent;

	list_for_each_entry(extent, &dext->de_extents, e_list)
		*shared += extent_shared_bytes(extent);
}

/*
 * Fiemap the file and get our post-dedupe extent state.
 *
 * XXX: We're running fiemap too much for this. At the least we should
 * shoot for one fiemap call(s) per filerec.
 */
static void add_shared_extents_post(struct dupe_extents *dext, uint64_t *shared)
{
	int ret;
	uint64_t bytes = 0;
	struct extent *extent;
	struct filerec *file;

	list_for_each_entry(extent, &dext->de_extents, e_list) {
		file = extent->e_file;
		ret = filerec_open(file, true);
		if (ret)
			return;

		ret = fiemap_count_shared(file->fd, extent->e_loff, extent->e_loff + dext->de_len,
					   &bytes);

		*shared += bytes;
		filerec_close(file);

		if (ret)
			return;
	}
}

static int disk_extent_grew(struct dupe_extents *dext, struct extent *extent)
{
	/*
	 * Check length of the virtual extent versus that of the 1st
	 * physical extent in our range.
	 *
	 * If the physical extent is smaller than our virtual
	 * (duplicate) extent, we want to go ahead and dedupe in order
	 * to catch two cases:
	 *
	 * - The files were appended to (separately) with duplicate
	 *   data - this will result in a pair of new extents on each
	 *   file that can be deduped.
	 *
	 * - Kernels before 4.2 rejected unaligned lengths, so we can
	 *   have a residual tail extent to dedupe.
	 */
	if (extent_plen(extent) < dext->de_len)
		return 1;
	return 0;
}

/*
 * Removes extents which it believes have already been deduped. We err
 * on the side of more deduping here.
 */
static void clean_deduped(struct dupe_extents **ret_dext)
{
	int left;
	int extents_kept = 0;
	int first = 1;
	struct dupe_extents *dext = *ret_dext;
	struct rb_node *inner, *outer;
	struct extent *inner_extent, *outer_extent;

	if (!dext || dext->de_num_dupes == 0)
		return;

	outer = rb_first(&dext->de_extents_root);
	while (outer) {
		outer_extent = rb_entry(outer, struct extent, e_node);

		/*
		 * First extent will not be considered for removal
		 * below, which is fine as remove_extent() handles the
		 * case of only 1 extent left on the dext for us.
		 *
		 * Replicate the checks though and count it as kept if
		 * we don't want it deleted. That will trigger the
		 * logic below to save the dext if we should wind up
		 * throwing everything else out.
		 */
		if (first &&
		    (extent_poff(outer_extent) == 0 ||
		     disk_extent_grew(dext, outer_extent)))
			extents_kept++;
		first = 0;

		inner = rb_next(outer);
		while (inner) {
			inner_extent = rb_entry(inner, struct extent, e_node);
			inner = rb_next(inner);

			/*
			 * Track if any extents have survived the
			 * culling. If we're down to the last two and
			 * at least one of them was deemed worthy,
			 * exit here so that he may be deduped.
			 */
			if (dext->de_num_dupes == 2 && extents_kept)
				return;

			/*
			 * e_poff could be zero if fiemap from
			 * add_shared_extents fails. In that case,
			 * skip the extent (it might want to be
			 * deduped).
			 */
			if (extent_poff(inner_extent)
			    && extent_poff(outer_extent) == extent_poff(inner_extent)
			    && !disk_extent_grew(dext, inner_extent)) {
				dprintf("Remove extent "
					"(\"%s\", %"PRIu64", %"PRIu64")\n",
					inner_extent->e_file->filename,
					extent_poff(inner_extent),
					extent_plen(inner_extent));

				g_mutex_lock(&mutex);
				left = remove_extent(results_tree,
						     inner_extent);
				g_mutex_unlock(&mutex);
				if (left == 0) {
					*ret_dext = dext = NULL;
					return;
				}
			} else
				extents_kept++;
		}
		outer = rb_next(outer);
	}
}

#define	DEDUPE_EXTENTS_CLEANED	(-1)
static int dedupe_extent_list(struct dupe_extents *dext, uint64_t *fiemap_bytes,
			      uint64_t *kern_bytes, unsigned long long passno)
{
	int ret = 0;
	int last = 0;
	int rc;
	uint64_t shared_prev, shared_post;
	struct extent *extent;
	struct dedupe_ctxt *ctxt = NULL;
	uint64_t len = dext->de_len;
	OPEN_ONCE(open_files);
	struct extent *tgt_extent = NULL;

	abort_on(dext->de_num_dupes < 2);

	/* Dedupe extents with id %s*/
	if (!quiet) {
		g_mutex_lock(&console_mutex);
		printf("[%p] (%0*llu/%llu) Try to dedupe extents with id ",
		       g_thread_self(), leading_spaces, passno,
		       total_dedupe_passes);
		debug_print_digest_short(stdout, dext->de_hash);
		printf("\n");
		g_mutex_unlock(&console_mutex);
	}

	shared_prev = shared_post = 0ULL;
	/*
	 * Remove any extents which have already been deduped. This
	 * will free dext for us if the number of available extents
	 * goes below 2. If that happens, we return a special value so
	 * the caller knows not to reference dext any more.
	 */
	clean_deduped(&dext);
	if (!dext) {
		qprintf("[%p] Skipping - extents are already deduped.\n",
		       g_thread_self());
		return DEDUPE_EXTENTS_CLEANED;
	}

	/*
	 * Do this after clean_deduped as we may have removed some
	 * extents.
	 */
	add_shared_extents(dext, &shared_prev);

	/*
	 * --dedupe-target-priority: if any extent's file path matches
	 * the configured prefix, move it to the head of the list so
	 * the loop below selects it as the dedupe target (the extent
	 * whose physical data survives). This is a stronger guarantee
	 * than relying on scan/argument order. When the option is
	 * unset this is a no-op and ordering is unchanged.
	 */
	if (options.dedupe_target_priority) {
		struct extent *prio = find_priority_target(dext);

		if (prio && dext->de_extents.next != &prio->e_list) {
			list_move(&prio->e_list, &dext->de_extents);
			vprintf("[%p] coalesce: target priority selected "
				"\"%s\"\n", g_thread_self(),
				prio->e_file->filename);
		}
	}

	/*
	 * Contiguous range coalescing pre-pass. This is side-effect
	 * free: it only opens files read-only and compares bytes. It
	 * computes the largest contiguous match length that is common
	 * to *every* (target, destination) pair in the group (the
	 * minimum across pairs), so a single FIDEDUPERANGE of that
	 * length replaces many per-block ioctls. Destinations already
	 * covered by an earlier coalesced range are excluded here (and
	 * skipped again in the loop below).
	 */
	if (options.coalesce && !list_empty(&dext->de_extents)) {
		OPEN_ONCE(cfiles);
		struct extent *tgt = list_first_entry(&dext->de_extents,
						      struct extent, e_list);
		uint64_t min_len = 0;
		unsigned int n_dests = 0, n_covered = 0, n_measured = 0;

		if (filerec_open_once(tgt->e_file, &cfiles) == 0) {
			list_for_each_entry(extent, &dext->de_extents,
					    e_list) {
				uint64_t l;

				if (extent == tgt)
					continue;
				n_dests++;
				if (coalesce_covered(tgt->e_file->fileid,
						extent->e_file->fileid,
						extent->e_loff)) {
					n_covered++;
					continue;
				}
				if (filerec_open_once(extent->e_file,
						      &cfiles))
					continue;
				l = extend_match(tgt->e_file, tgt->e_loff,
						 extent->e_file,
						 extent->e_loff,
						 dext->de_len);
				if (n_measured == 0 || l < min_len)
					min_len = l;
				n_measured++;
			}
		}
		filerec_close_open_list(&cfiles);

		/*
		 * Only skip the group when it is genuinely redundant
		 * (every destination already covered by an earlier
		 * coalesced range) or when the common range is below
		 * --min-dedupe-size. In every other situation (target
		 * or a destination not openable in this read-only
		 * pre-pass, nothing measured) fall back to the
		 * unmodified per-group length so that enabling
		 * --coalesce is never worse than not enabling it.
		 */
		if (n_dests > 0 && n_covered == n_dests) {
			qprintf("[%p] coalesce: extents already covered, "
				"skipping.\n", g_thread_self());
			return DEDUPE_EXTENTS_CLEANED;
		}
		if (n_measured > 0) {
			if (options.min_dedupe_size &&
			    min_len < options.min_dedupe_size) {
				vprintf("[%p] coalesce: range %"PRIu64" < "
					"min-dedupe-size %"PRIu64
					", skipping.\n", g_thread_self(),
					min_len, options.min_dedupe_size);
				return DEDUPE_EXTENTS_CLEANED;
			}
			len = min_len;
		}
	}

	list_for_each_entry(extent, &dext->de_extents, e_list) {
		if (list_is_last(&extent->e_list, &dext->de_extents))
			last = 1;

		ret = filerec_open_once(extent->e_file, &open_files);
		if (ret) {
			if (ret == ENOENT) {
				/*
				 * File were deleted. Maybe it was scanned
				 * a long time ago. Let's clean the db.
				 */
				dbfile_lock();
				dbfile_remove_file(dbfile_get_handle(), extent->e_file->filename);
				dbfile_unlock();
			}
			eprintf("%s: Skipping dedupe.\n",
				extent->e_file->filename);
			/*
			 * If this was our last duplicate extent in
			 * the list, and we added dupes from a
			 * previous iteration of the loop we need to
			 * run dedupe before exiting.
			 */
			if (ctxt && last)
				goto run_dedupe;
			continue;
		}

		vprintf("[%p] Add extent for file \"%s\" at offset %s (%d)\n",
			g_thread_self(), extent->e_file->filename,
			pretty_size(extent->e_loff), extent->e_file->fd);

		/*
		 * Coalesce: skip destinations already covered by a
		 * previously submitted larger range for this exact
		 * (target, destination) file pair. The target extent
		 * itself is never a destination, so it is exempt.
		 */
		if (options.coalesce && tgt_extent &&
		    extent != tgt_extent &&
		    coalesce_covered(tgt_extent->e_file->fileid,
				     extent->e_file->fileid,
				     extent->e_loff)) {
			vprintf("[%p] coalesce: skipping covered extent "
				"\"%s\" @ %s\n", g_thread_self(),
				extent->e_file->filename,
				pretty_size(extent->e_loff));
			if (ctxt && last)
				goto run_dedupe;
			continue;
		}

		if (ctxt == NULL) {
			if (tgt_extent == NULL) {
				/*
				 * We had some errors adding files
				 * previously and are down to the last
				 * dedupe candidate. Proceed only if
				 * we can guarantee two extents for
				 * dedupe (target, and this file).
				 */
				if (last)
					goto close_files;

				tgt_extent = extent;
			}
			ctxt = new_dedupe_ctxt(dext->de_num_dupes,
					       tgt_extent->e_loff, len,
					       tgt_extent->e_file);
			if (ctxt == NULL) {
				eprintf("Out of memory while "
					"allocating dedupe context.\n");
				ret = ENOMEM;
				goto out;
			}

			/*
			 * If we just picked the target, it got added
			 * with the new context. Otherwise fall
			 * through to let other extents onto the
			 * dedupe ctxt.
			 */
			if (tgt_extent == extent)
				continue;
		}

		rc = add_extent_to_dedupe(ctxt, extent->e_loff, extent->e_file);
		if (rc) {
			if (rc < 0) {
				/* This can only be ENOMEM. */
				eprintf("%s: Request not queued.\n",
					extent->e_file->filename);
				ret = ENOMEM;
				goto out;
			}

			if (!last)
				continue;
		}

run_dedupe:
		/*
		 * We can get here with only the target extent (0
		 * queued) for many reasons. Skip the dedupe in that
		 * case but always do cleanup.
		 */
		if (ctxt->num_queued) {
			if (!quiet) {
				g_mutex_lock(&console_mutex);
				printf("[%p] Dedupe %u extents (id: ",
				       g_thread_self(), ctxt->num_queued);
				debug_print_digest_short(stdout, dext->de_hash);
				printf(") with target: (%s, %s), "
				       "\"%s\"\n",
				       pretty_size(ctxt->orig_file_off),
				       pretty_size(ctxt->orig_len),
				       ctxt->ioctl_file->filename);
				g_mutex_unlock(&console_mutex);
			}

			ret = dedupe_extents(ctxt);
			if (ret == 0) {
				process_dedupe_results(ctxt, kern_bytes);
			} else {
				ret = errno;
				eprintf("FAILURE: Dedupe ioctl returns %d: %s\n",
					ret, strerror(ret));
			}
		}
close_files:
		filerec_close_open_list(&open_files);
		free_dedupe_ctxt(ctxt);
		ctxt = NULL;

		if (!last) {
			/* reopen target file as it got closed above */
			ret = filerec_open_once(tgt_extent->e_file,
						&open_files);
			if (ret) {
				eprintf("%s: Could not re-open as target.\n",
					extent->e_file->filename);
				break;
			}
		}
	}

	abort_on(ctxt != NULL);
	abort_on(!RB_EMPTY_ROOT(&open_files.root));

	add_shared_extents_post(dext, &shared_post);

	/*
	 * It's entirely possible that some other process is
	 * manipulating files underneath us. Take care not to
	 * report some randomly enormous 64 bit value.
	 */
	if (shared_prev  < shared_post)
		*fiemap_bytes += shared_post - shared_prev;

	/* The only error we want to bubble up is ENOMEM */
	ret = 0;
out:
	/*
	 * ENOMEM error during context allocation may have caused open
	 * files to stay in our list.
	 */
	filerec_close_open_list(&open_files);
	/*
	 * We might have allocated a context above but not
	 * filled it with any extents, make sure to free it
	 * here.
	 */
	free_dedupe_ctxt(ctxt);

	abort_on(!RB_EMPTY_ROOT(&open_files.root));

	return ret;
}

static GMutex dedupe_counts_mutex;
struct dedupe_counts {
	uint64_t	kern_bytes;
	uint64_t	fiemap_bytes;
};

static int extent_dedupe_worker(struct dupe_extents *dext,
				uint64_t *fiemap_bytes, uint64_t *kern_bytes)
{
	int ret;
	unsigned long long passno = __atomic_add_fetch(&curr_dedupe_pass, 1, __ATOMIC_SEQ_CST);

	struct extent *extent;
	struct dbhandle *db = dbfile_get_handle();

	ret = dedupe_extent_list(dext, fiemap_bytes, kern_bytes, passno);
	if (ret) {
		if (ret == DEDUPE_EXTENTS_CLEANED)
			return 0;
		/* dedupe_extent_list already printed to stderr for us */
		return ret;
	}

	dbfile_lock();
	list_for_each_entry(extent, &dext->de_extents, e_list) {
		if (whole_file_dedup) {
			/* If we are deduping a whole file, then the extents may be remapped
			 * by Linux. Let's drop them from the hashfile: even if some other file
			 * share one on those extents, keeping the whole file deduplicated is
			 * a better move.
			 * TODO: do not delete the extents but rescan every files to fetch
			 * the new extents mapping as well as their new hashes
			 * This may cause dbfile_load_one_file_extent() to raise an error.
			 */
			dbfile_remove_extent_hashes(db, extent->e_file->fileid);
		} else {
			/* Rescan physical offset and update the hashfile accordingly */
			ret = fiemap_scan_extent(extent);
			if (!ret)
				dbfile_update_extent_poff(db, extent->e_file->fileid, extent->e_loff, extent->e_poff);
		}
	}
	dbfile_unlock();

	if (!list_empty(&dext->de_extents)) {
		g_mutex_lock(&mutex);
		dupe_extents_free(dext, results_tree);
		g_mutex_unlock(&mutex);
	}

	return 0;
}

static void dedupe_worker(void *priv, struct dedupe_counts *counts)
{
	uint64_t fiemap_bytes = 0ULL;
	uint64_t kern_bytes = 0ULL;

	extent_dedupe_worker(priv, &fiemap_bytes, &kern_bytes);

	g_mutex_lock(&dedupe_counts_mutex);
	counts->fiemap_bytes += fiemap_bytes;
	counts->kern_bytes += kern_bytes;
	g_mutex_unlock(&dedupe_counts_mutex);
}

static GThreadPool *dedupe_pool = NULL;

/* Errors from this function are fatal. */
static int push_extents(struct results_tree *res)
{
	struct rb_root *root = &res->root;
	struct rb_node *node = rb_first(root);
	struct dupe_extents *dext;
	GError *err = NULL;

	while (node) {
		dext = rb_entry(node, struct dupe_extents, de_node);

		/*
		 * dext may be free'd by the dedupe threads, so get
		 * the next node now. In addition we want to lock
		 * around the rbtree code here so rb_erase doesn't
		 * change the tree underneath us.
		 */
		g_mutex_lock(&mutex);
		node = rb_next(node);
		g_mutex_unlock(&mutex);

		if (dext->de_num_dupes < 2) {
			qprintf("Skipping extent - insufficient duplicates (%u)\n",
				   dext->de_num_dupes);
			continue;
		}

		g_thread_pool_push(dedupe_pool, dext, &err);
		if (err) {
			eprintf("Fatal error while deduping: %s\n",
				err->message);
			g_error_free(err);
			return 1;
		}
	}
	return 0;
}

void dedupe_results(struct results_tree *res, bool whole_file)
{
	int ret;
	struct dedupe_counts counts = { 0ULL, };
	GError *err = NULL;

	/*
	 * dedupe_results() could be called multiple times, so we reset that bit
	 * to its initial value every time
	 */
	curr_dedupe_pass = 0;
	results_tree = res;

	whole_file_dedup = whole_file;

	print_dupes_table(res, whole_file);

	if (RB_EMPTY_ROOT(&res->root)) {
		printf("Nothing to dedupe.\n");
		return;
	}

	qprintf("Using %u threads for dedupe phase\n", options.io_threads);

	dedupe_pool = g_thread_pool_new((GFunc) dedupe_worker, &counts,
					options.io_threads, TRUE, &err);
	if (err) {
		eprintf("Unable to create dedupe thread pool: %s\n",
			err->message);
		g_error_free(err);
		return;
	}

	total_dedupe_passes = res->num_dupes;
	leading_spaces = num_digits(total_dedupe_passes);

	coalesce_map_init();

	ret = push_extents(res);
	if (ret) {
		eprintf("Fatal error while deduping: %s\n", err->message);
		g_error_free(err);
	}

	g_thread_pool_free(dedupe_pool, FALSE, TRUE);

	coalesce_map_destroy();

	if (ret == 0) {
		vprintf("Kernel processed data (excludes target files): "
			"%s\n", pretty_size(counts.kern_bytes));
		printf("Comparison of extent info shows a net "
		       "change in shared extents of: %s\n",
		       pretty_size(counts.fiemap_bytes));
	}
}

int fdupes_dedupe(void)
{
	int ret;
	struct filerec *file;
	struct dedupe_ctxt *ctxt = NULL;
	uint64_t bytes = 0;
	OPEN_ONCE(open_files);

	SLIST_FOREACH(file, &filerec_head, rec_list) {
		ret = filerec_open_once(file, &open_files);
		if (ret) {
			eprintf("%s: Skipping dedupe.\n", file->filename);
			continue;
		}

		qprintf("Queue entire file for dedupe: %s\n", file->filename);

		if (ctxt == NULL) {
			ctxt = new_dedupe_ctxt(MAX_DEDUPES_PER_IOCTL,
					       0, file->size, file);
			if (ctxt == NULL) {
				eprintf("Out of memory while "
					"allocating dedupe context.\n");
				ret = ENOMEM;
				goto out;
			}
			continue;
		}

		ret = add_extent_to_dedupe(ctxt, 0, file);
		if (ret < 0) {
			eprintf("%s: Request not queued.\n", file->filename);
			ret = ENOMEM;
			goto out;
		} else if (ret == 0 || SLIST_NEXT(file, rec_list) == 0) {
			ret = dedupe_extents(ctxt);
			if (ret) {
				ret = errno;
				eprintf("FAILURE: Dedupe ioctl returns %d: %s\n",
					ret, strerror(ret));
				goto out;
			}
			filerec_close_open_list(&open_files);
			process_dedupe_results(ctxt, &bytes);
			free_dedupe_ctxt(ctxt);
			ctxt = NULL;

			printf("Dedupe pass on %llu files completed\n",
			       num_filerecs);
		}
	}

	ret = 0;
out:
	filerec_close_open_list(&open_files);
	free_dedupe_ctxt(ctxt);
	free_all_filerecs();
	return ret;
}
