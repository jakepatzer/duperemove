/*
 * zero_dedupe.c
 *
 * --zero-only-dedupe mode. See zero_dedupe.h for the design summary.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <linux/btrfs.h>
#include <linux/fiemap.h>
#include <linux/fs.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "debug.h"
#include "dedupe.h"		/* MAX_DEDUPES_PER_IOCTL, syno_dedupe_ids */
#include "btrfs-syno.h"
#include "btrfs-util.h"
#include "opt.h"
#include "util.h"
#include "zero_dedupe.h"

/*
 * Synology DSM 7 ships the legacy BTRFS_IOC_LOGICAL_INO uapi but not
 * the V2 macro added in upstream 4.15. Define both ourselves if
 * missing - matches the same workaround in srccount_seed.c.
 */
#ifndef BTRFS_IOC_LOGICAL_INO
#define BTRFS_IOC_LOGICAL_INO	_IOWR(BTRFS_IOCTL_MAGIC, 36, \
				      struct btrfs_ioctl_logical_ino_args)
#endif
#ifndef BTRFS_IOC_LOGICAL_INO_V2
#define BTRFS_IOC_LOGICAL_INO_V2 _IOWR(BTRFS_IOCTL_MAGIC, 59, \
				       struct btrfs_ioctl_logical_ino_args)
#endif

/*
 * Tuning constants. Detection and dedupe both operate at 4 KiB
 * granularity - every 4 KiB zero block becomes its own dedupe unit.
 * Contiguous zero blocks are detected by extending the run and
 * submitted as a single batched FIDEDUPERANGE.
 *
 * SCAN_CHUNK is the pread buffer size; large enough to amortize
 * syscall overhead but bounded for memory safety.
 */
#define BLOCKSIZE_BYTES		(4UL * 1024UL)		/* 4 KiB */
#define SCAN_CHUNK		(4UL * 1024UL * 1024UL)	/* 4 MiB */

/*
 * Canonical state. Persists across files. Exactly one canonical
 * extent is "current" at any time; when its observed reference count
 * reaches cap, we rotate to a fresh one. count is the most recently
 * known ref count (kernel truth at adoption, plus locally tracked
 * increments since). phys is the physical disk address used to
 * detect "next zero block is already pointing at me" cases during
 * rotation - we skip such candidates so the new canonical is a
 * genuinely-separate extent with headroom.
 */
struct zero_canon {
	int		fd;	/* dup()'d fd, owned by canonical lifetime */
	uint64_t	off;	/* offset within the canonical's file */
	uint64_t	phys;	/* physical address from FIEMAP */
	uint64_t	count;	/* current ref count, including this canon's own ref */
	uint64_t	rootid;	  /* btrfs subvolume id (for SYNO ioctl) */
	uint64_t	objectid; /* btrfs inode number (for SYNO ioctl) */
};

struct zero_state {
	struct zero_canon	canon;
	bool			canon_initialized;
	uint64_t		cap;

	/* Counters / progress */
	struct timespec		start_time;
	struct timespec		last_progress_time;
	uint64_t		files_visited;
	uint64_t		total_files_in_walk;
	uint64_t		total_bytes_to_scan;
	uint64_t		bytes_scanned_total;
	uint64_t		bytes_skipped_start_from;
	uint64_t		bytes_skipped_too_small;
	uint64_t		bytes_scanned_at_last;

	uint64_t		zero_runs_found;
	uint64_t		zero_bytes_found;
	uint64_t		bytes_deduped;
	uint64_t		dedupe_calls;
	uint64_t		canon_rotations;
	uint64_t		zero_runs_skipped_no_headroom;

	int			is_tty;
	bool			progress_active;
};

/*
 * Same human-readable size formatter used by lookup_dedupe. Duplicated
 * rather than shared because it's small and the alternative would be
 * either making lookup_dedupe's static non-static (and exposing in a
 * header) or moving it to util.c (a slightly larger refactor than
 * justified for one small inline function).
 */
static void fmt_size_h(uint64_t size, char *str, size_t str_bytes)
{
	static const char *units[] = { "B", "K", "M", "G", "T", "P", "E" };
	unsigned int u = 0;
	double v = (double)size;

	while (v >= 1024.0 && u + 1 < sizeof(units) / sizeof(units[0])) {
		v /= 1024.0;
		u++;
	}
	snprintf(str, str_bytes, "%.1f%s", v, units[u]);
}

/*
 * Fast all-zero check. Reads in 64-bit chunks for speed; falls
 * through to a byte-wise tail for the (usually empty) trailing
 * fraction. Returns true if every byte in [buf, buf+len) is zero.
 *
 * Trusts the buffer to be at least 8-byte aligned where possible -
 * pread/malloc results from glibc are typically 16-byte aligned. The
 * code is defensive: a misaligned buffer just incurs slightly slower
 * uint64_t loads, not a crash.
 */
static bool buf_is_zero(const void *buf, size_t len)
{
	const uint64_t *q = (const uint64_t *)buf;
	size_t qcount = len / 8;
	size_t i;

	for (i = 0; i < qcount; i++) {
		if (q[i] != 0)
			return false;
	}

	const uint8_t *p = (const uint8_t *)(q + qcount);
	size_t rem = len - qcount * 8;
	for (i = 0; i < rem; i++) {
		if (p[i] != 0)
			return false;
	}
	return true;
}

/*
 * FIEMAP for a single extent at (fd, loff). Writes the physical
 * address of the containing extent into *out_phys. Returns 0 on
 * success, errno on failure. Used both at canonical adoption time
 * (to record phys for later rotation checks) and at rotation time
 * (to verify a candidate isn't pointing at the current canonical).
 */
static int fiemap_phys_at(int fd, uint64_t loff, uint64_t *out_phys)
{
	struct {
		struct fiemap fm;
		struct fiemap_extent fe[1];
	} req;
	int ret;

	memset(&req, 0, sizeof(req));
	req.fm.fm_start = loff;
	req.fm.fm_length = 1;
	req.fm.fm_flags = FIEMAP_FLAG_SYNC;
	req.fm.fm_extent_count = 1;

	ret = ioctl(fd, FS_IOC_FIEMAP, &req);
	if (ret < 0)
		return errno;
	if (req.fm.fm_mapped_extents != 1)
		return ENOENT;
	if (req.fe[0].fe_flags & FIEMAP_EXTENT_UNKNOWN)
		return ENOENT;

	*out_phys = req.fe[0].fe_physical;
	return 0;
}

/*
 * LOGICAL_INO V1 to count refs to a single 16 KiB canonical extent.
 * Sizes the buffer for cap+10 triples to bound worst-case cost (same
 * as srccount_seed). V2 is preferred but Synology DSM 7's 4.4 kernel
 * lacks it; we detect and fall back. Returns 0 + writes count, or
 * errno on failure.
 */
static int logical_ino_count(int fd, uint64_t phys, uint64_t cap,
			     uint64_t *out_count)
{
	struct btrfs_ioctl_logical_ino_args args;
	struct btrfs_data_container *container;
	size_t buf_size;
	uint64_t count;
	int ret;
	static bool v2_unsupported = false;
	static bool v2_unsupported_logged = false;

	buf_size = sizeof(*container) + (cap + 10) * 3 * sizeof(uint64_t);
	container = calloc(1, buf_size);
	if (!container)
		return ENOMEM;

	memset(&args, 0, sizeof(args));
	args.logical = phys;
	args.size = buf_size;
	args.inodes = (uintptr_t)container;

	if (!v2_unsupported) {
		ret = ioctl(fd, BTRFS_IOC_LOGICAL_INO_V2, &args);
		if (ret < 0 && (errno == ENOTTY || errno == EOPNOTSUPP)) {
			if (!v2_unsupported_logged) {
				eprintf("zero-dedupe: LOGICAL_INO_V2 "
					"not supported on this kernel "
					"(errno=%d); falling back to V1\n",
					errno);
				v2_unsupported_logged = true;
			}
			v2_unsupported = true;
			memset(&args, 0, sizeof(args));
			args.logical = phys;
			args.size = buf_size;
			args.inodes = (uintptr_t)container;
			memset(container, 0, buf_size);
		}
	}

	if (v2_unsupported)
		ret = ioctl(fd, BTRFS_IOC_LOGICAL_INO, &args);

	if (ret < 0) {
		ret = errno;
		free(container);
		return ret;
	}

	count = container->elem_cnt / 3;
	if (container->bytes_missing > 0) {
		uint64_t extra = container->bytes_missing /
				 (3 * sizeof(uint64_t));
		count += extra;
	}
	/* Cap at cap+10 to match srccount_seed semantics. */
	if (count > cap + 10)
		count = cap + 10;

	free(container);
	*out_count = count;
	return 0;
}

/*
 * Adopt (fd, off) as the new canonical. Resolves its physical
 * address and ref count, dup()s the fd so the caller can close
 * the file without invalidating us, and releases any previously-
 * held canonical fd. Returns 0 on success, errno on failure.
 *
 * If the candidate's physical address matches the current
 * canonical's, returns EAGAIN to signal "this candidate is already
 * sharing with current canonical, try another." Caller advances and
 * retries.
 */
static int adopt_canonical(struct zero_state *zs, int fd, uint64_t off)
{
	uint64_t phys = 0;
	uint64_t count = 0;
	int dup_fd;
	int ret;

	ret = fiemap_phys_at(fd, off, &phys);
	if (ret) {
		/*
		 * ENOENT means no extent maps this offset (true hole) or
		 * the extent is unwritten/preallocated. In either case we
		 * can't use this offset as a canonical (no physical address
		 * to reflink against), but it's not a hard error - just
		 * skip and try the next chunk. Other errnos (EINVAL,
		 * EOPNOTSUPP on non-btrfs) are genuine failures and bubble
		 * up so the caller can bail the whole run.
		 */
		if (ret == ENOENT)
			return EAGAIN;
		return ret;
	}

	if (zs->canon_initialized && phys == zs->canon.phys)
		return EAGAIN;

	ret = logical_ino_count(fd, phys, zs->cap, &count);
	if (ret)
		return ret;

	/* If the candidate is already at cap, skip it. */
	if (count >= zs->cap)
		return EAGAIN;

	dup_fd = dup(fd);
	if (dup_fd < 0)
		return errno;

	if (zs->canon_initialized && zs->canon.fd >= 0)
		close(zs->canon.fd);

	zs->canon.fd = dup_fd;
	zs->canon.off = off;
	zs->canon.phys = phys;
	zs->canon.count = count;
	zs->canon.rootid = 0;	/* lazy lookup on first SYNO use */
	zs->canon.objectid = 0;
	zs->canon_initialized = true;
	zs->canon_rotations++;

	/*
	 * Lazy-resolve (rootid, objectid) for the canonical only if SYNO
	 * mode is in play. Skipping this when --use-syno-dedupe=off avoids
	 * an extra ioctl + fstat per canonical rotation in the legacy path.
	 */
	if (options.use_syno_dedupe != SYNO_OFF && !g_syno_unavailable) {
		uint64_t rid = 0;
		struct stat st;

		if (lookup_btrfs_subvol(dup_fd, &rid) == 0 &&
		    fstat(dup_fd, &st) == 0) {
			zs->canon.rootid = rid;
			zs->canon.objectid = (uint64_t)st.st_ino;
		}
		/* On failure, IDs stay 0; submit_batch falls back to
		 * FIDEDUPERANGE for this canonical. */
	}

	return 0;
}

/*
 * Submit one batched dedupe: src is the current canonical, dst entries
 * are N consecutive BLOCKSIZE_BYTES-aligned positions in dst_fd
 * starting at dst_off. Returns the number of entries the kernel
 * accepted (== n on full success), or 0 on error.
 *
 * Dispatches by options.use_syno_dedupe:
 *
 *   SYNO mode (options.use_syno_dedupe != SYNO_OFF, canon ids resolved,
 *   g_syno_unavailable not set): issue N sequential pairwise
 *   syno_dedupe_ids() calls. Pairwise loses the 119:1 batching but
 *   bypasses the DSM 7 compression-mismatch check. Each call passes
 *   backref_limit = zs->cap so the kernel will return DITTO once the
 *   canonical hits cap (we use that as a signal to bail this batch).
 *
 *   FIDEDUPERANGE mode (legacy / fallback): one batched ioctl with N
 *   destinations. Unchanged from the original implementation.
 *
 * Allocates the file_dedupe_range struct on the heap to avoid blowing
 * the stack on large N. The struct is small (about 16 + 24*n bytes)
 * so this is cheap.
 */
static int submit_batch(struct zero_state *zs, int dst_fd,
			uint64_t dst_rootid, uint64_t dst_objectid,
			uint64_t dst_off, unsigned int n)
{
	struct file_dedupe_range *same;
	size_t same_size;
	int ret;
	unsigned int i;
	unsigned int accepted = 0;

	if (n == 0)
		return 0;

	/*
	 * SYNO path: issue N sequential pairwise calls. Cheaper than
	 * compression-mismatch EINVAL noise; loses batching but each call
	 * is metadata-bounded (kernel does flush+wait+check_backref_limit
	 * then either DITTO or proceed).
	 */
	if (options.use_syno_dedupe != SYNO_OFF && !g_syno_unavailable &&
	    zs->canon.rootid != 0 && zs->canon.objectid != 0 &&
	    dst_rootid != 0 && dst_objectid != 0) {
		uint64_t kern_bytes = 0;
		int syno_status = SYNO_EXTENT_SAME_MAX;
		uint32_t cap32 = zs->cap > UINT32_MAX
				 ? UINT32_MAX
				 : (uint32_t)zs->cap;

		for (i = 0; i < n; i++) {
			uint64_t dst_i = dst_off + (uint64_t)i * BLOCKSIZE_BYTES;
			int rc;

			kern_bytes = 0;
			syno_status = SYNO_EXTENT_SAME_MAX;
			rc = syno_dedupe_ids(zs->canon.fd,
					     zs->canon.rootid,
					     zs->canon.objectid,
					     zs->canon.off,
					     dst_rootid, dst_objectid, dst_i,
					     BLOCKSIZE_BYTES,
					     cap32,
					     0,	/* min_dedupe_length: irrelevant for single-block */
					     &kern_bytes, &syno_status, NULL);

			if (rc == ENOTSUP) {
				/*
				 * Fall back to FIDEDUPERANGE for the
				 * REMAINING dst entries in this batch. We've
				 * already deduped `accepted` of them via SYNO.
				 * The remaining go through the legacy path.
				 */
				break;
			}
			if (rc != 0) {
				/* Real ioctl error; stop this batch. */
				break;
			}
			if (kern_bytes >= BLOCKSIZE_BYTES) {
				accepted++;
				zs->bytes_deduped += BLOCKSIZE_BYTES;
				zs->dedupe_calls++;
			} else {
				/*
				 * DITTO (kern_bytes==0, cap hit on canon),
				 * DIFF (mismatch — shouldn't happen for
				 * verified zero blocks), or *_NOT_FOUND.
				 * Stop this batch: rest of the canonical is
				 * presumably also saturated or otherwise
				 * not addable.
				 */
				zs->dedupe_calls++;
				break;
			}
		}

		/* If we got at least one accept or it wasn't an ENOTSUP
		 * fallback, return what we have. */
		if (accepted > 0 || !g_syno_unavailable)
			return (int)accepted;

		/* ENOTSUP on the very first call: fall through to
		 * FIDEDUPERANGE batched path for these N dst entries. */
	}

	/* FIDEDUPERANGE batched path (legacy / fallback). */
	same_size = sizeof(*same) +
		    n * sizeof(struct file_dedupe_range_info);
	same = calloc(1, same_size);
	if (!same)
		return 0;

	same->src_offset = zs->canon.off;
	same->src_length = BLOCKSIZE_BYTES;
	same->dest_count = n;
	for (i = 0; i < n; i++) {
		same->info[i].dest_fd = dst_fd;
		same->info[i].dest_offset = dst_off + (uint64_t)i * BLOCKSIZE_BYTES;
		same->info[i].bytes_deduped = 0;
		same->info[i].status = 0;
	}

	ret = ioctl(zs->canon.fd, FIDEDUPERANGE, same);
	if (ret < 0) {
		/*
		 * Whole ioctl failed. Log once-ish; don't spam. The
		 * caller treats accepted=0 as "skip these entries
		 * silently and move on."
		 */
		static bool errored = false;
		if (!errored) {
			eprintf("zero-dedupe: FIDEDUPERANGE failed: %s\n",
				strerror(errno));
			errored = true;
		}
		free(same);
		return 0;
	}

	for (i = 0; i < n; i++) {
		if (same->info[i].status == 0 &&
		    same->info[i].bytes_deduped == BLOCKSIZE_BYTES) {
			accepted++;
			zs->bytes_deduped += BLOCKSIZE_BYTES;
		}
	}
	zs->dedupe_calls++;

	free(same);
	return (int)accepted;
}

/*
 * Print progress. Throttled to options.lookup_progress_interval, like
 * the main path. Format is a simpler variant focused on the metrics
 * relevant to zero-only mode.
 */
static void zero_print_progress(struct zero_state *zs,
				const char *current_path, bool force)
{
	struct timespec now;
	double elapsed_since, elapsed_total;
	uint64_t bytes_since;
	double speed_now_mbs, speed_avg_mbs;
	const char *name;
	int hrs, mins;
	char deduped_buf[16];
	char zero_found_buf[16];
	double files_pct = 0.0;
	double bytes_pct = 0.0;
	uint64_t cap_progress;

	clock_gettime(CLOCK_MONOTONIC, &now);
	elapsed_since = (now.tv_sec - zs->last_progress_time.tv_sec) +
			(now.tv_nsec - zs->last_progress_time.tv_nsec) / 1e9;

	if (!force &&
	    elapsed_since < (double)options.lookup_progress_interval)
		return;

	elapsed_total = (now.tv_sec - zs->start_time.tv_sec) +
			(now.tv_nsec - zs->start_time.tv_nsec) / 1e9;
	bytes_since = zs->bytes_scanned_total - zs->bytes_scanned_at_last;
	speed_now_mbs = (elapsed_since > 0.001) ?
		(bytes_since / 1048576.0 / elapsed_since) : 0.0;
	speed_avg_mbs = (elapsed_total > 0.001) ?
		(zs->bytes_scanned_total / 1048576.0 / elapsed_total) : 0.0;

	if (current_path) {
		name = strrchr(current_path, '/');
		name = name ? name + 1 : current_path;
	} else {
		name = "(idle)";
	}

	hrs = (int)(elapsed_total / 3600);
	mins = (int)((elapsed_total - hrs * 3600) / 60);

	fmt_size_h(zs->bytes_deduped, deduped_buf, sizeof(deduped_buf));
	fmt_size_h(zs->zero_bytes_found, zero_found_buf,
		   sizeof(zero_found_buf));

	if (zs->total_files_in_walk > 0)
		files_pct = 100.0 * zs->files_visited /
			    zs->total_files_in_walk;
	if (zs->total_bytes_to_scan > 0) {
		uint64_t done = zs->bytes_scanned_total +
				zs->bytes_skipped_start_from +
				zs->bytes_skipped_too_small;
		bytes_pct = 100.0 * done / zs->total_bytes_to_scan;
	}

	cap_progress = zs->canon_initialized ? zs->canon.count : 0;

	fprintf(stderr,
		"[zero] file %"PRIu64"/%"PRIu64" \"%-30.30s\" "
		"(corpus %5.1f%%f %5.1f%%b) | "
		"%5.0f MB/s now %5.0f avg | "
		"runs %"PRIu64" zero %s | "
		"canon %"PRIu64"/%"PRIu64" rotations %"PRIu64" calls %"PRIu64" | "
		"deduped %s | %dh%02dm%s",
		zs->files_visited, zs->total_files_in_walk,
		name,
		files_pct, bytes_pct,
		speed_now_mbs, speed_avg_mbs,
		zs->zero_runs_found, zero_found_buf,
		cap_progress, zs->cap,
		zs->canon_rotations, zs->dedupe_calls,
		deduped_buf,
		hrs, mins,
		zs->is_tty ? "\033[K\r" : "\n");
	fflush(stderr);

	zs->last_progress_time = now;
	zs->bytes_scanned_at_last = zs->bytes_scanned_total;
	zs->progress_active = true;
}

/*
 * Dedupe a single contiguous zero run [run_start, run_end). Both
 * are absolute file offsets, blocksize-aligned. The run length is
 * not required to be a 16 KiB multiple; we round down internally,
 * dropping any 4-12 KiB tail.
 *
 * Issues one or more batched FIDEDUPERANGE calls, rotating the
 * canonical when its count hits cap. If we can't rotate (no fresh
 * 16 KiB region available in the current run), we drop the
 * remainder silently - the caller's next zero run will get another
 * chance at finding a fresh canonical.
 */
static void dedupe_run(struct zero_state *zs, int dst_fd,
		       uint64_t dst_rootid, uint64_t dst_objectid,
		       uint64_t run_start, uint64_t run_end,
		       const char *path)
{
	uint64_t off = run_start;
	uint64_t end = run_end;

	zs->zero_runs_found++;
	zs->zero_bytes_found += (run_end - run_start);

	/* Both run_start and run_end are BLOCKSIZE-aligned multiples by
	 * construction in zero_scan_file, so no rounding is needed.
	 * Caller already guarantees end > start; defensive guard anyway. */
	if (end <= off)
		return;

	/*
	 * Initialize canonical from this run's first chunks if we don't
	 * have one yet. EAGAIN means the candidate's phys matched the
	 * (non-existent on first init) current canonical OR the candidate
	 * itself is already at cap. Either way: skip and try the next
	 * chunk. Hard error: bail on the whole run.
	 */
	while (!zs->canon_initialized && off + BLOCKSIZE_BYTES <= end) {
		int ret = adopt_canonical(zs, dst_fd, off);
		off += BLOCKSIZE_BYTES;
		if (ret == 0)
			break;
		if (ret == EAGAIN)
			continue;
		eprintf("zero-dedupe: cannot init canonical in "
			"\"%s\" @ %"PRIu64": %s\n",
			path, off - BLOCKSIZE_BYTES, strerror(ret));
		return;
	}
	if (!zs->canon_initialized)
		return;	/* exhausted run searching for a usable canonical */

	while (off + BLOCKSIZE_BYTES <= end) {
		uint64_t headroom_refs;
		uint64_t avail_chunks;
		unsigned int batch_n;
		int accepted;

		if (zs->canon.count >= zs->cap) {
			/* Cap hit on current canonical. Adopt this
			 * chunk as the new canonical. */
			int ret = adopt_canonical(zs, dst_fd, off);
			if (ret == EAGAIN) {
				/* Candidate already shares with old
				 * canonical (or other already-shared
				 * with someone at cap). Skip and try
				 * the next chunk. */
				off += BLOCKSIZE_BYTES;
				continue;
			}
			if (ret != 0) {
				/* FIEMAP / V1 / dup failure. Give up
				 * on this run. */
				zs->zero_runs_skipped_no_headroom++;
				return;
			}
			off += BLOCKSIZE_BYTES;
			continue;
		}

		/*
		 * src and dst must not overlap; if we're in the same file
		 * as the canonical, ensure dst_off + len <= canon_off OR
		 * dst_off >= canon_off + len. Since we're advancing
		 * forward and the canonical is at some earlier offset
		 * within this run (or in a prior file), the only overlap
		 * risk is if dst_off == canon_off. Guard with explicit
		 * check.
		 */
		if (dst_fd == zs->canon.fd && off == zs->canon.off) {
			off += BLOCKSIZE_BYTES;
			continue;
		}

		headroom_refs = zs->cap - zs->canon.count;
		avail_chunks = (end - off) / BLOCKSIZE_BYTES;
		batch_n = (unsigned int)avail_chunks;
		if (batch_n > MAX_DEDUPES_PER_IOCTL - 1)
			batch_n = MAX_DEDUPES_PER_IOCTL - 1;
		if ((uint64_t)batch_n > headroom_refs)
			batch_n = (unsigned int)headroom_refs;

		if (batch_n == 0) {
			off += BLOCKSIZE_BYTES;
			continue;
		}

		accepted = submit_batch(zs, dst_fd,
					dst_rootid, dst_objectid,
					off, batch_n);
		/*
		 * Whatever the kernel accepted counts toward the
		 * canonical's ref total. If accepted < batch_n the
		 * remainder isn't retried - it's likely a status-per-
		 * entry failure (typically content mismatch, which
		 * shouldn't happen for verified-zero blocks, but we
		 * play defensive).
		 */
		if (accepted > 0)
			zs->canon.count += (uint64_t)accepted;

		off += (uint64_t)batch_n * BLOCKSIZE_BYTES;
	}
}

/*
 * Scan one open file for zero runs and dedupe them. Reads in
 * SCAN_CHUNK-sized blocks via pread. Handles zero runs that
 * straddle chunk boundaries by extending forward across chunks
 * before issuing the dedupe.
 *
 * blocksize-aligned positions only (matching the main path's
 * 4 KiB-step seed positions). A zero run must be at least
 * BLOCKSIZE_BYTES to be considered for dedupe.
 */
static void zero_scan_file(struct zero_state *zs, const char *path,
			   int fd, uint64_t size)
{
	char *buf;
	uint64_t buf_off = 0;	/* file offset of buf[0]; valid iff buf_len>0 */
	size_t buf_len = 0;
	uint64_t file_pos = 0;
	uint64_t dst_rootid = 0, dst_objectid = 0;

	if (size < BLOCKSIZE_BYTES)
		return;

	buf = malloc(SCAN_CHUNK);
	if (!buf) {
		eprintf("zero-dedupe: out of memory scanning \"%s\"\n", path);
		return;
	}

	/*
	 * Resolve (rootid, objectid) for this dst file once. Only needed
	 * when SYNO mode is active; submit_batch falls back to FIDEDUPERANGE
	 * if either id stays 0 (e.g., lookup failed). One BTRFS_IOC_INO_LOOKUP
	 * + fstat per file, dwarfed by the file's read cost.
	 */
	if (options.use_syno_dedupe != SYNO_OFF && !g_syno_unavailable) {
		struct stat st;
		uint64_t rid = 0;

		if (lookup_btrfs_subvol(fd, &rid) == 0 &&
		    fstat(fd, &st) == 0) {
			dst_rootid = rid;
			dst_objectid = (uint64_t)st.st_ino;
		}
	}

	while (file_pos + BLOCKSIZE_BYTES <= size) {
		size_t in_buf;

		/*
		 * Refill if buf doesn't cover [file_pos, file_pos+MIN_DEDUPE).
		 * The condition handles: (a) initial state (buf_len=0),
		 * (b) file_pos is before buffer start (shouldn't happen
		 * since we only advance forward but defensive),
		 * (c) needed window extends past buffer end.
		 */
		if (buf_len == 0 || file_pos < buf_off ||
		    file_pos + BLOCKSIZE_BYTES > buf_off + buf_len) {
			size_t want = (size - file_pos < SCAN_CHUNK) ?
				(size_t)(size - file_pos) : SCAN_CHUNK;
			ssize_t got = pread(fd, buf, want, file_pos);
			if (got <= 0) {
				if (got < 0)
					eprintf("zero-dedupe: pread \"%s\" "
						"@ %"PRIu64": %s\n",
						path, file_pos,
						strerror(errno));
				break;
			}
			if ((size_t)got < BLOCKSIZE_BYTES) {
				/* Short read; can't proceed at this offset.
				 * Should only happen on signal interruption
				 * very near EOF. Bail rather than loop. */
				break;
			}
			buf_off = file_pos;
			buf_len = (size_t)got;
		}

		in_buf = (size_t)(file_pos - buf_off);

		if (!buf_is_zero(buf + in_buf, BLOCKSIZE_BYTES)) {
			file_pos += BLOCKSIZE_BYTES;
			zs->bytes_scanned_total += BLOCKSIZE_BYTES;
			zero_print_progress(zs, path, false);
			continue;
		}

		/*
		 * Zero seed found. Extend forward in BLOCKSIZE steps
		 * while still all-zero, refilling buf as needed across
		 * chunk boundaries.
		 */
		uint64_t run_start = file_pos;
		uint64_t run_end = file_pos + BLOCKSIZE_BYTES;

		while (run_end + BLOCKSIZE_BYTES <= size) {
			size_t check_in_buf;

			if (run_end < buf_off ||
			    run_end + BLOCKSIZE_BYTES > buf_off + buf_len) {
				size_t want = (size - run_end < SCAN_CHUNK) ?
					(size_t)(size - run_end) : SCAN_CHUNK;
				ssize_t got = pread(fd, buf, want, run_end);
				if (got <= 0) {
					if (got < 0)
						eprintf("zero-dedupe: pread "
							"\"%s\" @ %"PRIu64
							": %s\n", path,
							run_end,
							strerror(errno));
					break;
				}
				if ((size_t)got < BLOCKSIZE_BYTES)
					break;
				buf_off = run_end;
				buf_len = (size_t)got;
			}
			check_in_buf = (size_t)(run_end - buf_off);

			if (!buf_is_zero(buf + check_in_buf, BLOCKSIZE_BYTES))
				break;
			run_end += BLOCKSIZE_BYTES;
		}

		dedupe_run(zs, fd, dst_rootid, dst_objectid,
			   run_start, run_end, path);

		/* bytes_scanned_total advances by full run length; the
		 * blocks beyond the dedupable portion (4-12 KiB tail)
		 * still count as scanned. */
		zs->bytes_scanned_total += (run_end - run_start);
		file_pos = run_end;
		zero_print_progress(zs, path, false);
	}

	/* Trailing bytes below MIN_DEDUPE: count toward scanned total
	 * so corpus % converges. */
	if (file_pos < size)
		zs->bytes_scanned_total += (size - file_pos);

	free(buf);
}

/*
 * Process one regular file: open, scan, close. Handles
 * --lookup-start-from skip and the size-too-small skip.
 */
static int zero_process_one_file(const char *path, struct stat *sb,
				 struct zero_state *zs)
{
	int fd;

	zs->files_visited++;
	if (options.lookup_start_from > 0 &&
	    zs->files_visited <= options.lookup_start_from) {
		if (sb->st_size > 0)
			zs->bytes_skipped_start_from +=
				(uint64_t)sb->st_size;
		return 0;
	}

	if ((uint64_t)sb->st_size < BLOCKSIZE_BYTES) {
		if (sb->st_size > 0)
			zs->bytes_skipped_too_small +=
				(uint64_t)sb->st_size;
		return 0;
	}

	fd = open(path, O_RDONLY);
	if (fd < 0) {
		eprintf("zero-dedupe: open \"%s\": %s\n",
			path, strerror(errno));
		return 0;	/* swallow and continue */
	}

	zero_scan_file(zs, path, fd, (uint64_t)sb->st_size);

	close(fd);
	return 0;
}

/*
 * Recursive walker. Mirrors lookup_dedupe's walk_path semantics:
 * recurses into directories iff options.recurse_dirs, otherwise
 * processes only direct regular-file children of cmdline dirs.
 */
static int zero_walk_path(const char *path, struct zero_state *zs)
{
	struct stat sb;
	DIR *d;
	struct dirent *e;

	if (lstat(path, &sb) < 0) {
		eprintf("zero-dedupe: lstat \"%s\": %s\n",
			path, strerror(errno));
		return errno;
	}

	if (S_ISREG(sb.st_mode))
		return zero_process_one_file(path, &sb, zs);

	if (!S_ISDIR(sb.st_mode))
		return 0;

	d = opendir(path);
	if (d == NULL) {
		eprintf("zero-dedupe: opendir \"%s\": %s\n",
			path, strerror(errno));
		return errno;
	}

	while ((e = readdir(d)) != NULL) {
		char child[PATH_MAX];

		if (e->d_name[0] == '.' &&
		    (e->d_name[1] == 0 ||
		     (e->d_name[1] == '.' && e->d_name[2] == 0)))
			continue;
		if ((size_t)snprintf(child, sizeof(child), "%s/%s",
				     path, e->d_name) >= sizeof(child))
			continue;

		if (options.recurse_dirs) {
			zero_walk_path(child, zs);
		} else {
			struct stat csb;
			if (lstat(child, &csb) < 0)
				continue;
			if (S_ISREG(csb.st_mode))
				zero_process_one_file(child, &csb, zs);
		}
	}
	closedir(d);
	return 0;
}

/*
 * Pre-walk: same shape as lookup_dedupe's prewalk_path but writes
 * directly into zero_state fields. Duplicated rather than shared
 * because lookup_dedupe's version is static and tied to its own
 * totals struct; the count of duplicated lines is small.
 */
struct zero_prewalk_totals {
	uint64_t files;
	uint64_t bytes;
	uint64_t last_heartbeat_files;
};
#define ZERO_PREWALK_HEARTBEAT 100000ULL

static void zero_prewalk_path(const char *path,
			      struct zero_prewalk_totals *t)
{
	struct stat sb;
	DIR *d;
	struct dirent *e;

	if (lstat(path, &sb) < 0)
		return;
	if (S_ISREG(sb.st_mode)) {
		t->files++;
		if (sb.st_size > 0)
			t->bytes += (uint64_t)sb.st_size;
		if (t->files - t->last_heartbeat_files >=
		    ZERO_PREWALK_HEARTBEAT) {
			t->last_heartbeat_files = t->files;
			fprintf(stderr,
				"zero-dedupe: pre-walk %"PRIu64
				" files, %s total\n",
				t->files, pretty_size(t->bytes));
			fflush(stderr);
		}
		return;
	}
	if (!S_ISDIR(sb.st_mode))
		return;
	d = opendir(path);
	if (!d)
		return;
	while ((e = readdir(d)) != NULL) {
		char child[PATH_MAX];
		if (e->d_name[0] == '.' &&
		    (e->d_name[1] == 0 ||
		     (e->d_name[1] == '.' && e->d_name[2] == 0)))
			continue;
		if ((size_t)snprintf(child, sizeof(child), "%s/%s",
				     path, e->d_name) >= sizeof(child))
			continue;
		if (options.recurse_dirs) {
			zero_prewalk_path(child, t);
		} else {
			struct stat csb;
			if (lstat(child, &csb) < 0)
				continue;
			if (S_ISREG(csb.st_mode)) {
				t->files++;
				if (csb.st_size > 0)
					t->bytes += (uint64_t)csb.st_size;
			}
		}
	}
	closedir(d);
}

int zero_only_dedupe_main(int argc, char **argv, int filelist_idx)
{
	struct zero_state zs;
	int i;
	struct timespec pt_start, pt_end;
	double pt_elapsed;
	struct zero_prewalk_totals pt = { 0 };

	memset(&zs, 0, sizeof(zs));
	zs.canon.fd = -1;
	zs.cap = options.lookup_max_reflinks > 0 ?
		 (uint64_t)options.lookup_max_reflinks : 1000;
	zs.is_tty = isatty(STDERR_FILENO);

	if (filelist_idx >= argc) {
		eprintf("zero-dedupe: no paths supplied; nothing to do.\n");
		return EINVAL;
	}

	qprintf("zero-dedupe: cap=%"PRIu64" blocksize=%lu\n",
		zs.cap, BLOCKSIZE_BYTES);

	clock_gettime(CLOCK_MONOTONIC, &pt_start);
	fprintf(stderr, "zero-dedupe: pre-walk starting...\n");
	fflush(stderr);
	for (i = filelist_idx; i < argc; i++)
		zero_prewalk_path(argv[i], &pt);
	clock_gettime(CLOCK_MONOTONIC, &pt_end);
	pt_elapsed = (pt_end.tv_sec - pt_start.tv_sec) +
		     (pt_end.tv_nsec - pt_start.tv_nsec) / 1e9;
	zs.total_files_in_walk = pt.files;
	zs.total_bytes_to_scan = pt.bytes;
	fprintf(stderr,
		"zero-dedupe: pre-walk complete: %"PRIu64" files, "
		"%s total (%.1fs)\n",
		pt.files, pretty_size(pt.bytes), pt_elapsed);
	fflush(stderr);

	clock_gettime(CLOCK_MONOTONIC, &zs.start_time);
	zs.last_progress_time = zs.start_time;

	for (i = filelist_idx; i < argc; i++)
		zero_walk_path(argv[i], &zs);

	/* TTY line termination (matches main path's behavior). */
	if (zs.is_tty && zs.progress_active) {
		fputc('\n', stderr);
		fflush(stderr);
	}

	/* End-of-run summary. */
	{
		char dbuf[16], zbuf[16];
		fmt_size_h(zs.bytes_deduped, dbuf, sizeof(dbuf));
		fmt_size_h(zs.zero_bytes_found, zbuf, sizeof(zbuf));
		qprintf("zero-dedupe: complete: %"PRIu64" files visited, "
			"%"PRIu64" zero runs (%s), %s deduped, "
			"%"PRIu64" canonical rotations, "
			"%"PRIu64" FIDEDUPERANGE calls.\n",
			zs.files_visited,
			zs.zero_runs_found, zbuf,
			dbuf,
			zs.canon_rotations,
			zs.dedupe_calls);
	}

	if (zs.canon_initialized && zs.canon.fd >= 0)
		close(zs.canon.fd);

	return 0;
}
