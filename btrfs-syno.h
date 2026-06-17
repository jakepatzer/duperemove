/*
 * btrfs-syno.h
 *
 * Synology DSM-specific BTRFS ioctl definitions. These are gated by
 * #ifdef MY_ABC_HERE in Synology's published GPL source (DSM 7.3,
 * v1000nk/linux-5.10.x, include/uapi/linux/btrfs.h:901-924, 1738) and
 * are not present in upstream Linux kernel headers, so we declare them
 * locally with #ifndef guards so future upstream additions take precedence.
 *
 * The wire protocol of BTRFS_IOC_SYNO_EXTENT_SAME is unusual:
 *
 *   - ioctl returns 0 when status==SUCCESS
 *   - ioctl returns -1 with errno=EMLINK when status is non-zero
 *     (DITTO/DIFF/SRC_NOT_FOUND/DST_NOT_FOUND); the args struct is
 *     fully populated in either case
 *   - ioctl returns -1 with errno != EMLINK for genuine failures
 *     (ENOTTY, EOPNOTSUPP, EPERM, EAGAIN, EROFS, ETXTBSY, etc.)
 *
 * From reflink.c:1839 in the Synology source:
 *     if (!ret) {
 *         ret = copy_to_user(...);
 *         if (ret)
 *             ret = -EFAULT;
 *         else if (same->status)
 *             ret = -EMLINK; // this errno should be handled in user space
 *     }
 *
 * Phase 0 smoke test (tools/syno_smoke.c) verified the ABI layout and
 * the EMLINK protocol on the user's DS1825+ kernel.
 */

#ifndef DUPE_BTRFS_SYNO_H
#define DUPE_BTRFS_SYNO_H

#include <linux/types.h>
#include <sys/ioctl.h>

#ifndef BTRFS_IOCTL_MAGIC
#define BTRFS_IOCTL_MAGIC 0x94
#endif

#ifndef BTRFS_IOC_SYNO_EXTENT_SAME

/*
 * Outcome enum for btrfs_ioctl_syno_extent_same_args.status.
 *
 * SUCCESS is returned both for fresh dedupe AND for the case where dst
 * was already sharing the requested range with src (no new reflink
 * added). Distinguish via release_size: 0 means "no space freed"
 * (likely already-shared or dst's old extent had other references).
 *
 * DITTO is returned ONLY for kernel-side backref_limit saturation, NOT
 * for already-shared. Phase 0 smoke test verified this empirically.
 */
enum btrfs_ioctl_syno_extent_same_status {
	SYNO_EXTENT_SAME_SUCCESS = 0,
	SYNO_EXTENT_SAME_DITTO = 1,
	SYNO_EXTENT_SAME_DIFF = 2,
	SYNO_EXTENT_SAME_SRC_NOT_FOUND = 3,
	SYNO_EXTENT_SAME_DST_NOT_FOUND = 4,
	SYNO_EXTENT_SAME_MAX
};

/*
 * Arguments for BTRFS_IOC_SYNO_EXTENT_SAME.
 *
 * Inputs:
 *   src_rootid, src_objectid:  subvolume + inode of the source file
 *   src_offset:                source offset in bytes (sector-aligned)
 *   dst_rootid, dst_objectid:  subvolume + inode of the destination file
 *   dst_offset:                destination offset in bytes (sector-aligned)
 *   length:                    range length in bytes (sector-aligned).
 *                              Kernel internally chunks at 16 MiB
 *                              (BTRFS_MAX_DEDUPE_LEN).
 *   min_dedupe_length:         after byte-comparison may truncate the
 *                              matching range, the kernel skips the
 *                              btrfs_clone if the truncated length is
 *                              below this value. Defense-in-depth
 *                              against extend_match over-claiming.
 *   backref_limit:             cap on src extent reflink count. If
 *                              exceeded for any sub-range, kernel
 *                              returns DITTO with failed_dst_offset
 *                              pointing at the saturated region.
 *
 * Outputs (populated on both ret==0 and ret==-EMLINK):
 *   failed_dst_offset:         dst offset where kernel stopped
 *                              processing (cause depends on status)
 *   failed_dst_length:         length of the unprocessed region
 *   release_size:              bytes actually freed from disk
 *                              (0 if dst was already shared or had
 *                              other references)
 *   status:                    one of btrfs_ioctl_syno_extent_same_status
 *
 * For both DITTO and DIFF:
 *   kern_bytes (bytes successfully deduped before stopping)
 *     = failed_dst_offset - dst_offset
 *   The prefix [dst_offset, failed_dst_offset) was successfully cloned;
 *   the region [failed_dst_offset, failed_dst_offset + failed_dst_length)
 *   was skipped; the tail [..., dst_offset + length) was NOT processed
 *   in this call (will be re-evaluated by caller's outer loop).
 */
struct btrfs_ioctl_syno_extent_same_args {
	__u64 src_rootid;
	__u64 src_objectid;
	__u64 src_offset;
	__u64 dst_rootid;
	__u64 dst_objectid;
	__u64 dst_offset;
	__u64 length;
	__u32 min_dedupe_length;
	__u32 backref_limit;
	__u64 failed_dst_offset;
	__u64 failed_dst_length;
	__u64 release_size;
	__u8  status;
};

#define BTRFS_IOC_SYNO_EXTENT_SAME \
	_IOWR(BTRFS_IOCTL_MAGIC, 238, struct btrfs_ioctl_syno_extent_same_args)

#endif /* BTRFS_IOC_SYNO_EXTENT_SAME */

#endif /* DUPE_BTRFS_SYNO_H */
