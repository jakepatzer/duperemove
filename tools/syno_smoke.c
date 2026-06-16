/*
 * tools/syno_smoke.c
 *
 * Phase 0 smoke test for BTRFS_IOC_SYNO_EXTENT_SAME. Verifies struct
 * layout, ioctl number, and status-enum encoding match the running
 * Synology kernel BEFORE any production duperemove code is changed.
 *
 * This program is intentionally minimal: open two files, look up
 * (rootid, objectid) for each, issue the SYNO ioctl, print results.
 * No dedupe bookkeeping, no DB writes, no risk to the corpus.
 *
 * Build (on the DS1825+ or anywhere with a btrfs-aware libc):
 *     gcc -O2 -Wall -Wextra -o syno_smoke tools/syno_smoke.c
 *
 * Usage:
 *     syno_smoke [options] <src-path> <dst-path>
 *
 * Options:
 *     --src-offset=N      source offset in bytes (default: 0)
 *     --dst-offset=N      destination offset in bytes (default: 0)
 *     --length=N          dedupe range length in bytes (default: 4096)
 *     --backref-limit=N   kernel backref-walk limit (default: 1000)
 *     --min-dedupe=N      minimum dedupe length (default: 0)
 *     --help              show this message
 *
 * Exit codes:
 *     0 — ioctl returned a status code (success path; status printed)
 *     1 — ioctl returned -1 with errno set (printed to stderr)
 *     2 — setup error (file open / INO_LOOKUP / fstat / bad args)
 *
 * --- Phase 0 test scenarios (per plan, Stage A) ---
 *
 * Run each scenario and capture the full output. The aggregate result
 * disambiguates DITTO semantics and validates ABI assumptions.
 *
 * Setup root: pick a writable dir on the same btrfs as your corpus:
 *     TEST=/volume2/syno-smoke-test && sudo mkdir -p "$TEST"
 *
 * 1. Two freshly-created identical files (different physical extents)
 *    Expected: SUCCESS, release_size = length, failed_dst_* = 0
 *
 *      sudo dd if=/dev/urandom of=$TEST/identical_a bs=4K count=4 status=none
 *      sudo cp $TEST/identical_a $TEST/identical_b
 *      sudo sync
 *      sudo ./syno_smoke --length=16384 $TEST/identical_a $TEST/identical_b
 *
 * 2. cp --reflink=always pair (same physical extent ALREADY shared)
 *    CRITICAL: observe whether status is DITTO or SUCCESS.
 *      - DITTO  -> SYNO has an already-shared short-circuit
 *      - SUCCESS -> SYNO behaves like FIDEDUPERANGE here
 *    Result drives plan edge-case 13 (cross-canonical over-count).
 *
 *      sudo cp --reflink=always $TEST/identical_a $TEST/reflinked_b
 *      sudo sync
 *      sudo ./syno_smoke --length=16384 $TEST/identical_a $TEST/reflinked_b
 *
 * 3. Two files with different content
 *    Expected: DIFF, failed_dst_offset/failed_dst_length point at mismatch
 *
 *      sudo dd if=/dev/urandom of=$TEST/different_a bs=4K count=4 status=none
 *      sudo dd if=/dev/urandom of=$TEST/different_b bs=4K count=4 status=none
 *      sudo sync
 *      sudo ./syno_smoke --length=16384 $TEST/different_a $TEST/different_b
 *
 * 4. Forced low backref_limit on a canonical with >1 existing reflinks
 *    Expected: DITTO with failed_dst_* set; values 4 KiB-aligned.
 *
 *      sudo dd if=/dev/urandom of=$TEST/canon bs=4K count=4 status=none
 *      sudo cp --reflink=always $TEST/canon $TEST/share1
 *      sudo cp --reflink=always $TEST/canon $TEST/share2
 *      sudo dd if=/dev/urandom of=$TEST/fresh_dst bs=4K count=4 status=none
 *      sudo sync
 *      # canon now has 3 references (itself + share1 + share2). Try to dedupe
 *      # fresh_dst against canon with backref_limit=1 (already over the limit).
 *      sudo ./syno_smoke --backref-limit=1 --length=16384 \
 *          $TEST/canon $TEST/fresh_dst
 *
 * 5. Partial-mismatch test (matching prefix, mismatched tail)
 *    Expected: DIFF with failed_dst_offset = end of matching prefix
 *
 *      sudo bash -c "(head -c 8192 $TEST/identical_a; head -c 8192 /dev/urandom) > $TEST/partial"
 *      sudo sync
 *      sudo ./syno_smoke --length=16384 $TEST/identical_a $TEST/partial
 *
 * 6. Cross-canonical scenario (validates plan edge case 13)
 *    Three files all reflinked to the same physical extent.
 *    Dedupe two of them; observe whether second call indicates
 *    "already shared" via DITTO or processes as SUCCESS.
 *
 *      sudo cp --reflink=always $TEST/identical_a $TEST/cross_a
 *      sudo cp --reflink=always $TEST/identical_a $TEST/cross_b
 *      sudo cp --reflink=always $TEST/identical_a $TEST/cross_c
 *      sudo sync
 *      # Both already share; SYNO call should reveal whether
 *      # status is DITTO (already-shared detected) or SUCCESS (no-op).
 *      sudo ./syno_smoke --length=16384 $TEST/cross_a $TEST/cross_b
 *      sudo ./syno_smoke --length=16384 $TEST/cross_a $TEST/cross_c
 *
 * Cleanup when done:
 *      sudo rm -rf "$TEST"
 *
 * Pass criteria (per plan):
 *     - Scenarios 1, 3, 4, 5 produce their expected status codes
 *     - Scenarios 4 and 5 produce 4 KiB-aligned failed_dst_offset/length
 *     - Scenarios 2 and 6 produce a DEFINITE outcome we can document
 */

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <linux/btrfs.h>
#include <linux/types.h>

#ifndef BTRFS_IOCTL_MAGIC
#define BTRFS_IOCTL_MAGIC 0x94
#endif

#ifndef BTRFS_FIRST_FREE_OBJECTID
#define BTRFS_FIRST_FREE_OBJECTID 256ULL
#endif

/*
 * SYNO-specific definitions extracted from DSM 7.3 GPL source release
 * (v1000nk/linux-5.10.x.txz), include/uapi/linux/btrfs.h lines 901-924
 * and 1738. These are inside #ifdef MY_ABC_HERE in the Synology source
 * and are not in upstream Linux headers; we define them locally.
 */
#ifndef BTRFS_IOC_SYNO_EXTENT_SAME

enum btrfs_ioctl_syno_extent_same_status {
	SYNO_EXTENT_SAME_SUCCESS = 0,
	SYNO_EXTENT_SAME_DITTO = 1,
	SYNO_EXTENT_SAME_DIFF = 2,
	SYNO_EXTENT_SAME_SRC_NOT_FOUND = 3,
	SYNO_EXTENT_SAME_DST_NOT_FOUND = 4,
	SYNO_EXTENT_SAME_MAX
};

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

static const char *status_name(uint8_t s)
{
	switch (s) {
	case SYNO_EXTENT_SAME_SUCCESS:       return "SUCCESS";
	case SYNO_EXTENT_SAME_DITTO:         return "DITTO";
	case SYNO_EXTENT_SAME_DIFF:          return "DIFF";
	case SYNO_EXTENT_SAME_SRC_NOT_FOUND: return "SRC_NOT_FOUND";
	case SYNO_EXTENT_SAME_DST_NOT_FOUND: return "DST_NOT_FOUND";
	default:                             return "(unknown)";
	}
}

/* Returns 0 on success, errno on failure. */
static int get_rootid(int fd, uint64_t *out)
{
	struct btrfs_ioctl_ino_lookup_args args;

	memset(&args, 0, sizeof(args));
	args.objectid = BTRFS_FIRST_FREE_OBJECTID;

	if (ioctl(fd, BTRFS_IOC_INO_LOOKUP, &args) < 0)
		return errno;

	*out = args.treeid;
	return 0;
}

/* Returns 0 on success, errno on failure. */
static int get_objectid(int fd, uint64_t *out)
{
	struct stat st;

	if (fstat(fd, &st) < 0)
		return errno;

	*out = (uint64_t)st.st_ino;
	return 0;
}

static void usage(const char *prog)
{
	fprintf(stderr,
	    "Usage: %s [options] <src-path> <dst-path>\n"
	    "Options:\n"
	    "  --src-offset=N      source offset in bytes (default: 0)\n"
	    "  --dst-offset=N      destination offset in bytes (default: 0)\n"
	    "  --length=N          dedupe range length in bytes (default: 4096)\n"
	    "  --backref-limit=N   kernel backref-walk limit (default: 1000)\n"
	    "  --min-dedupe=N      minimum dedupe length (default: 0)\n"
	    "  --help              show this message\n",
	    prog);
}

int main(int argc, char **argv)
{
	uint64_t src_off = 0, dst_off = 0, length = 4096;
	uint32_t backref_limit = 1000, min_dedupe = 0;
	int opt;

	static const struct option long_opts[] = {
		{ "src-offset",    required_argument, NULL, 1 },
		{ "dst-offset",    required_argument, NULL, 2 },
		{ "length",        required_argument, NULL, 3 },
		{ "backref-limit", required_argument, NULL, 4 },
		{ "min-dedupe",    required_argument, NULL, 5 },
		{ "help",          no_argument,       NULL, 'h' },
		{ NULL, 0, NULL, 0 },
	};

	while ((opt = getopt_long(argc, argv, "h", long_opts, NULL)) != -1) {
		switch (opt) {
		case 1: src_off       = strtoull(optarg, NULL, 0); break;
		case 2: dst_off       = strtoull(optarg, NULL, 0); break;
		case 3: length        = strtoull(optarg, NULL, 0); break;
		case 4: backref_limit = (uint32_t)strtoul(optarg, NULL, 0); break;
		case 5: min_dedupe    = (uint32_t)strtoul(optarg, NULL, 0); break;
		case 'h':
			usage(argv[0]);
			return 0;
		default:
			usage(argv[0]);
			return 2;
		}
	}

	if (argc - optind != 2) {
		usage(argv[0]);
		return 2;
	}

	const char *src_path = argv[optind];
	const char *dst_path = argv[optind + 1];

	int src_fd = open(src_path, O_RDONLY);
	if (src_fd < 0) {
		fprintf(stderr, "open(%s): %s\n", src_path, strerror(errno));
		return 2;
	}

	int dst_fd = open(dst_path, O_RDONLY);
	if (dst_fd < 0) {
		fprintf(stderr, "open(%s): %s\n", dst_path, strerror(errno));
		close(src_fd);
		return 2;
	}

	uint64_t src_rootid = 0, src_objectid = 0;
	uint64_t dst_rootid = 0, dst_objectid = 0;
	int rc;

	rc = get_rootid(src_fd, &src_rootid);
	if (rc) {
		fprintf(stderr, "INO_LOOKUP on src: %s\n", strerror(rc));
		close(src_fd); close(dst_fd);
		return 2;
	}
	rc = get_objectid(src_fd, &src_objectid);
	if (rc) {
		fprintf(stderr, "fstat on src: %s\n", strerror(rc));
		close(src_fd); close(dst_fd);
		return 2;
	}
	rc = get_rootid(dst_fd, &dst_rootid);
	if (rc) {
		fprintf(stderr, "INO_LOOKUP on dst: %s\n", strerror(rc));
		close(src_fd); close(dst_fd);
		return 2;
	}
	rc = get_objectid(dst_fd, &dst_objectid);
	if (rc) {
		fprintf(stderr, "fstat on dst: %s\n", strerror(rc));
		close(src_fd); close(dst_fd);
		return 2;
	}

	printf("== inputs ==\n");
	printf("  src: %s\n", src_path);
	printf("       rootid=%" PRIu64 " objectid=%" PRIu64
	       " offset=%" PRIu64 "\n",
	       src_rootid, src_objectid, src_off);
	printf("  dst: %s\n", dst_path);
	printf("       rootid=%" PRIu64 " objectid=%" PRIu64
	       " offset=%" PRIu64 "\n",
	       dst_rootid, dst_objectid, dst_off);
	printf("  length=%" PRIu64 " backref_limit=%" PRIu32
	       " min_dedupe=%" PRIu32 "\n",
	       length, backref_limit, min_dedupe);

	struct btrfs_ioctl_syno_extent_same_args args;
	memset(&args, 0, sizeof(args));
	args.src_rootid        = src_rootid;
	args.src_objectid      = src_objectid;
	args.src_offset        = src_off;
	args.dst_rootid        = dst_rootid;
	args.dst_objectid      = dst_objectid;
	args.dst_offset        = dst_off;
	args.length            = length;
	args.min_dedupe_length = min_dedupe;
	args.backref_limit     = backref_limit;
	/* output fields zeroed by memset */

	printf("\n== calling BTRFS_IOC_SYNO_EXTENT_SAME (sizeof args = %zu) ==\n",
	       sizeof(args));

	int ret = ioctl(src_fd, BTRFS_IOC_SYNO_EXTENT_SAME, &args);
	int saved_errno = errno;

	/*
	 * Per Synology kernel source (reflink.c:1839, btrfs_ioctl_syno_extent_same):
	 *   if (!ret) {
	 *       ret = copy_to_user(argp, same, sizeof(*same));
	 *       if (ret)
	 *           ret = -EFAULT;
	 *       else if (same->status)
	 *           ret = -EMLINK; // this errno should be handled in user space
	 *   }
	 *
	 * So -EMLINK is a SIGNAL that args.status is non-zero, NOT a fatal
	 * error. The args struct is fully populated when EMLINK is returned —
	 * we just need to read the status field.
	 */
	if (ret < 0 && saved_errno != EMLINK) {
		fprintf(stderr, "\nioctl returned -1, errno=%d (%s)\n",
		        saved_errno, strerror(saved_errno));
		if (saved_errno == ENOTTY || saved_errno == EOPNOTSUPP)
			fprintf(stderr,
			    "  => SYNO_EXTENT_SAME ioctl NOT SUPPORTED on this kernel.\n"
			    "     Check that the kernel is DSM 7 with the MY_ABC_HERE\n"
			    "     reflink patches (not stock upstream Linux 5.10).\n");
		close(src_fd); close(dst_fd);
		return 1;
	}

	if (ret < 0)
		printf("\n== ioctl returned -1 errno=EMLINK (status field is set; read args) ==\n");
	else
		printf("\n== ioctl returned ret=%d ==\n", ret);
	printf("  status              = %u (%s)\n",
	       args.status, status_name(args.status));
	printf("  release_size        = %" PRIu64 " bytes\n",
	       (uint64_t)args.release_size);
	printf("  failed_dst_offset   = %" PRIu64 "\n",
	       (uint64_t)args.failed_dst_offset);
	printf("  failed_dst_length   = %" PRIu64 "\n",
	       (uint64_t)args.failed_dst_length);

	/* Interpretation: bytes the kernel actually processed before stopping. */
	uint64_t prefix_bytes = 0;
	if (args.status == SYNO_EXTENT_SAME_SUCCESS) {
		prefix_bytes = length;
		printf("  (interpreted) bytes_deduped = %" PRIu64
		       " (full length)\n", prefix_bytes);
	} else if (args.status == SYNO_EXTENT_SAME_DITTO ||
	           args.status == SYNO_EXTENT_SAME_DIFF) {
		if ((uint64_t)args.failed_dst_offset >= dst_off)
			prefix_bytes = (uint64_t)args.failed_dst_offset - dst_off;
		printf("  (interpreted) bytes_deduped (prefix) = %" PRIu64 "\n",
		       prefix_bytes);
		printf("  (interpreted) stopped region length  = %" PRIu64 "\n",
		       (uint64_t)args.failed_dst_length);
	}

	/* Alignment sanity */
	if (args.failed_dst_offset && (args.failed_dst_offset % 4096))
		printf("  WARNING: failed_dst_offset is NOT 4 KiB aligned\n");
	if (args.failed_dst_length && (args.failed_dst_length % 4096))
		printf("  WARNING: failed_dst_length is NOT 4 KiB aligned\n");

	close(src_fd);
	close(dst_fd);
	return 0;
}
