#!/usr/bin/env python3
"""
backref_audit.py — find files containing high-backref-count extents.

Recurses a directory tree and, for each file, finds the maximum number of
on-disk references (backrefs) of any extent in that file. Prints ONLY files
whose max exceeds a threshold (default 5000), as:

    <maxrefs>\t<path>

to stdout. Progress and a final summary go to stderr.

How it works
------------
For each file: FIEMAP -> list of physical extents. For each distinct physical
extent, BTRFS_IOC_LOGICAL_INO_V2 -> exact backref count (elem_cnt/3 +
bytes_missing/24; bytes_missing is exact because the kernel builds the full
ref list internally). Results are cached by physical address.

The physical-extent cache is ESSENTIAL: LOGICAL_INO cost is O(total refs of the
extent) on this kernel (the output buffer is bounded, the backref walk is not —
verified in fs/btrfs/backref.c). A hot extent referenced by thousands of files
would otherwise be re-walked thousands of times. With the cache each distinct
extent is walked once.

CONCURRENCY WARNING
-------------------
Do NOT run this while a duperemove dedupe is active on the same filesystem.
Synology's kernel has an add_all_parents() loop bug under concurrent
LOGICAL_INO + dedupe on the same extent (see srccount_seed.c). Run when idle.

Needs root (LOGICAL_INO requires CAP_SYS_ADMIN). btrfs only.

Usage:
    sudo python3 backref_audit.py [--threshold N] [--min-cache R]
                                  [--progress N] [--quiet] PATH [PATH ...]
"""

import os
import sys
import stat
import struct
import fcntl
import ctypes
import argparse
import time

# ---- ioctl numbers ----------------------------------------------------------
FS_IOC_FIEMAP = 0xC020660B
BTRFS_IOC_LOGICAL_INO_V2 = 0xC038943B

# ---- FIEMAP -----------------------------------------------------------------
FIEMAP_FLAG_SYNC = 0x0001
FIEMAP_EXTENT_LAST = 0x0001
FIEMAP_EXTENT_UNKNOWN = 0x0002
FIEMAP_EXTENT_DELALLOC = 0x0004
# extents we can't / shouldn't resolve to a physical backref count
FIEMAP_SKIP = FIEMAP_EXTENT_UNKNOWN | FIEMAP_EXTENT_DELALLOC

_FIEMAP_HDR = struct.Struct("=QQIIII")          # fm_start,fm_length,flags,mapped,count,reserved
_FIEMAP_EXT = struct.Struct("=QQQQQI3I")         # fe_logical,fe_physical,fe_length,res64[2],flags,res[3]
_FIEMAP_EXT_SIZE = 56
assert _FIEMAP_EXT.size == _FIEMAP_EXT_SIZE, _FIEMAP_EXT.size

_EXTS_PER_CALL = 512


def fiemap_extents(fd, file_size):
    """Yield (fe_physical, fe_flags) for every mapped extent of fd."""
    if file_size == 0:
        return
    start = 0
    buf = bytearray(_FIEMAP_HDR.size + _EXTS_PER_CALL * _FIEMAP_EXT_SIZE)
    while start < file_size:
        _FIEMAP_HDR.pack_into(buf, 0, start, file_size - start,
                              FIEMAP_FLAG_SYNC, 0, _EXTS_PER_CALL, 0)
        fcntl.ioctl(fd, FS_IOC_FIEMAP, buf, True)
        _, _, _, mapped, _, _ = _FIEMAP_HDR.unpack_from(buf, 0)
        if mapped == 0:
            return
        last_seen = False
        off = _FIEMAP_HDR.size
        next_start = start
        for _ in range(mapped):
            (fe_logical, fe_physical, fe_length,
             _r0, _r1, fe_flags, _r2, _r3, _r4) = _FIEMAP_EXT.unpack_from(buf, off)
            off += _FIEMAP_EXT_SIZE
            yield fe_physical, fe_flags
            next_start = fe_logical + fe_length
            if fe_flags & FIEMAP_EXTENT_LAST:
                last_seen = True
        if last_seen or next_start <= start:
            return
        start = next_start


# ---- LOGICAL_INO_V2 ---------------------------------------------------------
# struct btrfs_ioctl_logical_ino_args: logical, size, reserved[3], flags, inodes
_LINO_ARGS = struct.Struct("=QQ24sQQ")
# btrfs_data_container header: bytes_left, bytes_missing, elem_cnt, elem_missed
_DC_HDR = struct.Struct("=IIII")


def logical_ino_count(fs_fd, phys, buf_size):
    """Exact backref count of the extent containing physical address `phys`.
    Returns an int, or None if the address can't be resolved (hole/error)."""
    buf = bytearray(buf_size)
    cbuf = (ctypes.c_char * buf_size).from_buffer(buf)
    args = bytearray(_LINO_ARGS.pack(phys, buf_size, b"\0" * 24, 0,
                                     ctypes.addressof(cbuf)))
    try:
        fcntl.ioctl(fs_fd, BTRFS_IOC_LOGICAL_INO_V2, args, True)
    except OSError:
        return None
    _bytes_left, bytes_missing, elem_cnt, _elem_missed = _DC_HDR.unpack_from(buf, 0)
    return elem_cnt // 3 + bytes_missing // 24


# ---- main -------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Find files with high-backref extents.")
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--threshold", type=int, default=5000,
                    help="report files whose max extent backref count exceeds this (default 5000)")
    ap.add_argument("--min-cache", type=int, default=64,
                    help="only cache counts >= this, to bound memory (default 64)")
    ap.add_argument("--buf-kib", type=int, default=64,
                    help="LOGICAL_INO output buffer size in KiB (default 64; count is exact regardless)")
    ap.add_argument("--progress", type=int, default=2000,
                    help="emit a progress line to stderr every N files (default 2000; 0 disables)")
    ap.add_argument("--quiet", action="store_true", help="suppress progress")
    args = ap.parse_args()

    buf_size = args.buf_kib * 1024
    cache = {}                       # phys -> refcount (only for counts >= min_cache)
    fs_fd = os.open(args.paths[0], os.O_RDONLY)   # any fd on the fs serves LOGICAL_INO

    n_files = n_extents = n_probes = n_hits = n_errs = 0
    t0 = time.monotonic()
    last = t0

    def progress():
        nonlocal last
        now = time.monotonic()
        rate = n_files / (now - t0) if now > t0 else 0
        sys.stderr.write(
            "[backref_audit] files=%d extents=%d probes=%d cache=%d hits=%d "
            "errs=%d %.0f files/s\n" % (n_files, n_extents, n_probes, len(cache),
                                        n_hits, n_errs, rate))
        sys.stderr.flush()
        last = now

    try:
        for root in args.paths:
            for dirpath, _dirs, files in os.walk(root):
                for name in files:
                    path = os.path.join(dirpath, name)
                    try:
                        st = os.lstat(path)
                    except OSError:
                        continue
                    if not stat.S_ISREG(st.st_mode) or st.st_size == 0:
                        continue
                    n_files += 1
                    file_max = 0
                    try:
                        ffd = os.open(path, os.O_RDONLY)
                    except OSError:
                        n_errs += 1
                        continue
                    try:
                        for phys, flags in fiemap_extents(ffd, st.st_size):
                            n_extents += 1
                            if flags & FIEMAP_SKIP or phys == 0:
                                continue
                            c = cache.get(phys)
                            if c is None:
                                c = logical_ino_count(fs_fd, phys, buf_size)
                                n_probes += 1
                                if c is None:
                                    n_errs += 1
                                    continue
                                if c >= args.min_cache:
                                    cache[phys] = c
                            if c > file_max:
                                file_max = c
                    except OSError:
                        n_errs += 1
                    finally:
                        os.close(ffd)

                    if file_max > args.threshold:
                        n_hits += 1
                        sys.stdout.write("%d\t%s\n" % (file_max, path))
                        sys.stdout.flush()

                    if (not args.quiet and args.progress
                            and n_files % args.progress == 0):
                        progress()
    finally:
        os.close(fs_fd)

    dt = time.monotonic() - t0
    sys.stderr.write(
        "[backref_audit] DONE: %d files, %d extents, %d distinct probes, "
        "%d hits, %d errors in %.1fs (%.0f files/s)\n"
        % (n_files, n_extents, n_probes, n_hits, n_errs, dt,
           n_files / dt if dt else 0))


if __name__ == "__main__":
    main()
