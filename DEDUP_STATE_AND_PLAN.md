# Duperemove fork — dedup state, root cause, and path forward

_Status snapshot: 2026-06-17. Scope: the Synology DS1825+ (DSM 7, kernel 5.10, btrfs `/volume2`, 32.71 TB encrypted) one-time-ish dedupe of ~5 TB master HDD images + R-Studio recovery output, hashfile at `/volume1/docker/dedup.hash` (~1.3 B blocks). This file is the single source of truth for the investigation; everything below is either **[verified in source]** or **[inferred]**, marked explicitly._

---

## 0. TL;DR

1. **The dangerous 100k+-backref state cannot recur under the migrated SYNO ioctl.** The Synology kernel enforces the reflink cap *per physical extent*, counting all pre-existing refs, and hard-refuses (DITTO) past the limit. **[verified in kernel source]**
2. **The 128,293-ref extents are historical damage** from **uncapped** `FIDEDUPERANGE` runs in **May 18–26 2026**, before the cap mechanism existed (first committed `081061d`, 2026-05-28). **[verified: git timeline + early transcripts]**
3. **The miscount is now an efficiency problem, not a safety problem.** `srccount`/`alias_root` are keyed per logical position `(fileid, loff)`; the cap is a per-physical-extent property. They only diverge in the presence of *untracked physical sharing* — which a clean capped run does not create. **[verified by code trace]**
4. **The current DITTO storm** is caused by: stale `srccount` values clamped to `old_cap+10` (=1010) that fell below a later-raised cap, with the generation not bumped on the stalled run → no `cap_skip`, no re-seed → every saturated canonical costs one expensive kernel DITTO; band-aids drain it too slowly at fragmentation scale. **[verified]**
5. **Chosen path: full reset option (ii)** — rewrite the master images (`cp --reflink=never`) to break all untracked sharing, `--reset-lookup-state`, re-run Phase 1.5, re-run Phase 2 with a **constant cap**. This restores the 1:1 position↔extent invariant, makes the cap correct, and **eliminates the old 128k extents** (master refs broken by the rewrite; extracted-file refs migrated to new bounded extents by the Phase 2 re-run). **No per-physical-extent code change (formerly "Track A") is needed.** **[verified reasoning]**
6. **Re-scan the masters after the rewrite (recommended).** Because `cp`+`mv` changes inodes and files are classified by `(ino,subvol)`, a re-scan is what makes Phase 1.5 run as true `--lookup-self` (populating `alias_root`), which is what keeps **Phase 2 efficient** (one seed/cap_skip per content-group instead of per-position). The hashfile is essentially just the masters, so re-scanning them *is* the rebuild, and it makes a separate reset redundant. _Reset-only without re-scan is a valid lighter fallback_ — correct and storm-free, just slower Phase 2 (transient/lookup-only Phase 1.5, no master `alias_root`). Correctness/safety are identical either way (Phase 5 seeding carries it); the difference is Phase 2 speed.
7. **Zero-block scan bug** (`file_scan.c` `process_blocks`) — the `--skip-zeroes` check tests a loop-invariant block, so zeros leak into the hashfile and some real blocks can be omitted. **[verified in source]** Must be fixed **before** the re-scan in bullet 6. (Not needed for the reset-only fallback, where existing zero rows are inert under `--skip-zeroes`.)

---

## 1. The kernel enforces the cap per physical extent [verified]

`fs/btrfs/reflink.c:1739` (Synology 5.10 GPL source):
```
ret = check_backref_limit(src, same->src_offset, same->length,
                          same->backref_limit, &ditto_offset, &ditto_len);
```
called **before** any dedupe work. `get_backref_remain` (`reflink.c:1408-1442`):
```
ref_remain  = backref_limit
ref_remain -= inline_backref_count(...)      // existing inline refs
ref_remain -= delayed_backref_count(...)     // in-flight refs
ref_remain -= get_extent_ref_remain(...)     // walks EXTENT_DATA_REF / SHARED_DATA_REF
```
So `ref_remain = backref_limit − (true total on-disk ref count of that physical extent)`; the kernel returns DITTO and refuses when it reaches 0. duperemove passes `backref_limit = options.lookup_max_reflinks` on every call (`lookup_dedupe.c:131-134` → `dedupe.c:446`), checked on the **src** (canonical/master) extent.

**Consequence:** no physical extent can be pushed past `cap` total references via `BTRFS_IOC_SYNO_EXTENT_SAME`. The 128k extents therefore predate this path (see §3). They are now **frozen** — the kernel won't grow them, won't shrink them.

---

## 2. Root cause of the miscount [verified by code trace]

`srccount` is keyed per logical position `(fileid, loff)`; the cap's job is to bound a property of the **physical extent**. They diverge only with *untracked physical sharing* — positions that physically share an extent but whose `alias_root` is NULL or wrong.

- `inc_srccount_at` and the Tier-2 success path (`lookup_dedupe.c:1515-1554`) increment only the *canonical's* counter, only for dedupes we perform against *that* position. Blind to (a) pre-existing reflinks and (b) refs added against a *different* position sharing the same extent.
- `alias_root` is written **only across ranges we successfully dedupe** (`bulk_set_alias_root`, `1447-1499`). `cap_skip` (`1146-1148`, `1202-1204`) and DITTO (`1352-1383`) never write it. `resolve_canonical` follows the alias link **one level only** (`534-614`; the code names this its "depth-1 invariant", `751-753`).
- The only userspace mechanism that sees per-physical-extent truth is **Phase 5 `LOGICAL_INO_V2` seeding** (`srccount_seed.c`): FIEMAP → physical addr → counts all backrefs. Confirmed working on Synology V2 this session (`bytes_missing` is set correctly; a 24 KB buffer yields `total_derived = 128293`).

### Why a clean capped run keeps it 1:1 (the key insight)
In a clean run, content X starts as m positions on m **distinct** extents. We dedupe ≤`cap` of them onto one canonical's extent — and **every one we dedupe gets `alias_root` set to that canonical** (success path always aliases the whole deduped range). At `cap`, the next position `cap_skip`s — it is *not* deduped, so it stays on its **own separate extent**. A later seed makes it a *new* canonical on a *new* distinct extent. Result: many extents, each ≤`cap`. So **per-position `srccount` == per-extent count, and a clean capped run cannot create an over-cap extent.**

The divergence (many unaliased canonicals on ONE physical extent) requires sharing that exists physically but isn't reflected in our bookkeeping. That comes from:
- **Uncapped history** (§3) — sharing built before `alias_root`/cap existed.
- **`--reset-lookup-state` without a rewrite** — it wipes `srccount`/`alias_root` but NOT on-disk reflinks (`duperemove.c:995`); a later run then sees physically-shared positions with NULL alias. (Phase 5 seeding mostly repairs this.)

(Earlier in this investigation I framed "multi-canonical fragmentation" as a *creation* mechanism for the 128k extent. That was wrong — corrected here. It is a *consequence* of untracked sharing, not a cause.)

---

## 3. How the 128k extents were actually created [verified: timeline + early transcripts]

- **May 19** (`968698d`): `lookup_dedupe.c` first appears.
- **May 18–26**: real, **uncapped** dedupe on the masters. `srccount`/`--lookup-max-reflinks`/`cap_skip` appear **0 times** in that session. Verbatim (May 21 21:39, scanning `…/HDD Images/old pc ssd/image.raw`):
  `duperemove-patched -drh --hashfile=… --dedupe-options=partial,same --skip-zeroes --coalesce --min-dedupe-size=16384 -b 4096 …`
- **May 28** (`081061d` "phase4+5: srccount cap-aware spillover"): cap introduced for the first time, default 500.
- **June**: capped runs at 500 → 1000.
- User, Jun 1: _"in previous runs we did that several times for the same image files over and over again."_

`FIDEDUPERANGE` has no per-extent ceiling. So the May runs collapsed all copies of a hot block onto a single canonical extent → 128,293 refs. A 500- or 1000-cap run cannot produce that count; it is consistent only with the uncapped May runs. _(Caveat: the literal 128,293 isn't quoted in the early transcripts; the timeline proves the uncapped-runs premise.)_

---

## 4. Why the current run stalls (DITTO storm) [verified]

Storm progress line: `cand 62 attempts 62 ok 7 | cap_skip 0 alias 0 seed 12 bl 0 dt 0/55`.

1. Master canonicals were Phase-5-seeded at cap=1000 → clamped to **1010**, stamped at the current gen.
2. The stalled Phase 2 run raised the cap above 1010 and did **not** carry `--bump-srccount-gen` (`seed 12` proves it; the combined flag re-seeds at startup, `duperemove.c:1084-1091`).
3. So per candidate: short-circuit needs `srccount ≥ cap` → `1010 ≥ 2000`? no. Re-seed needs gen-stale → gen matches → no. The dedupe is **submitted**, the kernel walks the saturated extent and returns DITTO. `cap_skip` stays 0.
4. Band-aid #44 (`1352-1383`) slams the DITTO'd canonical to `cap` so a *re-encounter* would `cap_skip` — but fragmentation means each file hits *fresh* unaliased canonicals, so re-encounters are rare and the slam never catches up.
5. Band-aid #45 (`1577-1611`) blacklists an h16 only when `!seed_had_success`; the 7 partial successes keep `seed_had_success` true, so `bl` stays 0.

Overnight the run mostly recovered — consistent with the band-aids slowly "learning" saturated canonicals/h16s; occasional 0 MB/s stalls are fresh batches. Since we are doing a full reset, there is no need to nurse it.

### Kernel cost model [verified in source — matters for tooling]
`build_ino_list` (`ioctl.c:5390`) always returns 0; when the buffer is full it just adds to `bytes_missing`/`elem_missed`. `iterate_extent_inodes` (`backref.c:2795`) builds the **full** ref ulist via `btrfs_find_all_leafs` and iterates **all** of it. So **`LOGICAL_INO` cost is O(total refs of the extent), NOT bounded by the output buffer.** `bytes_missing` is exact because the full ulist is built. (The `srccount_seed.c:8-14` comment claiming V2 "stops walking when the buffer fills" is **inaccurate** for this kernel — the *output* is bounded, the *walk* is not. This only affects efficiency, not correctness, and does not change the reset plan.)

---

## 5. The zero-block bug [verified in source — FIX BEFORE REBUILD]

`file_scan.c` `process_blocks` (≈828-845):
```c
unsigned int nb_blocks = buffer->dl_len / blocksize;
for (unsigned int i = 0; i < nb_blocks; i++) {
    if (!is_block_ignored(ctxt->fiemap, curr_file_off) &&
        !(options.skip_zeroes &&
          is_block_zeroed(buffer->buf + buffer->dl_offset))) {   // BUG: loop-invariant
        ret = process_block(buffer->buf + i * blocksize, blocksize, curr_file_off, hashes);
        ...
    }
    curr_file_off += blocksize;
}
```
`process_block` hashes block `i` at `buffer->buf + i * blocksize`, but the zero-check reads `buffer->buf + buffer->dl_offset`, which **does not depend on `i`**. `fill_buffer` sets `dl_offset = 0` before processing (`file_scan.c:971-973`), so the check always tests **block 0** of the current read buffer for every block in that buffer.

**Effects:**
- Buffer whose block 0 is **non-zero** → `is_block_zeroed` false → *all* blocks stored, **including zero blocks** → zero blocks leak into `blocks` → into `blocks_h16` (the observed 4.6 M `6ae9da11…` rows).
- Buffer whose block 0 **is** zero → *all* blocks skipped → **real non-zero blocks omitted** from the hashfile (missed dedupe candidates; not on-disk data loss).

**Fix (one line):** test the same block being hashed —
```c
is_block_zeroed(buffer->buf + i * blocksize)
```
(`dl_offset == 0` during `process_blocks`, so this matches `process_block`'s addressing.) The lookup-path check (`lookup_dedupe.c:78` `block_is_zero`, used at `:869`) is correct and unaffected.

**When it matters:** only if you *re-scan* (rebuild `blocks`/`blocks_h16`). The reset path does **not** re-scan, so the bug is dormant for it — existing zero rows are inert under `--skip-zeroes` (lookup skips zero seeds before any candidate query, `lookup_dedupe.c:869`). Fix is required only if you choose the optional master re-scan in §6 step 4; rebuilding with the buggy scanner would reproduce the zero leak and omit some real blocks.

---

## 6. The path forward — full reset option (ii)

Goal: a clean corpus where every physical extent is bounded at `cap`, the old 128k extents are gone, and the hashfile is complete and zero-free.

**Prerequisites (code):**
- [ ] Fix the zero-block scan bug (§5).
- [ ] (Optional) Fix/relax the `srccount_seed.c:8-14` comment; no functional change required.

**Sequence:**
1. **Free space.** The rewrite temporarily *increases* usage (`cp --reflink=never` writes each master's full logical size unshared). Phase 1.5 cross-dedup needs all masters present and unshared simultaneously → peak ≈ summed logical size of all masters (~5 TB) before Phase 1.5 shrinks them. The current run's successful dedupes (`ok`) are what free the space being waited on.
   - _Lower-peak alternative:_ rewrite-and-dedupe incrementally against the hashfile (rewrite image 1 → scan into hashfile; rewrite image 2 → `--lookup-only` against the hashfile; …). Keeps peak near one image's un-deduped delta.
2. **Rewrite the master images** per-file: `cp --reflink=never --preserve=all image.raw image.raw.new && mv image.raw.new image.raw`. Breaks all untracked sharing → fresh unshared extents.
3. **Re-scan the masters (rebuild the hashfile content) — recommended.** The hashfile is essentially just the masters (~1.3 B blocks ≈ 5 TB ÷ 4 KB; extracted files are lookup-only transients, never scanned in), so re-scanning the masters *is* the rebuild. This is needed for **true `--lookup-self`**: files are classified by `(ino,subvol)` (`dbfile_describe_file`, `lookup_dedupe.c:1780`), and `cp`+`mv` changes inodes, so without a re-scan the rewritten masters are seen as **transient** (negative fileid) and Phase 1.5 runs in lookup-only mode — still correct and cap-bounded (Phase 5 seeding + kernel), but `alias_root` is not populated, which forces Phase 2 to Phase-5-seed every distinct master position (slower, though not the DITTO storm). Re-scanning also yields fresh `blocks` rows (`srccount=-1`, `alias_root` NULL), making a separate `--reset-lookup-state` redundant. **The re-scan invokes the scanner, so the §5 zero fix must land first.**
   - _Lighter fallback:_ `--reset-lookup-state` only, no re-scan → transient Phase 1.5. Correct and storm-free, saves one ~5 TB master read, but Phase 2 does more per-position seeding and the index keeps its (inert, under `--skip-zeroes`) zero rows. Zero fix not required on this path.
4. **(covered by step 3)**
5. **Phase 1.5** (`--lookup-only --lookup-self`) over the rewritten, re-scanned masters at cap **C**.
6. **Phase 2** (`--lookup-only`) over the extracted files at cap **C** (or a one-time-increased cap **C′ ≥ C**; see §7). This re-dedupes every extracted file against the new bounded master extents, migrating refs off the old 128k extents; once unreferenced, btrfs frees them.

**Result:** every new physical extent bounded at the cap by both userspace `cap_skip` and the kernel; old 128k extents freed corpus-wide; no DITTO storm (clean 1:1 invariant means `cap_skip` fires before the kernel ever needs to DITTO).

---

## 7. Cap policy and the one-time increase between phases

- **Hold the cap constant *within* each phase.** Raising mid-stream after seeding is what stranded the 1010 values below the new cap and caused the current storm.
- **A one-time increase between Phase 1.5 (C) and Phase 2 (C′ ≥ C) is safe in the clean reset scenario.** Because the clean run keeps `srccount` accurate (1:1 with reality), a master canonical with `srccount = C` and a physical extent of exactly C refs simply sits below C′ → Phase 2 dedupes extracted files against it as normal **successes** (the kernel has room: real refs C < C′), incrementing toward C′, then `cap_skip`. **No DITTO, no stall.** You do **not** need `--bump-srccount-gen` for the increase to take effect — the cap is read at runtime and compared to the (already accurate) stored value. If you *do* bump, the only cost is a cheap one-time re-seed per canonical (bounded by C′ refs), not a storm.
- The current run's "stall" is **not** re-seeding cost — it is the DITTO storm from DB-undercount-vs-reality. Re-seeding (via a gen bump) would actually *fix* the current run, not slow it.

---

## 8. Tradeoffs / open considerations

- **Cap value choice:** higher cap = better space savings (more copies collapse onto one extent) but larger per-extent backref counts (balance/scrub cost). The whole point of the rewrite is to *avoid* huge counts, so keep the cap in the low thousands (historical: 500–1000; bees uses 1024 default). Content repeated more than `cap` times will keep `⌈copies/cap⌉` physical copies — the deliberate space/safety tradeoff.
- **Extracted files are not rewritten** — only the masters. That's fine: their refs migrate during the Phase 2 re-run (§6.6). If any extracted files are internally shared, Phase 5 seeding (kept on) handles it.
- **Did NOT pursue:** the per-physical-extent `srccount` code change (formerly "Track A"). Option (ii) solves the same problem physically, so it's unnecessary. (If a future ongoing-service use ever arises without a rewrite, Track A — an in-memory phys-saturation set or a persisted `phys_srccount(phys,count,gen)` table — would be the right efficiency fix.)
- **Concurrency hazard:** `srccount_seed.c:23-28` notes an `add_all_parents()` kernel loop bug under concurrent `LOGICAL_INO` + dedupe on the same extent. Any diagnostic that runs `LOGICAL_INO` (e.g. the backref-audit script) should run when the dedupe is **idle**.

---

## 9. Tooling

- `tools/backref_audit.py` (this repo): recurse a tree, report files containing any extent with > N backrefs and that file's max. Uses FIEMAP + `LOGICAL_INO_V2` with a physical-extent cache (essential — see §4 cost model). Run when dedupe is idle.
- `/tmp/refcount_phase5_sim.py`, `/tmp/refcount_v2.py`, `/tmp/get_phys.py` (NAS): ad-hoc refcount probes (recreate from base64 blobs in chat history; `/tmp` is wiped on reboot).

## 10. Diagnostic reference data (this corpus)

Top hot h16 true ref counts (V2 ioctl): `dc949906` db 8.97M / true 5,248; `6ae9da11` (zeros) db 4.60M / true 1; `bc9f9e95` + 7 siblings (contiguous 32 KB @ phys 21616858447872): **128,293 each**; `1b6b7afa` 8,653; `40e90531` 12,500; etc. srccount histogram: 1,315,562,210 rows at −1 (never seeded), 229,251 at ≥1000. ~88% NULL `alias_root` for the hottest h16 (1,074,235 aliased vs 7,897,122 unaliased) — expected consequence of capping super-hot content (§2), not a bug.
