# Rebuild runbook — reset & rebuild (full reset, option ii)

_Operational sequence for eliminating the historical 100k+-backref extents and rebuilding all
sharing bounded at the cap, with no DITTO storm and no recurrence. Rationale and root-cause
analysis for every step live in `DEDUP_STATE_AND_PLAN.md`; this file is the runbook._

**Goal state:** every physical extent bounded at the reflink cap (enforced by both userspace
`cap_skip` and the Synology kernel), old 128k-ref extents freed corpus-wide, hashfile complete
and zero-free.

**Cap policy:** pick `C` for Phase 1.5 (e.g. **1000**), optionally a one-time-higher `C′ ≥ C`
for Phase 2. Keep the cap constant *within* each phase; the only legal change is the single
C→C′ step between Phase 5 and Phase 6. Keep both in the low thousands.

**Constants used below:**
- binary: `/volume1/docker/duperemove-patched`
- hashfile: `/volume1/docker/dedup.hash`
- masters: `/volume2/Archive/Master Final` (images under `…/HDD Images/<name>/image.raw`)
- audit tool: `/tmp/backref_audit.py` (from `tools/backref_audit.py`)

---

## Phase 0 — Prep (does not disturb the running dedupe)

- **0a. Zero-fix binary.** The `file_scan.c` `process_blocks` zero-skip bug is **fixed in source**
  (`is_block_zeroed(buffer->buf + i * blocksize)`). Rebuild and deploy `duperemove-patched`.
  Deploying a new binary does not affect an already-running process. **Must be deployed before the
  Phase 4 scan.**
- **0b. Choose `C` (and optional `C′`).**
- **0c.** Let the current dedupe run keep freeing space until ~5 TB is free and you're ready to
  start; then stop it.

---

## Phase 1 — Map the damage (dedupe MUST be idle)

```bash
sudo python3 /tmp/backref_audit.py --threshold 1000 "/volume2/Archive/Master Final" \
    2>&1 >/volume1/docker/highref_before.tsv | tee /volume1/docker/highref_progress.log
```
Output `highref_before.tsv` = list of master files with over-cap extents (rewrite targets) and
the baseline to diff against at the end. (Run only with the dedupe idle — concurrent
`LOGICAL_INO` + dedupe can hit the kernel `add_all_parents` loop bug.)

---

## Phase 2 — Rewrite the masters (breaks the 128k sharing)

Per file, for each flagged image:
```bash
cp --reflink=never --preserve=all "image.raw" "image.raw.new" && mv -f "image.raw.new" "image.raw"
```
- Creates fresh, fully-unshared extents; the old 128k extents lose their master references.
- **Peak space** ≈ summed logical size of all rewritten masters (~5 TB) before re-dedup shrinks them.
- **Checkpoint:** re-run `backref_audit` on a rewritten file → expect ~1 ref (no high-ref extents).

> Lower-peak variant if 5 TB free is hard to reach: rewrite → scan-into-hashfile →
> lookup-dedupe each image against the growing hashfile, one at a time, instead of rewriting all
> up front. More steps, same end state.

---

## Phase 3 — Recover zero space (optional, independent of the hashfile)

```bash
sudo /volume1/docker/duperemove-patched --zero-only-dedupe --skip-zeroes -b 4096 \
    --min-dedupe-size=16384 --lookup-max-reflinks=C \
    -r "/volume2/Archive/Master Final/HDD Images"
```
Consolidates the now-unshared zero runs (~18 GB) against a rotating, cap-bounded zero extent.
Bypasses the DB; can run any time after the rewrite.

---

## Phase 4 — Build a FRESH hashfile (not `--reset-lookup-state`)

A fresh scan (vs. reset) is what restores true `--lookup-self` after the inode change, gives a
zero-free index, and leaves no stale rows. **Requires the zero-fix binary.**

```bash
# 4a. set the old hashfile aside
mv /volume1/docker/dedup.hash /volume1/docker/dedup.hash.old

# 4b. scan the masters into a fresh hashfile (CONFIRM these flags match your original build step)
sudo /volume1/docker/duperemove-patched --hashfile=/volume1/docker/dedup.hash -b 4096 \
    --skip-zeroes --io-threads=1 -r "/volume2/Archive/Master Final"

# 4c. build the h16 secondary index
sudo /volume1/docker/duperemove-patched --hashfile=/volume1/docker/dedup.hash --build-h16-index
```

---

## Phase 5 — Phase 1.5: master self-dedup (cap C)

```bash
sudo SQLITE_TMPDIR=/volume1/docker/dedup-tmp /volume1/docker/duperemove-patched \
    --lookup-only --lookup-self --coalesce --skip-zeroes \
    --hashfile=/volume1/docker/dedup.hash -b 4096 --min-dedupe-size=16384 \
    --io-threads=1 --lookup-max-reflinks=C --lookup-progress-interval 60 \
    --lookup-fd-cache 900 -r "/volume2/Archive/Master Final/HDD Images"
```
**No `--bump-srccount-gen`** — fresh hashfile, nothing stale. Constant cap C.

---

## Phase 6 — Phase 2: extracted files (cap C, or one-time C′)

```bash
sudo SQLITE_TMPDIR=/volume1/docker/dedup-tmp /volume1/docker/duperemove-patched \
    --lookup-only --coalesce --skip-zeroes \
    --hashfile=/volume1/docker/dedup.hash -b 4096 --min-dedupe-size=16384 \
    --io-threads=1 --lookup-max-reflinks=C' --lookup-progress-interval 120 \
    --lookup-fd-cache 900 -r "<extracted-files root>"
```
If `C′ > C`, that's the clean one-time increase — no bump needed; the fresh, accurate srccounts
simply sit below `C′`.

---

## Phase 7 — Verify

```bash
sudo python3 /tmp/backref_audit.py --threshold C "/volume2/Archive/Master Final" \
    > /volume1/docker/highref_after.tsv 2>/dev/null
```
**Expect zero hits above `C`** — every physical extent is now bounded at the cap. Diff against
`highref_before.tsv` to confirm the 128k / 12.5k / etc. extents are gone. Optionally audit the
extracted-files tree too.

---

## Don't-get-wrong checklist
- [ ] Zero-fix binary deployed **before** the Phase 4 scan.
- [ ] `backref_audit` only ever run with the dedupe **idle**.
- [ ] **No mid-phase cap changes**; only the one-time C→C′ between Phase 5 and Phase 6.
- [ ] `--skip-zeroes` on every phase (zeros handled separately in Phase 3).
- [ ] Confirm the Phase 4b scan flags match your original hashfile-build command.
