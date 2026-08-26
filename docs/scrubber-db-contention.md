# Privacy scrubber DB contention: root cause and fix

## Symptoms

On the production Mac deployment (276k `cloud_message` rows / 261k bridgev2
`message` rows), message bridging latency degraded badly:

- 889 slow-query warnings in 24h; median 3.3s, p90 18.5s, worst **145s**
- Live messages bridged with p90 ~22s latency, worst observed **10.5 minutes**
- Chronic since at least Aug 10; ~118 minutes/day of cumulative DB stall time

## Root cause

The privacy scrubber (`scrubBridgedBodies` + `scrubReactionText`, every 5 min)
was the dominant writer:

1. **No usable index.** Both scrubbers' candidate scans filter on
   `body_scrubbed=FALSE AND updated_ts < cutoff`, but neither column appears
   in any index (`cloud_message_portal_ts_idx`, `cloud_message_chat_ts_idx`,
   PK `(login_id, guid)`). SQLite therefore walked **every** row of
   `cloud_message` per chunk — even when only a handful were eligible.
2. **Per-chunk re-materialization.** `scrubBridgedBodies`' chunk UPDATE
   embedded a UNION subquery over the *entire* bridgev2 `message` table
   (base ids + part-suffix normalization) — recomputed from scratch for every
   1000-row chunk. A pass draining N rows paid ~(N/1000)×2 full scans of a
   261k-row table.

Each long-running statement held SQLite's single write lock and occupied one
of only 4 pooled connections, so live-message lookups (`handleMessage`,
`GetOutboundGroupSession`, portal/kv writes) queued behind it. Only ~9% of
slow live-message queries overlapped a slow scrub directly — the damage was
mostly lock/pool starvation, not single-statement blocking.

The backlog could never fully drain either: rows whose backfill failed
upstream (Beeper HTTP 500 "failed to save message batch") are correctly
*never* scrubbed (not delivered ⇒ plaintext must persist until delivered),
so they permanently sit in the candidate population the old query rescanned
every tick.

## Fix

Minimal, behavior-preserving, three pieces (all in
`pkg/connector/cloud_backfill_store.go`):

1. **Partial index** created in `ensureSchema` (after the column migrations,
   because legacy tables gain `body_scrubbed` via ALTER):
   ```sql
   CREATE INDEX IF NOT EXISTS cloud_message_scrub_idx
       ON cloud_message (login_id, updated_ts) WHERE body_scrubbed=FALSE;
   ```
   Steady-state size tracks only the un-scrubbed backlog (a few MB at most).
2. **`loadBridgedGUIDSet`**: the delivered-guid set is read **once per pass**
   into memory instead of being re-materialized inside every chunk's
   subquery. Membership semantics are byte-for-byte those of the old UNION
   (UPPER() both sides for APNs-vs-CloudKit case mismatch,
   `instr()`-based part-suffix stripping, receiver scoping) — pinned by test.
3. **Chunking rewritten**: candidates are listed once per pass via
   `scrubCandidates` (index-backed, oldest-first), sliced into 1000-row
   chunks in memory, filtered Go-side against the delivered set, then applied
   with plain `guid IN (...)` updates that preserve all write-time rechecks
   (`body_scrubbed=FALSE AND updated_ts < cutoff`). Soft-deleted rows still
   bypass the delivered-set test (cleared unconditionally), restore-pipeline
   portal exclusions are unchanged, and the infinite-loop hazard of
   "UPDATE … LIMIT with an unsatisfiable membership filter" is gone because
   progress is bounded by the finite candidate list.

`scrubReactionText` needed no code change beyond benefiting from the index.

## Tests (TDD)

New file `cloud_scrubber_perf_test.go`:

| Test | Guards |
|---|---|
| `TestEnsureSchemaCreatesScrubIndex` | index exists post-migration, is partial, and EXPLAIN QUERY PLAN uses it for the scrubber's candidate scan |
| `TestLoadBridgedGUIDSet` | case-normalization, part-suffix stripping, bridge/receiver scoping |
| `TestScrubBridgedBodiesMultiChunkPreservesBehavior` | 2500-row multi-chunk pass: delivered+aged cleared exactly, undelivered/fresh/excluded-portal kept, soft-deleted cleared without a message-table row, tapbacks out of scope, second pass = 0 |

All three were written first and observed failing/passing appropriately
before implementation; the full connector suite passes.

## Expected effect

Scrub passes should drop from minutes-scale statements (40–145s chunks) to
sub-second seeks; slow-query warns should collapse; live bridging latency
should return to the ~1–3s median seen outside scrub windows. The 16.8k-row
backlog will drain progressively each 5-min tick (bounded per pass), except
rows awaiting upstream delivery — which is correct behavior.

## Ops notes

- The index was already applied manually to the live DB before this change
  shipped; `IF NOT EXISTS` makes startup idempotent either way.
- Backfill failures ("M_UNKNOWN (HTTP 500): failed to save message batch")
  are server-side (hungryserv) and orthogonal; they also explain why some
  rows legitimately remain un-scrubbed.
