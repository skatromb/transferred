# Incremental sync — how Airbyte and dlt do it, and what `transferred` should borrow

Research notes for redesigning `transferred`'s incremental trait surface. The
first prototype (capability traits + destination-ID diffing for deletes) was
shelved because both phases re-scanned the whole destination. This doc records
how the two reference tools avoid that, and the redesign it implies.

All factual claims below were cross-checked against primary sources (dlt docs,
Airbyte docs, Google Cloud BigQuery guidance); 25/25 verified, sources listed at
the end.

---

## Original design

Incremental loads should be default when they're available. If incremental load is not supported by Source and Destination, `full_refresh` is used.

Requirements from `Source`:
- impl `TrackingField`: `tracking_column()` (like `updated_at` — to get new batch of rows for INSERT + UPDATE at `Destination`)
- might impl `IdField`: `id_field()` (should have unique identifying column for UPDATE and DELETE at `Destination`)

Abilities of `Source` based on requirements:
- impl `InsertAndUpdate(TrackingField)`:
    - `stream_inserts_and_updates` query rows that were updated >= current `Destination`'s `...?(get_current_track()? — bad name)` 
- impl `PropagateDeletes(IdField)`:
    - `stream_deletes(id)` stream ids from `Destination` that are deleted at `Source`. Should be able to be streamed, not all values at once to support 1 billion rows tables.

Requirements from `Destination`:
- impl `IdField` — for UPDATE and DELETE

Abilities of `Destination` based on requirements:
- impl `IncrementalMerge(IdField? — maybe getting this from `Source`?)`: `merge_rows()` to apply INSERTs and UPDATEs. If row exists (by `IdFIeld`), we UPDATE it, if not — INSERT.
    - if not `IdField`, streams inserts and producing possible duplicates, since it doesn't know if row is updated or it's a newly created row.
- might impl `IncrementalDeletes` to sync deleted rows:
    - `stream_existing_ids(IdField)`, so that source may check then, if they still exist
    - `apply_deleted_rows(IdField)`, so that it deletes those rows, that are reported as absent by source.


## The core problem we hit

The shelved design propagated deletes by enumerating **every ID in the
destination**, streaming them to the source, diffing, and writing back. Upserts
similarly assumed a destination scan to merge. Both are O(destination) per run —
unacceptable for billion-row tables.

**Key result: neither Airbyte nor dlt scans the whole destination.** Both stage
the incoming batch and mutate only the keys present in that batch.

---

## Decisions (this session)

Conclusions reached while reviewing the research. These drive the redesign and
**supersede** the shelved prototype's stance.

### D1. Full refresh is the default. Incremental is opt-in only.

**`transferred` does NOT auto-pick incremental.** Default load = **full refresh**.
The user must explicitly ask for incremental; we never infer it from
capabilities.

Rationale: there is no way to do incremental and stay **100% in sync with the
source**. Source-side hard-deletes are invisible without a soft-delete marker or
CDC (see D4); watermark boundaries can skip or duplicate rows; mutable rows
spread across partitions defeat pruning. Full refresh is the only mode that is
trivially, provably correct. So correctness is the default; incremental is a
performance opt-in the user takes on with eyes open.

This reverses the shelved design's auto-inference ("Incremental when both sides
support it"). That stance is dropped.

### D2. Partition key must be immutable.

Partitioning the destination on a **mutable** column (e.g. `updated_at`) breaks
both correctness and pruning. Example: row PK=5 created Jan1 lands in partition
Jan1; later `updated_at` becomes Mar1.

- With a static-range prune (`updated_at BETWEEN ~Mar1`): the old row in
  partition Jan1 is pruned away → `NOT MATCHED` → INSERT → **two rows for PK=5**.
  Corruption.
- Without the prune: MERGE scans all partitions, finds + moves the row →
  correct, but **full scan**. Pruning gone.

On a mutable partition column, correctness and pruning are mutually exclusive.
**Rule:** partition by an immutable column; use the mutable cursor only for the
source read (`WHERE updated_at > watermark`), never for destination layout.

### D3. Two regimes — be honest about which we serve.

1. **Append / event tables** — timestamp immutable, rows never move. Partition by
   event time, merge a recent window, static-range prune works (the BQ
   161 GB→41 GB case is this regime).
2. **Mutable dimension tables** — rows update in place; the matching row can live
   in any partition, so **partition pruning eliminates nothing on the MERGE**.
   Only lever is **clustering by PK** (BQ block-prune) / **B-tree index on PK**
   (PG point lookup). Don't promise partition-prune savings here.

### D4. Deletes: out of scope. We do not handle them at all.

`transferred` does **not** propagate source-side deletes in incremental mode —
no CDC, no destination diffing, not even a soft-delete marker. Too much
complexity for now. If a row carries a `deleted_at`/`is_deleted` column it is
synced as ordinary data; interpreting it is the **user's** problem (e.g. a
`WHERE deleted_at IS NULL` view on their side). Only full refresh reflects
source deletions, as a side effect of rewriting everything.

Net: no `StreamDeletes`/`WriteDeletes`, no delete marker, no `deletes_since`, no
delete phase. Incremental = inserts + updates only.

### D5. Default incremental shape = append log + dedup-on-read.

With no key configured, incremental is **append-only**: changed rows
(`cursor > watermark`) get appended; versions pile up per key. Current state is
reconstructed at query time:

```sql
QUALIFY row_number() OVER (PARTITION BY primary_key ORDER BY updated_at DESC) = 1
```

Cheap writes, dedup pushed to read. A configured `primary_key` upgrades this to
replace-by-key (delete-insert) or in-place MERGE.

### D6. Naming: physical partition ≠ stream chunk → rename to "streams".

We overloaded "partition" for two unrelated things: in-flight read chunks
(parallelism) and destination **physical** storage layout (BQ/PG). Resolved:

- **Stream chunk** (the parallel-read unit, a `BatchStream`) → **"stream"**.
  Methods rename: `stream_partitions` → **`streams`**, `write_partitions` →
  **`write_streams`**. The unit literally *is* a `BatchStream`. (Rejected
  `batches` — collides with the inner `RecordBatch`; `split`/`shard` — extra
  vocab.)
- **Physical layout** keeps **`partition_key`** / `cluster_key` — "partition"
  now refers only to storage, nothing else.

Three distinct levels, three words: `RecordBatch` (rows) → `stream` (a
`BatchStream`, the parallel unit) → the source (a `Vec` of streams).

### D7. Immutable partition key: `created_at` default, else cluster-only.

The `partition_key` (D2: must be immutable) resolves:

1. **`created_at` if present** — the default. Note what it buys: it prunes
   user **time-range queries** (`WHERE created_at > …`), NOT the upsert MERGE.
   The MERGE joins on PK, which is uncorrelated with creation date, so the
   matching row may sit in any partition (D3 mutable-dimension case). MERGE rides
   **clustering by PK**, not the partition.
2. **else no partition, cluster by PK only** — the honest floor. Don't invent a
   partition column that helps no access pattern. Correct; merge rides
   clustering; no partition pruning anywhere.

### D8. Watermark boundary is inclusive: `tracking_column >= watermark`.

The source read cuts at `WHERE tracking_column >= MAX(tracking_column)`, not `>`.

`>` is correct only if the tracking column is strictly unique per write —
`updated_at` is not (timestamps collide; bulk updates stamp identical values).
With `>`, two rows sharing the watermark value → run 1 reads one, run 2 skips the
sibling → **silent data loss**. Unacceptable.

`>=` re-reads the boundary rows every run, but the overlap is absorbed by existing
dedup:
- incremental + `primary_key` → idempotent upsert; re-writing the same key is a
  no-op.
- incremental, no key (append-log, D5) → read-time `QUALIFY row_number()`
  collapses the duplicate boundary rows.

Safe in both shapes. Empty destination → `watermark = None` → no predicate → full
stream (unchanged).

### D9. Postgres scan-bounding is symmetric with BigQuery.

The cost levers map 1:1 across destinations; no trait change needed.

| concern | Postgres | BigQuery |
|---|---|---|
| upsert key constraint | `ON CONFLICT (pk)` **requires a unique index**; `MERGE` (15+) requires none | none |
| bound the merge join | **B-tree index on PK** (else seq scan / hash join over whole target) | **clustering on PK** |
| prune by time | declarative range partition + **static** predicate | range partition + **static** predicate |

Three mechanisms:

1. **`ON CONFLICT DO UPDATE`** needs an *arbiter*: a unique index/constraint on
   the conflict-target columns. No matching index → errors `there is no unique or
   exclusion constraint matching the ON CONFLICT specification`. So `ON CONFLICT
   (primary_key)` forces a `UNIQUE` index on `primary_key`.
2. **`MERGE` (PG 15+)** requires no unique constraint, but without a B-tree index
   on the join key the planner scans the **whole target** — the rescan we avoid.
   The PK index is the PG analog of BQ clustering by PK (D3 mutable-dimension lever).
3. **Partition pruning** (PG 11+, plan- and run-time) is the PG analog of BQ
   static-range pruning. Same two conditions: range-partition on the immutable
   column (D2/D7 `created_at`) **and** a static range predicate. The MIN/MAX-from-
   staging trick transfers verbatim: `target.created_at BETWEEN min AND max` pushed
   into the MERGE → PG prunes partitions.

Trait mapping: `cluster_key()` = "B-tree index on PK" (PG) / "clustering" (BQ);
`partition_key()` = declarative range partition (both). Capability probe: old PG
(<15) lacks `MERGE` → fall back to `INSERT ... ON CONFLICT` (which *forces* the
unique index).

### D10. Single `primary_key`; drop `merge_key`.

dlt's two-key model exists *because* its default strategy is delete-insert:
`merge_key` scopes the **DELETE** (`DELETE target WHERE merge_key IN (staging)`,
then INSERT), letting you replace at a coarser grain than identity. `primary_key`
is row identity for dedup + upsert match.

D4 removed deletes. With no DELETE phase, `merge_key` has nothing to scope — a
merge_key broader than identity in a pure upsert would update rows we never
fetched (nonsensical). So expose **only `primary_key`**, and let its presence
switch the shape:

- **`Some(pk)` → upsert.** Dedup staging (latest wins), then key-scoped MERGE:
  ```sql
  MERGE target T USING staging S ON T.<pk> = S.<pk>
  WHEN MATCHED THEN UPDATE SET <all non-key cols>
  WHEN NOT MATCHED THEN INSERT (<cols>) VALUES (<cols>)
  ```
  Staging dedup: `QUALIFY row_number() OVER (PARTITION BY <pk> ORDER BY <tracking_column> DESC) = 1`.
- **`None` → append-log + dedup-on-read (D5).** Same QUALIFY, at read time.

This hides the delete-insert-vs-upsert distinction entirely: those are dlt's
*delete* strategies, and we have no deletes. User picks `Full` (default) vs
`Incremental`; if incremental, optionally a `primary_key`. Nothing else.

---

## 1. Cursor / watermark state

- **dlt**: persists a high-watermark cursor (e.g. newest `updated_at`) in
  pipeline state. Next run resumes from it. The cursor exposes three values:
  `initial_value` (configured start), `start_value` (max from previous run, or
  `initial_value` on first run), `last_value` (updated live as rows yield).
- **Airbyte**: persists the max cursor value as state; extracts deltas via
  `SELECT * FROM table WHERE cursor_field > 'last_max'`.
- **Boundary caveat**: strict `cursor_field > last_max` can skip rows inserted
  mid-sync at the same cursor value. Some Airbyte connectors use `>=` and dedup
  the overlap (Airbyte issue #14732).

**For `transferred` (stateless):** we already derive the watermark from the
destination (`MAX(tracking_column)` at run start) instead of a state store —
that is the stateless-correct move and avoids a side database. Decide `>` vs
`>=` deliberately; `>=` + dedup is safer at the boundary.

---

## 2. Upsert / merge — without a destination rescan

- **dlt** offers four per-resource strategies via `write_disposition`:
  `delete-insert` (default), `upsert`, `scd2`, `insert-only`.
  - `delete-insert`: load into a **staging dataset**, dedup staging only, then
    `DELETE FROM dest WHERE key IN (staging keys)` + `INSERT`. Replace-by-key, no
    in-place UPDATE. Bounded to batch keys via temp tables holding the keys.
  - `upsert` (newer): real key-based update-or-insert — `MERGE` on SQL
    destinations, `INSERT … ON CONFLICT DO UPDATE` on DuckDB. No dedup (assumes
    primary_key already unique). Deliberately avoids delete+insert.
- **Airbyte**: classic path lands JSON in **raw tables** (`airbyte_internal`
  schema), then a SQL **Typing-and-Deduping (T+D)** query builds final typed
  tables. Newer **direct-load** path writes typed data straight into final
  tables and applies dedup via periodic **key-based upserts** to a transient
  per-sync table, deleted after the sync.
- **BigQuery canonical pattern** (GCP): land changes in a staging table, then a
  single `MERGE … WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT` keyed
  on the join condition. Same shape dbt-bigquery uses.

**For `transferred`:** the destination should **stage the batch and run a
key-scoped MERGE**, never read itself first. Match cost belongs to the engine
(PG `ON CONFLICT`, BQ `MERGE`), bounded by batch keys + the destination's
index/partition layout. Our `write_inserts_updates` should not enumerate or scan
the destination.

### delete-insert vs MERGE — same goal, different mechanics

| | delete-insert | MERGE |
|---|---|---|
| statements | 2 (`DELETE … WHERE key IN`, then `INSERT`) | 1, atomic |
| atomicity | needs txn wrap, else a window with rows missing | atomic by definition |
| column scope | replaces the **whole row** | can `SET` a subset of columns |
| constraint | none needed | PG `ON CONFLICT` needs a unique index; BQ none |
| dup keys in batch | dedup staging first | BQ **errors** if >1 source matches a target → dedup first anyway |
| delete markers | natural — marked rows simply not re-inserted | explicit `WHEN MATCHED … THEN DELETE` |

For a **full-row sync** they are near-equivalent. dlt defaults to delete-insert
because it is **portable** (no MERGE / no unique-constraint requirement) and
composes with `hard_delete`. MERGE wins on atomicity and partial-column updates.
On BQ both obey the same partition/cluster pruning rules — neither is cheaper at
the scan level.

---

## 3. Deletion — the decisive insight

- **dlt** propagates deletes via a **`hard_delete` column hint carried in the
  data** — NOT CDC, NOT a destination diff. Value-based: for `bool` columns only
  `True` deletes; for other types any non-`None` value deletes the record sharing
  the `primary_key`/`merge_key`. Works under both `delete-insert` and `upsert`.
- **Airbyte** propagates CDC deletes via a virtual column `_ab_cdc_deleted_at`
  that sources already emit. Current behavior **hard-deletes** during dedup; an
  approved RFC (#31242, rolling out from BigQuery) moves to **soft delete** —
  keep the row, add a tombstone column (null = exists, non-null = deleted).

**This eliminates our entire delete machinery.** The shelved
`stream_existing_ids` → `stream_deletes` → `write_deletes` → `deletes_since`
pipeline existed only to compute the delete set by diffing the destination.
Carrying deletion **inline as a marker row** removes the destination enumeration
completely. Deletion becomes just another row the MERGE routes to
`WHEN MATCHED … THEN DELETE` (hard) or a tombstone update (soft).

**Locked scope (D4): deletes out of scope.** Recorded here only for context on
how the reference tools work — `transferred` does not implement any of it. No
CDC, no destination diff, no soft-delete handling. Deletes are the user's
problem (D4). Incremental does inserts + updates only.

---

## 4. Staging + partition pruning (the cost lever)

- A **naive MERGE still scans the whole destination**. BigQuery DML docs: for
  non-partitioned tables "there is no way to completely avoid full table scans
  with MERGE operations."
- Pruning requires **two** conditions: the target is physically
  partitioned/clustered on the predicate column, **and** the predicate is
  rewritten from dynamic to **static**. GCP guidance: compute `MIN/MAX` over the
  small staging table, push `T.col BETWEEN min AND max` into the MERGE → BigQuery
  prunes partitions. One case study: 161 GB → 41 GB (~75%).
- dlt separates two staging concepts: a staging **dataset** (schema inside the
  warehouse for merge/dedup) and a staging **storage** (external S3/GCS bucket
  for loader files before load).

**For `transferred`:** expose optional partition/cluster keys so the BigQuery
destination can push a static-range predicate from staging. Postgres is symmetric
(D9): declarative range partitioning + a static predicate prunes the same way;
the merge join is bounded by a B-tree index on PK (analog of BQ clustering).

---

## What to borrow vs avoid

**Borrow (dlt-leaning):**
- Stage-the-batch + **key-scoped MERGE**; destination never rescans itself.
- **Single `primary_key`** (upsert identity / dedup). Dropped dlt's `merge_key` —
  it scopes a DELETE we don't do (D10).
- BigQuery **static-range partition pruning** from staging MIN/MAX.
- Real `MERGE`/`UPDATE` (dlt `upsert`) over delete+insert temp-table churn where
  the destination supports it.

**Avoid (Airbyte-leaning):**
- Persistent **raw-table T+D normalization** layer — heavy, stateful.
- **Whole-stream-rewrite / dedup-by-rescan** cadence.

---

## Redesigned trait surface (sketch)

**Default load = full refresh (D1).** Incremental is opt-in; never auto-inferred.

```
Load (user choice, default = Full):
  Full          // rewrite the whole destination — always correct, the default
  Incremental   // opt-in; user accepts the in-sync caveats (D1, D4)

Source (only consulted when Load::Incremental):
  stream_inserts_updates(since) -> batches   // inserts + updates only (no deletes, D4)
  primary_key() -> Option<&str>              // Some → upsert; None → append-log + dedup-on-read (D10)
  tracking_column() -> &str                  // for the watermark cut

Destination:
  current_watermark(tracking_column) -> Option<Watermark>   // stateless, unchanged
  merge(batches, keys) -> ()                                // stage + key-scoped MERGE (upsert)
  partition_key() -> Option<&str>            // MUST be immutable (D2); usually created_at/event time
  cluster_key()   -> Option<&str>            // usually primary_key; the real lever for mutable dims (D3)
```

**Removed vs the shelved prototype:** `StreamDeletes`, `WriteDeletes`,
`stream_existing_ids`, `stream_deletes`, `write_deletes`, the `deletes_since`
window, and the entire delete phase in `Transfer::run` — deletes are out of
scope (D4). Incremental is inserts + updates only.

The capability-probe pattern from the prototype still applies for *whether a
destination can MERGE at all* (some can only append/replace) — keep that idea,
drop the deletes-specific traits.

---

## Open questions (resolve before committing the redesign)

None — all resolved this session.

Resolved this session: deletes scope (D4 — out of scope); default mode (D1 —
full refresh); state (destination-derived `MAX(cursor)`, stays stateless);
naming (D6 — `streams`); partition key (D7 — `created_at` else cluster-only);
watermark boundary (D8 — inclusive `>=`); PG scan-bounding (D9 — symmetric with BQ);
key model (D10 — single `primary_key`, drop `merge_key`).

---

## Sources

Primary:
- dlt — merge loading: https://dlthub.com/docs/general-usage/merge-loading
- dlt — incremental loading: https://dlthub.com/docs/general-usage/incremental-loading
- dlt — incremental cursor: https://dlthub.com/docs/general-usage/incremental/cursor
- dlt — staging: https://dlthub.com/docs/dlt-ecosystem/staging
- dlt — advanced state: https://dlthub.com/docs/general-usage/incremental/advanced-state
- dlt — upsert design issue #1129: https://github.com/dlt-hub/dlt/issues/1129
- Airbyte — typing & deduping: https://docs.airbyte.com/platform/using-airbyte/core-concepts/typing-deduping
- Airbyte — direct-load tables: https://docs.airbyte.com/platform/using-airbyte/core-concepts/direct-load-tables
- Airbyte — incremental append+deduped: https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/incremental-append-deduped
- Airbyte — CDC soft-delete RFC #31242: https://github.com/airbytehq/airbyte/discussions/31242
- GCP — optimizing BigQuery incremental ingestion: https://cloud.google.com/blog/products/data-analytics/optimizing-your-bigquery-incremental-data-ingestion-pipelines
- BigQuery — DML on partitioned tables: https://cloud.google.com/bigquery/docs/using-dml-with-partitioned-tables

Secondary: DeepWiki dlt write-dispositions; Medium/Hevo BigQuery MERGE optimization write-ups.
