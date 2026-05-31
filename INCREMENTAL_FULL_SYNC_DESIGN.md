# Incremental / full-sync — shelved design

**Status: shelved, pending rethink.** This branch (`incremental-full-sync-traits-design`)
holds the full incremental-load design and the prototyped code, moved out of the
shared files so future branches don't conflict on `PLAN.md` / `DESIGN.md` /
`transfer.rs`. Nothing here merges to `main` as-is.

## Why shelved

The first cut made both phases scan the whole destination, which is too heavy:

- **Inserts/updates** need to scan the whole destination to merge (upsert).
- **Deletes** need to scan the whole destination, compare (potentially multiple
  times) against the source, then apply against the whole destination again.

Before re-applying, **study how Airbyte and dlt do incremental** (state/cursor
handling, dedup strategy, how they avoid full-destination rescans) and rethink
the trait surface accordingly. The `deletes_since` window below is a partial
mitigation, not the answer.

---

## Design narrative (consolidated — drop back into DESIGN.md § "Incremental loads")

Incremental is the default whenever both sides support it. If incremental is not
supported by Source and Destination, a full refresh is used. The strategy is the
`Load` enum — `Full` or `Incremental { deletes_since }`. The Python API takes a
`load` argument (`None` default) accepting `Full()` or
`Incremental(deletes_since=...)`; the variant classes carry their own parameters
so illegal combinations (e.g. a delete window on a full refresh) are
unrepresentable. If a `load` is passed, Transfer follows the user's choice. If
`None`, Transfer chooses `Incremental` when full sync with deletes is possible,
otherwise a full refresh.

The watermark is destination-derived: `Transfer` reads `MAX(tracking_column)`
from the destination at run start and passes it to the source as `since`. Empty
destination → `None` → source streams all rows → first run is effectively a full
load via the incremental path. User-supplied `since` override is deferred.

Phase order inside an incremental run: deletes first, then inserts/updates. If
the delete phase fails, no inserts/updates have been written yet — destination
state is unchanged. Atomic-destination guarantees still apply per phase.

`Source` requirements: impl `TrackingField` — `tracking_column()` (like
`updated_at`; used by Transfer to ask the Destination for its current watermark,
then handed back to Source as `since` for the INSERT + UPDATE stream); might impl
`IdField` — `id_field()` (unique identifying column, source-declared, used for
UPDATE and DELETE at `Destination`). `Source` abilities: impl
`StreamInsertsAndUpdates(TrackingField)` — `stream_inserts_and_updates(since)`
queries rows with `tracking_column >= since`, `since=None` (empty destination)
means stream everything; impl `StreamDeletes(IdField)` (supertrait
`StreamInsertsAndUpdates`) — `stream_deletes(existing_ids)` streams the subset of
`existing_ids` no longer present at Source, stream in / stream out so
1-billion-row tables stay constant memory.

`Destination` requirements: impl `WriteInsertsAndUpdates` (default ability for any
destination on the incremental path). `Destination` abilities: impl
`WriteInsertsAndUpdates` — `current_watermark(tracking_column)` returns the
current `MAX(tracking_column)` or `None` if the destination is empty (column name
handed in by Transfer, sourced from the Source's `tracking_column()`),
`write_inserts_and_updates(batches)` applies INSERTs and UPDATEs (if `id_field` is
available and a row matches, UPDATE; else INSERT; without `id_field`, all rows
become INSERTs and duplicates are possible); might impl `WriteDeletes` (supertrait
`WriteInsertsAndUpdates`) — `stream_existing_ids(id_column)` streams the
destination's current IDs so Source can diff them (column name handed in by
Transfer), `write_deletes(ids)` deletes rows whose IDs Source has reported as
absent.

Capability accessors are probed on the trait object actually held. `Transfer`
owns `Box<dyn Source>` / `Box<dyn Destination>`, so the entry probe
(`as_stream_inserts_updates_mut` / `as_write_inserts_updates_mut`) must live on
the **root** `Source` / `Destination` trait — a `dyn Source` exposes no subtrait
methods, and there is no runtime downcast from a supertrait object to a subtrait.
The second-level probe (`as_stream_deletes_mut` / `as_write_deletes_mut`) lives on
the direct supertrait (`StreamInsertsUpdates` / `WriteInsertsUpdates`), since by
then the object has been narrowed to that form. The supertrait bound
`StreamDeletes: StreamInsertsUpdates` (and the symmetric destination one) reflects
that delete-only sync is incoherent — a destination that only shrinks never gains
new data; it also lets the resolved `&mut dyn StreamDeletes` upcast to
`&mut dyn StreamInsertsUpdates` so one borrow drives both phases.

Capability resolves exactly once, in `Transfer::run`. Auto-mode (`load = None`)
routes to `Incremental` only when both sides expose the full stack (inserts/updates
**and** deletes); a partial-capability pair downgrades to `Full` so source-side
deletions aren't silently lost. Forced `Incremental` on a partial pair runs the
inserts/updates phase only (no delete phase), and errors if either side lacks
inserts/updates. Real runtime failures inside an enabled phase (network drop, auth
expiry, permission revoked, FK refusal, quota) propagate as `ElError`; the trait
surface only filters static capability, never swallows runtime errors. No `unwrap`
/ `expect` on capability lookups.

Delete-window bound: `Incremental` carries an optional `deletes_since` cutoff (a
tracking-column scalar). When set, `stream_existing_ids` enumerates only rows with
`tracking_column >= deletes_since`, skipping older partitions — cheaper, at the
cost of not detecting deletes of rows below the cutoff. `None` is a full sweep. The
Python `Incremental(deletes_since=datetime)` takes a timestamp; the binding
converts it to a UTC microsecond Arrow scalar (`WatermarkValue`), and connectors
cast to the tracking column's own type. Note: no connector implements the delete
phase yet, so `deletes_since` is plumbed through but not yet observable.

Deferred: explicit `Transfer::full_refresh(...)` shorthand for `Load::Full`;
explicit `since=` override to seed the watermark from outside the destination;
resolving a `deletes_since` default from a relative `Duration` (`now() -
Duration`); a proper `WatermarkValue` wrapper enforcing the length-1 /
tracking-column-type invariants. All out of scope for the initial cut.

---

## Roadmap (drop back into PLAN.md § "0.4 — incremental loads")

Goal: stateless incremental sync. Auto-dispatched when both sides expose full
capability (inserts/updates + deletes), explicit opt-in via the `load` kwarg
otherwise.

**Scope:**

- Capability traits: `StreamInsertsAndUpdates`, `StreamDeletes`,
  `WriteInsertsAndUpdates`, `WriteDeletes`. Each ability gated by its direct
  supertrait — `StreamDeletes: StreamInsertsAndUpdates`, `WriteDeletes:
  WriteInsertsAndUpdates`. Delete-only sync is not representable.
- `Load { Full, Incremental { deletes_since } }` enum + `Transfer::with_load`
  builder. Python `Transfer(..., load=None | Full() | Incremental(deletes_since=...))`
  maps directly; variant classes carry their own params.
- Auto-inference (`load = None`): `Incremental` only when both sides expose the
  full capability stack; otherwise `Full`. No silent partial-capability runs.
- Forced `Load::Incremental` on a partial-capability pair: insert/update phase
  runs, delete phase is skipped; errors only if a side lacks inserts/updates.
- Watermark is destination-derived. `Transfer` reads `MAX(tracking_column)` from
  the destination at run start, hands it to the source as `since`. Empty
  destination → `None` → first run streams everything via the incremental path.
- Phase order inside the incremental path of `run`: deletes first
  (`stream_existing_ids` → `stream_deletes` → `write_deletes`), then
  inserts/updates. Delete-phase failure leaves the destination untouched.
- `id_field` source-declared; `tracking_column` source-declared.
- Deferred: `Transfer::full_refresh(...)` shorthand for `Load::Full`, `since=`
  override to seed the watermark from outside the destination.

**Tasks (state at shelving):**

- [x] Capability traits, watermark alias, phase helpers (`incremental.rs`).
- [x] `Load` enum + `Transfer::with_load` builder + dispatch in `Transfer::run`.
- [x] Auto-inference inlined in `Transfer::run` — full-capability gate; partial
  pairs default to `Full`.
- [ ] Replace `WatermarkValue = ArrayRef` placeholder with a proper Arrow scalar
  wrapper (length-1 invariant enforced at the type level, or a wrapper newtype).
- [x] Delete-window bound: `deletes_since: Option<WatermarkValue>` carried by
  `Load::Incremental`, threaded to `delete` / `WriteDeletes::stream_existing_ids`
  (+ `tracking_column`); skips older partitions. `None` = full sweep.
- [ ] Resolve `deletes_since` from a relative `Duration` (default `now() -
  Duration`) instead of requiring an absolute timestamp.
- [x] Python `load` kwarg on `Transfer.__new__` — `Full` /
  `Incremental(deletes_since=datetime)` classes (`load.py`), decomposed to
  primitives, built into `Load` in PyO3 (`datetime` → UTC-micros `WatermarkValue`).
- [ ] Connector impls — Postgres source (`StreamInsertsAndUpdates`, `StreamDeletes`).
- [ ] Connector impls — Postgres destination (`WriteInsertsAndUpdates`, `WriteDeletes`).
- [ ] Connector impls — BigQuery destination (`WriteInsertsAndUpdates`,
  `WriteDeletes`). MERGE statement against staging for upsert; delete-by-IDs via
  staging + MERGE DELETE. (First consumer of `deletes_since`.)
- [ ] `RunReport` per-phase stats (rows inserted, rows updated, rows deleted,
  watermark advanced from/to).
- [ ] Integration tests — PG → PG incremental round-trip, PG → BQ incremental,
  drift between runs.
- [ ] Docs — Python examples covering both auto and forced modes, plus the
  "destination empty" first-run behavior.

---

## Prototyped code (reference for re-apply)

The exact code as it stood at shelving. The conflict-prone parts are the
additions to the shared `Source` / `Destination` traits and `Transfer::run` in
`transfer.rs`; `incremental.rs` and `load.py` are standalone new files.

### `crates/transferred-core/src/transfer.rs` — additions

Import:

```rust
use crate::incremental::{
    StreamInsertsUpdates, WatermarkValue, WriteInsertsUpdates, delete, insert_and_update,
};
```

`Load` enum:

```rust
/// Explicit load strategy. Set via `Transfer::with_load` (Python kwarg
/// `load`). A `None` load (the `Transfer` field) means auto-infer; see
/// `Transfer::run`.
#[derive(Debug, Clone)]
pub enum Load {
    /// Full refresh: rewrite the whole destination.
    Full,
    /// Incremental load. `deletes_since` bounds the delete phase: only rows
    /// with `tracking_column >= deletes_since` are reconciled for deletion
    /// (older partitions skipped). `None` = full delete sweep.
    Incremental {
        /// Lower-bound cutoff for the delete scan; a tracking-column scalar.
        deletes_since: Option<WatermarkValue>,
    },
}
```

Root-trait probe added to `Source`:

```rust
    /// Incremental capability probe. Default `None` → full-refresh only.
    /// Connectors supporting incremental loads override to return `Some(self)`.
    fn as_stream_inserts_updates_mut(&mut self) -> Option<&mut dyn StreamInsertsUpdates> {
        None
    }
```

Root-trait probe added to `Destination`:

```rust
    /// Incremental capability probe. Default `None` → full-refresh only.
    /// Connectors supporting incremental loads override to return `Some(self)`.
    fn as_write_inserts_updates_mut(&mut self) -> Option<&mut dyn WriteInsertsUpdates> {
        None
    }
```

`Transfer` field `load: Option<Load>`, the `with_load` builder, and the `run`
dispatch:

```rust
    /// Force a load strategy, overriding auto-inference. Maps to the Python
    /// `load` kwarg. Without this, `run` auto-infers.
    #[must_use]
    pub fn with_load(mut self, load: Load) -> Self {
        self.load = Some(load);
        self
    }

    pub async fn run(mut self) -> Result<RunReport, ElError> {
        // Resolve the requested strategy. Forced full short-circuits (consumes
        // self). `deletes_since` is carried by the incremental variant; auto
        // mode (`None`) implies a full delete sweep.
        let (incremental_requested, deletes_since) = match &self.load {
            Some(Load::Full) => return self.run_full().await,
            Some(Load::Incremental { deletes_since }) => (true, deletes_since.clone()),
            None => (false, None),
        };
        let since = None;

        // Full incremental: both sides expose deletes.
        if let (Some(source), Some(destination)) = (
            self.source
                .as_stream_inserts_updates_mut()
                .and_then(|source| source.as_stream_deletes_mut()),
            self.destination
                .as_write_inserts_updates_mut()
                .and_then(|destination| destination.as_write_deletes_mut()),
        ) {
            delete(source, destination, deletes_since.as_ref()).await?;
            // Upcast `&mut dyn StreamDeletes` -> `&mut dyn StreamInsertsUpdates`.
            insert_and_update(source, destination, since).await?;
            return Ok(TODO_RUN_REPORT);
        }

        // No full incremental capability. Downgrade to full to not miss deletes.
        if !incremental_requested {
            return self.run_full().await;
        }

        // Forced incremental without delete support: inserts/updates only.
        let source = self.source.as_stream_inserts_updates_mut().ok_or_else(|| {
            ElError::Source(
                "incremental requested, but source lacks inserts/updates capability".to_string(),
            )
        })?;

        let destination = self
            .destination
            .as_write_inserts_updates_mut()
            .ok_or_else(|| {
                ElError::Destination(
                    "incremental requested, but destination lacks inserts/updates capability"
                        .to_string(),
                )
            })?;

        insert_and_update(source, destination, since).await?;

        Ok(TODO_RUN_REPORT)
    }
```

### `crates/transferred-core/src/incremental.rs` — full module (new file)

```rust
//! Incremental load capability traits and phase helpers.
//!
//! See `DESIGN.md` § "Incremental loads" for the design narrative.
//!
//! The entry probe (`as_stream_inserts_updates_mut` / `as_write_inserts_updates_mut`)
//! lives on the root `Source` / `Destination` trait, since that is the trait
//! object `Transfer` holds; the deletes probe lives on the direct supertrait.
//! `Transfer::run` probes them at run start and routes to the incremental
//! phases or `Transfer::run_full`. Connectors that do not implement the
//! incremental traits get the default `None` probe and stay on full-refresh.

use arrow::array::ArrayRef;
use async_trait::async_trait;

use crate::{BatchStream, Destination, ElError, Source};

/// A watermark scalar — a 1-element Arrow array carrying the current
/// `MAX(tracking_column)` value at the destination.
///
/// Length is required to be 1. Convention follows Arrow Rust's "scalar as
/// length-1 array" pattern; no canonical `Scalar` type lives at this layer.
pub type WatermarkValue = ArrayRef;

/// Source capability: stream rows newer than `since` for INSERT + UPDATE.
#[async_trait]
pub trait StreamInsertsUpdates: Source {
    /// Name of the column tracked for incremental cuts (e.g. `"updated_at"`).
    fn tracking_column(&self) -> &str;

    /// Stream rows with `tracking_column >= since`. `since == None` (empty
    /// destination) streams everything.
    async fn stream_inserts_updates(
        &mut self,
        since: Option<&WatermarkValue>,
    ) -> Result<Vec<BatchStream>, ElError>;

    /// Deletes capability probe. Default `None`. Connectors that also
    /// implement `StreamDeletes` override to return `Some(self)`.
    fn as_stream_deletes_mut(&mut self) -> Option<&mut dyn StreamDeletes> {
        None
    }
}

/// Source capability: enumerate rows present at source so the destination
/// can compute the delete set.
#[async_trait]
pub trait StreamDeletes: StreamInsertsUpdates {
    /// Name of the unique identifying column (e.g. `"id"`).
    fn id_field(&self) -> &str;

    /// Given the destination's current ID set, stream the subset of IDs that
    /// no longer exist at source. Streams in / streams out so billion-row
    /// tables stay constant memory.
    async fn stream_deletes(&mut self, existing_ids: BatchStream) -> Result<BatchStream, ElError>;
}

/// Destination capability: report current high-water mark  apply
/// INSERT + UPDATE batches.
#[async_trait]
pub trait WriteInsertsUpdates: Destination {
    /// Return `MAX(tracking_column)` from the destination, or `None` if the
    /// destination is empty.
    async fn current_watermark(
        &self,
        tracking_column: &str,
    ) -> Result<Option<WatermarkValue>, ElError>;

    /// Apply INSERTs and UPDATEs. If the destination knows an `id_field`
    /// (handed to it via source-side state at write time), matching rows
    /// UPDATE; new IDs INSERT. Without an `id_field`, all rows INSERT and
    /// duplicates are possible.
    async fn write_inserts_updates(&mut self, batches: Vec<BatchStream>)
    -> Result<(), ElError>;

    /// Deletes capability probe. Default `None`. Connectors that also
    /// implement `WriteDeletes` override to return `Some(self)`.
    fn as_write_deletes_mut(&mut self) -> Option<&mut dyn WriteDeletes> {
        None
    }
}

/// Destination capability: enumerate current destination IDs and apply
/// row-level deletes.
#[async_trait]
pub trait WriteDeletes: WriteInsertsUpdates {
    /// Stream the destination's current set of `id_column` values for the
    /// source to diff against. Column names are handed in by the dispatcher
    /// (`id_column` from `StreamDeletes::id_field`, `tracking_column` from
    /// `StreamInsertsUpdates::tracking_column`).
    ///
    /// `deletes_since` bounds the scan: when `Some`, only rows with
    /// `tracking_column >= deletes_since` are enumerated, so older partitions
    /// are skipped (cheaper, but deletes of rows below the cutoff go
    /// undetected). `None` enumerates everything — a full delete sweep.
    async fn stream_existing_ids(
        &self,
        id_column: &str,
        tracking_column: &str,
        deletes_since: Option<&WatermarkValue>,
    ) -> Result<BatchStream, ElError>;

    /// Delete rows whose IDs source has reported as absent.
    async fn write_deletes(&mut self, ids: BatchStream) -> Result<(), ElError>;
}

/// Delete phase. Caller has already resolved both sides to their
/// delete-capable forms (see `Transfer::run`).
///
/// `deletes_since` bounds how far back the destination enumerates IDs (see
/// `WriteDeletes::stream_existing_ids`). `None` is a full sweep.
pub(crate) async fn delete(
    source: &mut dyn StreamDeletes,
    destination: &mut dyn WriteDeletes,
    deletes_since: Option<&WatermarkValue>,
) -> Result<(), ElError> {
    let id_column = source.id_field().to_owned();
    let tracking_column = source.tracking_column().to_owned();
    let existing = destination
        .stream_existing_ids(&id_column, &tracking_column, deletes_since)
        .await?;

    let to_delete = source.stream_deletes(existing).await?;
    destination.write_deletes(to_delete).await?;

    Ok(())
}

/// Insert + update phase.
pub(crate) async fn insert_and_update(
    src: &mut dyn StreamInsertsUpdates,
    dst: &mut dyn WriteInsertsUpdates,
    since: Option<&WatermarkValue>,
) -> Result<(), ElError> {
    let batches = src.stream_inserts_updates(since).await?;

    dst.write_inserts_updates(batches).await?;

    Ok(())
}
```

### `crates/transferred-core/src/lib.rs` — additions

```rust
pub mod incremental;
// ...
pub use incremental::{
    StreamDeletes, StreamInsertsUpdates, WatermarkValue, WriteDeletes,
    WriteInsertsUpdates,
};
pub use transfer::{BatchStream, Destination, Load, Source, Transfer};
```

### `crates/transferred-py/src/transfer.rs` — additions

Imports:

```rust
use std::sync::Arc;
use arrow::array::TimestampMicrosecondArray;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use transferred_core::{Destination, Load, Source, Transfer, WatermarkValue};
```

`PyTransfer` gains `load: Option<Load>`; `new` signature and `run` wiring:

```rust
    #[new]
    #[pyo3(signature = (source, destination, load=None, deletes_since_micros=None))]
    fn new(
        source: &Bound<'_, PyAny>,
        destination: &Bound<'_, PyAny>,
        load: Option<&str>,
        deletes_since_micros: Option<i64>,
    ) -> PyResult<Self> {
        let source = extract_source(source)?;
        let destination = extract_destination(destination)?;
        let load = build_load(load, deletes_since_micros)?;
        Ok(Self {
            source: Some(source),
            destination: Some(destination),
            load,
        })
    }

    // inside run(), after taking source/destination:
    //   let load = self.load.take();
    //   let mut transfer = Transfer::new(source, destination);
    //   if let Some(load) = load {
    //       transfer = transfer.with_load(load);
    //   }
```

Helpers:

```rust
/// Build a core `Load` from the decomposed Python representation.
fn build_load(load: Option<&str>, deletes_since_micros: Option<i64>) -> PyResult<Option<Load>> {
    match load {
        None => Ok(None),
        Some("full") => Ok(Some(Load::Full)),
        Some("incremental") => Ok(Some(Load::Incremental {
            deletes_since: deletes_since_micros.map(watermark_from_micros),
        })),
        Some(other) => Err(PyValueError::new_err(format!(
            "unknown load kind {other:?}; expected \"full\" or \"incremental\""
        ))),
    }
}

/// A UTC microsecond-precision timestamp scalar (length-1 Arrow array) for the
/// delete-window cutoff. Connectors cast to the tracking column's own type.
fn watermark_from_micros(micros: i64) -> WatermarkValue {
    Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"))
}
```

`_native/__init__.pyi`: `_Transfer.__new__` gained
`load: typing.Optional[builtins.str] = None, deletes_since_micros:
typing.Optional[builtins.int] = None`.

### `crates/transferred-py/python/transferred/load.py` — full file (new)

```python
"""Load strategies for `Transfer`.

Pass one as `Transfer(..., load=...)` to force a strategy. Omitting `load`
(or passing `None`) auto-infers: incremental when both sides support it,
full refresh otherwise.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Full:
    """Full refresh — rewrite the whole destination."""


@dataclass(frozen=True)
class Incremental:
    """Incremental load — apply inserts/updates, and deletes when supported.

    Args:
        deletes_since: Lower-bound cutoff for delete reconciliation. Only rows
            whose tracking column is `>= deletes_since` are checked for
            deletion; older rows are skipped (cheaper, but their deletions are
            not propagated). `None` (default) reconciles all rows — a full
            delete sweep. Use a timezone-aware datetime; a naive datetime is
            interpreted in the system local timezone.
    """

    deletes_since: datetime | None = None


# A load strategy: an explicit `Full` / `Incremental`, or `None` to auto-infer.
Load = Full | Incremental
```

### `crates/transferred-py/python/transferred/transfer.py` — additions

```python
from transferred.load import Full, Incremental, Load

# Transfer.__new__ gains a `load: Load | None = None` param, then:
#   load_kind, deletes_since_micros = _decompose_load(load)
#   return super().__new__(cls, source, destination, load_kind, deletes_since_micros)


def _decompose_load(load: Load | None) -> tuple[str | None, int | None]:
    """Flatten a `Load` into the primitives `_Transfer.__new__` accepts:
    a kind string (`None` / `"full"` / `"incremental"`) and the delete-window
    cutoff as microseconds since the Unix epoch (UTC)."""
    if load is None:
        return None, None
    if isinstance(load, Full):
        return "full", None
    if isinstance(load, Incremental):
        deletes_since_micros = None
        if load.deletes_since is not None:
            deletes_since_micros = round(load.deletes_since.timestamp() * 1_000_000)
        return "incremental", deletes_since_micros
    raise TypeError(
        f"load must be Full, Incremental, or None, got {type(load).__name__!r}"
    )
```

### `crates/transferred-py/python/transferred/__init__.py` — additions

```python
from transferred.load import Full, Incremental
# __all__ gains "Full", "Incremental"
```
