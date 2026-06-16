"""The merge core: turn an unordered, at-least-once CDC stream into correct state.

Kafka gives you *at-least-once* delivery in *arrival* order. Neither matches what
a correct materialization needs:

- **Out of order.** Two updates to the same key can arrive in the wrong order.
  Applying them by arrival order leaves the stale value. The fix is
  last-writer-wins keyed on the source **LSN**, not arrival: an event is applied
  only if its LSN is strictly greater than the highest LSN already seen for that
  key. A late (lower-LSN) event is dropped.

- **Duplicates.** The same event can be delivered twice. Because "apply only if
  LSN strictly greater" rejects an equal LSN, re-delivering an event is a no-op.
  That makes the whole apply path **idempotent** -- reprocessing a batch (or the
  entire log) converges to the identical state, which is what "exactly-once"
  means for a materialization: effects are applied once regardless of delivery.

- **Deletes that must not resurrect.** A delete is a tombstone carrying its own
  LSN. A later-arriving but older-LSN update for that key is correctly ignored,
  so a deleted row never comes back.

The logic is deliberately engine-agnostic Python so it can be unit-tested
deterministically (see tests) and reused verbatim by both the local pipeline and
the Spark ``foreachBatch`` job, rather than trusting an untested SQL MERGE.
"""

from __future__ import annotations

from dataclasses import dataclass

from .events import CDCEvent

# Merge modes. "lsn" is correct; "arrival" is the naive last-arrival-wins
# baseline kept so the reconciler can *demonstrate* the drift it causes.
MODE_LSN = "lsn"
MODE_ARRIVAL = "arrival"


@dataclass
class KeyState:
    lsn: int
    deleted: bool
    row: dict | None


@dataclass
class MergeStats:
    seen: int = 0
    applied: int = 0
    ignored_stale: int = 0     # older-or-equal LSN (out-of-order or duplicate)


class MergeCore:
    """Materialized key -> latest state, resolved by source LSN."""

    def __init__(self, mode: str = MODE_LSN):
        if mode not in (MODE_LSN, MODE_ARRIVAL):
            raise ValueError(f"unknown merge mode {mode!r}")
        self.mode = mode
        self.state: dict[object, KeyState] = {}
        self.stats = MergeStats()

    def apply(self, ev: CDCEvent) -> bool:
        """Apply one event. Return True if it changed state."""
        self.stats.seen += 1
        cur = self.state.get(ev.key)
        if self.mode == MODE_LSN and cur is not None and ev.lsn <= cur.lsn:
            self.stats.ignored_stale += 1        # stale/duplicate -> idempotent no-op
            return False
        if ev.is_delete:
            self.state[ev.key] = KeyState(ev.lsn, True, None)
        else:
            self.state[ev.key] = KeyState(ev.lsn, False, ev.after)
        self.stats.applied += 1
        return True

    def apply_batch(self, events: list[CDCEvent]) -> set:
        """Apply a micro-batch; return the set of keys whose state changed."""
        changed = set()
        for ev in events:
            if self.apply(ev):
                changed.add(ev.key)
        return changed

    def changes_for(self, keys) -> tuple[list[dict], list]:
        """Split changed keys into (rows to upsert, keys to delete) for the sink."""
        upserts, deletes = [], []
        for k in keys:
            ks = self.state[k]
            if ks.deleted:
                deletes.append(k)
            else:
                upserts.append(ks.row)
        return upserts, deletes

    def materialize(self) -> list[dict]:
        """All live (non-deleted) rows -- the current table state."""
        return [ks.row for ks in self.state.values() if not ks.deleted]
