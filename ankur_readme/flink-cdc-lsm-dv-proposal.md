# Proposal: Paimon-Style Bucketed LSM Writer for Flink CDC into Iceberg v3

Status: design sketch for discussion, not implemented. As of 2026-10-10.
Diagram: [`flink-cdc-lsm-dv-design.mmd`](flink-cdc-lsm-dv-design.mmd). Background:
[`deletion-vectors-and-flink-upsert.md`](deletion-vectors-and-flink-upsert.md).

**Proposal in one paragraph:** Give every bucket of the table exactly one Flink writer. That writer
owns the bucket's primary-key index, the current deletion vector (DV) of every file in the bucket,
and the bucket's LSM levels. Because it owns all three, it can always resolve an update to an exact
`(file, position)` and write a **DV instead of an equality delete**. It also writes **no delete at
all for genuinely new keys**, and compacts its own files to keep DV counts bounded. The result is a
plain Iceberg v3 table: data files plus at most one DV each, **zero equality deletes**, readable by
any v3 engine.

---

## 1. Problem

| Today (upsert, measured in this repo) | Consequence at 100B rows |
| --- | --- |
| Every `+I`/`+U` calls `deleteKey`, giving one equality-delete row per input row unless the key was already written this checkpoint | Billions of equality-delete rows |
| One equality-delete file per subtask × partition × checkpoint (default `keyBy(pk)`) | Millions of tiny delete files per day |
| v3 DVs are used only for duplicates *within* a checkpoint (`PartitioningDVWriter` built with `p -> null`) | v3 changes nothing for cross-checkpoint updates |
| `ConvertEqualityDeletes` fixes this after the fact (staging branch → PK index → DVs) | Readers pay until it runs; it's a second pipeline holding the same index |

The root cause: the writer doesn't know **where** an older row lives, so it can only say "delete
whatever has this key".

## 2. What Paimon does differently

Paimon primary-key tables split data into a **fixed number of buckets** (`bucket = hash(pk) mod B`),
and each bucket has **one writer**. Each bucket is a small **LSM tree**:

- Writes go to a sorted in-memory buffer, then flush as sorted runs (level 0).
- Background compaction merges runs into higher levels, deduplicating by key through a merge engine:
  deduplicate, partial-update or aggregate.
- With `deletion-vectors.enabled`, the writer looks up older levels for each key and emits a DV for
  the old row's position. Readers then skip rows instead of merge-sorting runs.

The idea worth borrowing: **a single owner per key range makes position-level deletes cheap and
safe.** The Paimon file format is not needed. Everything below uses ordinary Iceberg data files, DVs
and commits.

## 3. Proposed design (numbers match the diagram)

### 3.1 Bucket assignment (steps 1–2)

- The table spec includes `bucket(B, pk)`, plus an optional time partition such as `day(ts)`.
  B is fixed, for example 1024.
- A new `BucketPartitioner`, used with `partitionCustom` in `IcebergSink.addPreWriteTopology`, sends
  bucket b to subtask `b mod N`. **Ownership is stable**: one bucket, one writer, for the job's
  lifetime. This replaces `keyBy(EqualityFieldKeySelector)`, which hashes keys across all subtasks.

### 3.2 The LSM writer (step 3)

A new `TaskWriter`, say `BucketedLsmDeltaWriter`, replaces `BaseDeltaTaskWriter` for these tables:

1. **Sort buffer per bucket.** Rows are de-duplicated by primary key in memory (last write wins, or a
   merge engine). A key updated 10 times in one checkpoint becomes one row: no position deletes and
   no extra data rows.
2. **PK index lookup** for each surviving key:

   | Lookup result | Action | Delete written |
   | --- | --- | --- |
   | Not found | Plain insert | **None** (today: an equality-delete row) |
   | In committed file F at position p | Set bit p in the owner's in-memory `DV(F)` | Part of one merged DV per touched file |
   | In this checkpoint's buffer | Replaced in memory | None |
   | `-D` for a found key | Set bit p in `DV(F)`; remove from index | Part of a DV |

3. **Flush at the checkpoint:** one L0 data file per touched bucket, **sorted by pk**, with a Parquet
   bloom filter on pk (`write.parquet.bloom-filter-enabled.column.<pk>=true`).

### 3.3 Writer state (checkpointed with Flink)

| State | Contents | Notes |
| --- | --- | --- |
| PK index | **Option A:** RocksDB `pk → (fileId, pos)`. **Option B:** no index; look up the sorted levels (key range → bloom filter → page index), like Paimon's lookup | A: about 20–40 B per key, O(1) lookups. B: little state, more I/O. See section 5. |
| DV bitmaps | Current `RoaringPositionBitmap` per owned file | The owner already has the previous DV, so there's no `loadPreviousDeletes` read from the table |
| Level manifest | file → level, key range, live-row ratio | Drives compaction |

Restore: the state is aligned with the committed snapshot through
`flink.max-committed-checkpoint-id`, as today. If state is lost, rebuild one bucket by reading only
the pk column of its files and applying their DVs.

### 3.4 In-writer LSM compaction (step 4)

Runs asynchronously per bucket, inside the owner, so **no lock and no index invalidation** is needed.

- **Triggers:** more than 4 L0 files; a file's live ratio below 70%; size amplification above 2×.
- **Merge:** merge-sort the chosen runs by pk, drop rows whose DV bit is set, and write new L1/L2
  files with non-overlapping key ranges.
- **Re-index:** update index entries for keys that moved (Option A), and drop the DVs of rewritten
  files. With Option B nothing changes, because the lookup reads the new levels directly.

This keeps DVs small: a DV exists only on files that haven't been compacted yet, and compaction
resets its cardinality to 0.

### 3.5 Commit (step 5): extend `IcebergCommitter`

Per checkpoint:

```java
table.newRowDelta()
    .addRows(l0File)                        // for each new L0 file
    .addDeletes(mergedDv)                   // new DV for every touched file
    .removeDeletes(previousDv)              // the DV it replaces (API exists: RowDelta.removeDeletes)
    .validateFromSnapshot(lastCommittedSnapshotId)
    .conflictDetectionFilter(bucketIn(ownedBuckets))
    .validateNoConflictingDeleteFiles()
    .commit();
```

When a compaction finishes, it commits through `RewriteFiles` (`deleteFile` the old data and DVs,
`addFile` the new files), ordered after that checkpoint's `RowDelta`. Equality deletes are never
committed.

### 3.6 What readers see (steps 6–7)

Data files, each with at most one DV, and no equality deletes. Spark, Trino and Flink need no changes
and do no merge-on-read sorting, unlike Paimon without DV mode.

## 4. Comparison

| | Today (upsert) | `ConvertEqualityDeletes` (in repo) | **Proposed LSM writer** | Paimon PK table |
| --- | --- | --- | --- | --- |
| Equality deletes written | ~1 row per input row | Written, then converted | **0** | n/a |
| Delete for a brand-new key | Yes | Yes, then dropped | **No** | No |
| Delete files per checkpoint | ≈ N × P | ≈ N × P, then converted | ≤ 1 Puffin per writer | n/a |
| DVs per data file | ≤ 1 (in-checkpoint only) | ≤ 1 | ≤ 1, reset by compaction | ≤ 1 |
| Time until readers stop paying | Until compaction | Until the conversion runs | **Immediately** | Immediately (DV mode) |
| Extra pipeline | None | Separate job with its own index | None (index lives in the writer) | None |
| Open-format readers | Yes | Yes | **Yes** | Paimon readers only |
| Rescaling | Free | Free | Fixed B (as in Paimon) | Fixed or dynamic buckets |

## 5. Index choice for 100B keys

| | Option A: RocksDB index | Option B: LSM lookup (Paimon style) |
| --- | --- | --- |
| State size | ~30 B per key → ~3 TB per 100B keys across the job (~47 GB per subtask at N = 64) | Level manifests only (MB) |
| Lookup cost | One RocksDB `get` | Per level: one file by key range, bloom check, one page read. **Batch it**: sort the checkpoint's keys and merge-join them against each level. |
| After compaction | Rewrite index entries for moved keys | Nothing |
| Best for | Up to a few billion keys per job, low latency | 10B+ keys, update-heavy streams with locality |

**Recommendation:** start with Option A to prove correctness (reuse `DVPosition` and the generation
handling from `EqualityConvertPKIndex`), then add Option B for the 100B-row table.

## 6. Where it plugs into this codebase

| Change | Where |
| --- | --- |
| `BucketPartitioner` + bucket ownership | `flink/v2.3/.../sink/IcebergSink.java` (`addPreWriteTopology`, beside `distributeDataStream`) |
| `BucketedLsmDeltaWriter` (buffer, lookup, DV bitmaps, L0 flush) | New class beside `BaseDeltaTaskWriter`, chosen in `RowDataTaskWriterFactory.create()` |
| DV writing with the owner's previous bitmaps | Reuse `core/.../deletes/BaseDVFileWriter`, passing `loadPreviousDeletes = state::bitmapFor` |
| PK index state | New keyed or operator state; borrow from `maintenance/operator/EqualityConvertPKIndex` |
| `removeDeletes` + `RewriteFiles` in the commit | `sink/IcebergCommitter.commitDeltaTxn`; `WriteResult` needs a "rewritten delete files" field (`DeleteWriteResult` already has `rewrittenDeleteFiles`) |
| Opt-in switch | New write option, for example `write.upsert.mode=lsm-dv` |

## 7. Risks and open questions

1. **Other writers.** A Spark `MERGE` or external compaction on the same buckets would invalidate the
   owner's index and DVs. Either reserve those buckets (with commit validation as the guard) or
   rebuild the bucket index on conflict, as `EqualityConvertPKIndex` does with generations.
2. **Fixed bucket count.** Changing B means rewriting the table, the same constraint as Paimon fixed
   buckets. Dynamic buckets would need a key → bucket index.
3. **Checkpoint latency.** Lookups at the barrier must stay small. Do Option A lookups per row as data
   arrives, and do only the flush at the barrier.
4. **Skew.** One hot bucket ties up one subtask. Size B at roughly 4–16× N.
5. **Spec fit.** LSM levels are writer-private bookkeeping. Iceberg sees only normal files, so no spec
   change is needed.

## 8. Suggested phases

| Phase | Scope | Win |
| --- | --- | --- |
| 1 | Bucket ownership + RocksDB index + DVs instead of equality deletes; keep the existing `RewriteDataFiles` | Equality deletes drop to 0; no deletes for new keys |
| 2 | In-writer LSM compaction with levels | DV counts and data file counts bounded without outside maintenance |
| 3 | Option B lookup (sorted levels + bloom filters) | State fits 100B+ keys |

Prototype check: rerun the file-count experiment from `deletion-vectors-and-flink-upsert.md` §2.2
against the new writer. Equality-delete files and rows should be 0, and checkpoint 1 (new keys) should
produce 0 deletes.
