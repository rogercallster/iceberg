# How Apache Iceberg Works, and How Flink CDC Writes Into It

As of 2026-10-10. Code references point to this repo, using the Flink 2.3 module (`flink/v2.3/flink`).

| Diagram | File | Type |
| --- | --- | --- |
| Iceberg architecture (horizontal) | [`iceberg-architecture.mmd`](iceberg-architecture.mmd) | Flowchart, left to right |
| Flink CDC end-to-end flow | [`flink-iceberg-cdc-flow.mmd`](flink-iceberg-cdc-flow.mmd) | Sequence diagram |
| Flink CDC classes | [`flink-iceberg-cdc-class-diagram.mmd`](flink-iceberg-cdc-class-diagram.mmd) | Class diagram |
| Flink sink topology (simpler view) | [`flink-upsert-ingestion-architecture.mmd`](flink-upsert-ingestion-architecture.mmd) | Flowchart |

To view a diagram: open the `.mmd` file in IntelliJ or VS Code with a Mermaid plugin, paste it into
<https://mermaid.live>, or render it locally:

```bash
npx -y @mermaid-js/mermaid-cli@11.4.2 -i iceberg-architecture.mmd -o iceberg-architecture.png -w 2600
```

---

# Part 1 — How Iceberg works

**In one paragraph:** An Iceberg table is a tree of **immutable files** plus **one mutable pointer**.
Data lives in Parquet, ORC or Avro files. A layer of metadata files records which data files make up
each version (snapshot) of the table. The catalog stores a single pointer to the current
`metadata.json`. Every write creates new files and then atomically swaps that pointer. That swap is
what gives Iceberg ACID commits, snapshot isolation, time travel and safe concurrent writers on plain
object storage.

## 1.1 The layers (left to right in `iceberg-architecture.mmd`)

| Layer | What it is | Key classes |
| --- | --- | --- |
| Engines and API | Spark, Flink, Trino and others call the Iceberg Java API | `Table`, `TableScan`, `AppendFiles`, `RowDelta`, `Transaction` (`api/`) |
| Catalog | Maps `db.table` to the current `metadata.json` location. The **only mutable state.** | `Catalog`, `BaseMetastoreCatalog`, `TableOperations`; REST, Hive, Glue, JDBC, Nessie, Hadoop |
| Metadata | Immutable tree: `metadata.json` → snapshot → manifest list → manifests | `TableMetadata`, `Snapshot`, `ManifestFile`, `ManifestReader/Writer` (`core/`) |
| Data | Immutable data files and delete files | `DataFile`, `DeleteFile` (`ContentFile`) |
| Storage | Pluggable file access | `FileIO`: `S3FileIO`, `GCSFileIO`, `ADLSFileIO`, `HadoopFileIO` |

## 1.2 The metadata tree

```
catalog pointer ──► v42.metadata.json        (TableMetadata: schemas, specs, sort orders,
                        │                     properties, snapshot log, refs, current-snapshot-id)
                        ▼
                    snapshot 9876             (id, parent, sequence-number, operation, summary)
                        │
                        ▼
                    snap-9876-....avro        (manifest list: one row per manifest +
                        │                      partition lower/upper summaries)
              ┌─────────┴──────────┐
              ▼                    ▼
       data manifest         delete manifest   (one row per file: status, data sequence number,
              │                    │            partition tuple, column stats)
              ▼                    ▼
        data files       position deletes / DVs / equality deletes
```

- **Snapshot**: a complete list of the table's files at one point in time. Old snapshots stay
  readable until expired, which is how time travel works.
- **Manifest list**: lets a reader skip whole manifests using partition summaries.
- **Manifest**: lets a reader skip individual files using per-column lower/upper bounds and null
  counts.
- **Refs**: `main` plus named branches and tags. Each ref points to a snapshot.

## 1.3 Row-level deletes and sequence numbers

Iceberg never rewrites a data file in place. To delete or update rows it adds **delete files**:

| Delete type | Contents | Applies to | Format version |
| --- | --- | --- | --- |
| Position delete file | `(file_path, pos)` pairs | Rows at those positions | v2 |
| Deletion vector (DV) | Roaring bitmap in a Puffin file, **one per data file** | Rows whose bit is set | v3 (replaces position delete files) |
| Equality delete file | Values of key columns (`equality_ids`) | Any row with matching key values | v2+ |

Each commit gets an increasing **sequence number**. Deletes respect it:

- An **equality delete** with sequence number N applies only to data files with sequence number
  **lower than N**. It deletes older versions and never hides rows added in the same commit.
- A **position delete or DV** applies to data files with sequence number **≤ N**, which includes
  files added in the same commit.

This rule is why Flink CDC needs both kinds of delete (Part 2).

## 1.4 Write path: optimistic concurrency

1. Write the new data and delete files.
2. Write new manifests, then a new manifest list and snapshot.
3. Write a new `metadata.json` that points to the new snapshot.
4. Ask the catalog to swap the pointer from the old metadata file to the new one
   (`TableOperations.commit(base, new)`), as a compare-and-swap.
5. If another writer won the race (`CommitFailedException`), `SnapshotProducer.commit()` refreshes,
   re-applies the change on the new base, checks for conflicts, and retries.

Operations: `AppendFiles` (`FastAppend`, `MergeAppend`), `RowDelta` (`BaseRowDelta`: add rows plus
deletes), `OverwriteFiles`, `RewriteFiles` (compaction), `ExpireSnapshots`.

## 1.5 Read path: scan planning

1. Load the current `metadata.json` (or the one for a time-travel snapshot).
2. Read the manifest list and drop manifests whose partition range can't match the filter.
3. Read the remaining manifests and drop files whose column stats can't match.
4. Attach each data file's applicable delete files, filtered by sequence number and partition.
5. Emit a `FileScanTask` per file split. The engine reads the data and applies the deletes.

**Cost to watch:** every equality delete file must be checked against every older data file in its
partition. Many equality deletes make reads slow until maintenance compacts them away.

## 1.6 Maintenance

| Action | Purpose |
| --- | --- |
| `rewrite_data_files` | Compact small files and apply deletes into new data files |
| `rewrite_position_delete_files` | Compact position deletes |
| `rewrite_manifests` | Re-cluster manifests for faster planning |
| `expire_snapshots` | Drop old snapshots and the files only they reference |
| `remove_orphan_files` | Delete files no snapshot references (for example, from failed writes) |
| Flink `ConvertEqualityDeletes` | Convert equality deletes into position deletes or DVs (`flink/.../maintenance/api/`) |

---

# Part 2 — How Flink CDC writes into Iceberg (`flink-iceberg-cdc-flow.mmd`)

**In one paragraph:** Each Flink row carries a `RowKind`: +I insert, -U update-before,
+U update-after, -D delete. The sink keys the stream by primary key so each key always lands on the
same writer. Each writer turns rows into data rows plus deletes, using a **position delete** when it
wrote that key earlier in the **same checkpoint** and an **equality delete** otherwise. At each
checkpoint the files are collected into manifests. After the checkpoint completes, they are
committed as one `RowDelta` snapshot per checkpoint, tagged with the checkpoint id for
exactly-once.

## 2.1 Phase A — per-row write (sequence steps 1–25)

1. **Entry.** For SQL, `IcebergTableSink` turns the table's `PRIMARY KEY` into equality fields and
   accepts every `RowKind`. In the DataStream API, `IcebergSink.forRowData(stream)` takes
   `.equalityFieldColumns(...)` and optionally `.upsert(true)`.
2. **Distribution.** `IcebergSink.distributeDataStream` applies `keyBy(EqualityFieldKeySelector)`,
   or `PartitionKeySelector` in `hash` mode when every partition column is in the key.
3. **Writer selection.** `RowDataTaskWriterFactory.create()` picks `PartitionedDeltaWriter` or
   `UnpartitionedDeltaWriter` when equality fields are set. `useDv = formatVersion > 2`.
4. **RowKind switch.** `BaseDeltaTaskWriter.write()`:

   | RowKind | Upsert mode | CDC mode |
   | --- | --- | --- |
   | +I, +U | `deleteKey(key)`, then `write(row)` | `write(row)` |
   | -U | skip | `delete(row)` |
   | -D | `deleteKey(key)` | `delete(row)` |

5. **Delete routing.** `BaseEqualityDeltaWriter` (`core/.../io/BaseTaskWriter.java`) keeps
   `insertedRowMap` (key → file and row offset) for rows written this checkpoint:
   - **Key found** → position delete for the old row. In v2 this goes to a position delete file; in
     v3, to a DV through `PartitioningDVWriter`.
   - **Key not found** → equality delete. In upsert mode only the key columns are stored.

   In the sequence diagram, `+I(1,'b')` finds key 1 written moments earlier, so it becomes a
   position delete. `-D(2)` targets a row from an older checkpoint, so it becomes an equality
   delete.

## 2.2 Phase B — checkpoint barrier (steps 26–33)

6. `IcebergSinkWriter.prepareCommit()` calls `complete()`. Every writer closes its files and
   returns a `WriteResult` (data files, delete files, referenced data files). `insertedRowMap` is
   cleared.
7. `IcebergWriteAggregator` (parallelism 1) collects the results from all subtasks. In
   `prepareSnapshotPreBarrier`, it writes **temporary** data and delete manifests (`DeltaManifests`)
   and emits one `IcebergCommittable`: manifest bytes, job id, operator id, checkpoint id.
8. Flink stores the committable in checkpoint state. Nothing is visible in the table yet.

## 2.3 Phase C — commit (steps 34–44)

9. Once checkpoint n completes, `IcebergCommitter.commit()` runs on subtask 0.
10. **Deduplication on restore.** It reads `flink.max-committed-checkpoint-id` for this job and
    operator from the table's snapshots (`SinkUtil.getMaxCommittedCheckpointId`). Any checkpoint at
    or below that value was already committed and is skipped.
11. `commitDeltaTxn`:
    - No delete files → one `AppendFiles`.
    - Delete files → **one `RowDelta` per pending checkpoint, in order**. They are never merged:
      checkpoint n+1's equality deletes need a higher sequence number than checkpoint n's data to
      delete it.
12. Each snapshot is tagged with `flink.max-committed-checkpoint-id`, `flink.job-id` and
    `flink.operator-id`. Then the normal Iceberg commit runs (Part 1.4), with CAS on the catalog and
    retry on conflict.
13. The temporary Flink manifests are deleted.
14. Optionally, `TableMaintenance` runs post-commit in the same job: `ConvertEqualityDeletes`,
    `RewriteDataFiles`, `ExpireSnapshots`, `DeleteOrphanFiles`.

## 2.4 Why this design, and where it hurts at scale

| Design choice | Reason | Cost |
| --- | --- | --- |
| `keyBy` on the primary key | `insertedRowMap` is per subtask, so all versions of a key must meet in one place | Hot keys cause skew |
| Position delete within a checkpoint | An equality delete can't hide rows from the same commit (same sequence number) | Memory: one map entry per key written this checkpoint |
| Equality delete across checkpoints | The writer doesn't know which file holds an older row | Readers must apply it to every older file in the partition |
| Upsert issues `deleteKey` on every +I | The source can't say whether the key exists | Up to one equality delete row per input row |
| One `RowDelta` per checkpoint | Keeps sequence-number ordering correct | Many small snapshots at short checkpoint intervals |
| DVs (v3) only for in-checkpoint duplicates | `PartitioningDVWriter` is built with `p -> null` and doesn't load existing DVs | Cross-checkpoint deletes remain equality deletes |

Fix the read-side cost with maintenance: `ConvertEqualityDeletes`, then `RewriteDataFiles`. Details
are in Parts 4–5 of `iceberg-build-and-actions-guide.md`.

---

# Part 3 — Class structure (`flink-iceberg-cdc-class-diagram.mmd`)

| Group | Classes | Role |
| --- | --- | --- |
| SQL entry | `FlinkDynamicTableFactory` → `IcebergTableSink` | Builds the sink from `CREATE TABLE ... WITH (...)`; PRIMARY KEY → equality fields |
| Sink V2 | `IcebergSink` (+ `Builder`), `EqualityFieldKeySelector`, `PartitionKeySelector`, `FlinkWriteConf`, `TableLoader` | Wires the topology: pre-write `keyBy`, writer, pre-commit aggregator, committer, post-commit maintenance |
| Writer operator | `IcebergSinkWriter` → `TaskWriterFactory` / `RowDataTaskWriterFactory` → `FlinkFileWriterFactory` | One per subtask; creates a fresh `TaskWriter` each checkpoint |
| Task writers (core) | `TaskWriter` ← `BaseTaskWriter` ← `UnpartitionedWriter`, `PartitionedFanoutWriter` (append) and `BaseDeltaTaskWriter` ← `Unpartitioned/PartitionedDeltaWriter` (CDC) | Turn rows into files |
| Delta logic (core) | `BaseEqualityDeltaWriter` ← `RowDataDeltaWriter`; `PartitioningDVWriter` → `BaseDVFileWriter` | `insertedRowMap`, position vs. equality delete, DVs |
| Pre-commit | `IcebergWriteAggregator`, `FlinkManifestUtil`, `DeltaManifests`, `IcebergCommittable` | `WriteResult`s → temp manifests → serialized committable |
| Commit | `IcebergCommitter`, `SinkUtil` | Exactly-once dedup, `AppendFiles` / `RowDelta`, snapshot properties |
| Iceberg API | `Table`/`BaseTable`, `SnapshotUpdate` ← `RowDelta`/`AppendFiles`, `SnapshotProducer` ← `MergingSnapshotProducer` ← `BaseRowDelta`, `FastAppend`, `TableOperations` | Snapshot creation and CAS commit with retry |
| Legacy sink | `FlinkSink` → `IcebergStreamWriter`, `IcebergFilesCommitter` | Same writer core; one operator both aggregates and commits |

Arrow key: `<|--` extends, `<|..` implements, `*--` owns, `-->` uses or creates.

## Source file map

| Class | Path |
| --- | --- |
| `IcebergTableSink`, `FlinkDynamicTableFactory`, `FlinkWriteConf`, `TableLoader` | `flink/v2.3/flink/src/main/java/org/apache/iceberg/flink/` |
| `IcebergSink`, `IcebergSinkWriter`, `IcebergWriteAggregator`, `IcebergCommitter`, `RowDataTaskWriterFactory`, `BaseDeltaTaskWriter`, `*DeltaWriter`, `FlinkManifestUtil`, `DeltaManifests`, `SinkUtil`, `FlinkSink` | `.../flink/sink/` |
| `BaseTaskWriter` (+ `BaseEqualityDeltaWriter`), `PartitioningDVWriter`, `WriteResult`, `UnpartitionedWriter`, `PartitionedFanoutWriter` | `core/src/main/java/org/apache/iceberg/io/` |
| `BaseDVFileWriter` | `core/src/main/java/org/apache/iceberg/deletes/` |
| `BaseTable`, `TableMetadata`, `SnapshotProducer`, `MergingSnapshotProducer`, `BaseRowDelta`, `FastAppend` | `core/src/main/java/org/apache/iceberg/` |
| `Table`, `RowDelta`, `AppendFiles`, `SnapshotUpdate`, `Snapshot`, `DataFile`, `DeleteFile` | `api/src/main/java/org/apache/iceberg/` |

## Try it

```bash
# Upsert end to end (passes: 18/18 parameter combinations)
./gradlew :iceberg-flink:iceberg-flink-2.3:test --tests "org.apache.iceberg.flink.TestFlinkUpsert"

# CDC changelog through SQL and through the Sink V2 API
./gradlew :iceberg-flink:iceberg-flink-2.3:test \
  --tests "org.apache.iceberg.flink.TestChangeLogTable" \
  --tests "org.apache.iceberg.flink.sink.TestIcebergSinkV2"

# The core position-vs-equality delete decision
./gradlew :iceberg-data:test --tests "org.apache.iceberg.io.TestTaskEqualityDeltaWriter"
```

More commands: `macos-build-and-run-commands.md` and `flink-upsert-ingestion-logic.md` (section 9).
