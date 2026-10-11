# Flink Ingestion and Upsert/CDC into Iceberg — How It Works

Covers the Flink 2.3 module (`flink/v2.3/flink`) on this branch, as of 2026-10-10.
Architecture diagram: [`flink-upsert-ingestion-architecture.mmd`](flink-upsert-ingestion-architecture.mmd).
Paths below are relative to `flink/v2.3/flink/src/main/java/org/apache/iceberg/flink/` unless they
start with `core/`.

**In one paragraph:** Flink rows carry a `RowKind` (+I, -U, +U, -D). The sink partitions rows on
the primary key so each key always reaches the same writer. The writer turns every row into data-file
rows plus deletes. If the key was already written **in the current checkpoint**, the delete is a
**position delete**: a V2 pos-delete file, or a deletion vector in V3. Otherwise the delete is an
**equality delete** on the key columns. On each checkpoint, files are gathered into manifests and
committed as one Iceberg `RowDelta` snapshot per checkpoint.

---

## 1. Entry points

| Path | Class | When used |
| --- | --- | --- |
| Flink SQL / Table API | `IcebergTableSink` | `INSERT INTO iceberg_table ...`; table `PRIMARY KEY` becomes equality fields |
| DataStream, Sink V2 | `sink/IcebergSink` | `IcebergSink.forRowData(stream)`; SQL uses it when `table.exec.iceberg.use-v2-sink=true` (`IcebergTableSink.java:162`) |
| DataStream, legacy | `sink/FlinkSink` | `FlinkSink.forRowData(stream)`; SQL default (`use-v2-sink` defaults to `false`) |
| Multi-table | `sink/dynamic/DynamicIcebergSink` | One job writing many tables or schemas |

`IcebergSink` and `FlinkSink` share the same writer core (`RowDataTaskWriterFactory` →
`BaseDeltaTaskWriter` → `BaseEqualityDeltaWriter`). They differ only in their operator wiring
(section 6).

DataStream example:

```java
IcebergSink.forRowData(cdcStream)
    .tableLoader(tableLoader)
    .equalityFieldColumns(List.of("id"))   // primary key
    .upsert(true)                          // optional: upsert semantics
    .append();
```

SQL example:

```sql
CREATE TABLE orders (
  id BIGINT, status STRING, dt DATE,
  PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt)
WITH ('format-version'='2', 'write.upsert.enabled'='true');

INSERT INTO orders SELECT * FROM mysql_cdc_orders;
```

## 2. Configuration that changes behavior

| Setting | Where | Effect |
| --- | --- | --- |
| Equality fields | `PRIMARY KEY` (SQL) or `.equalityFieldColumns()` | Turns on the delta writer. Without them the sink is append-only. |
| `write.upsert.enabled` (table) / `upsert-enabled` (write option) / `.upsert(true)` | `FlinkWriteConf.upsertMode()` | Upsert semantics (section 4). Requires equality fields and no overwrite mode (`IcebergSink.java:780`). |
| `format-version` | Table property | `2` → position-delete files. `3` → deletion vectors (`useDv = formatVersion > 2`, `RowDataTaskWriterFactory.java:175`). |
| `write.distribution-mode` | Table / write option | `none`, `hash` or `range`. With equality fields, every mode still keys by primary key or partition (section 3). |
| `table.exec.iceberg.use-v2-sink` | Flink config | SQL uses `IcebergSink` instead of `FlinkSink`. |

## 3. Distribution: same key → same writer

`IcebergSink.distributeDataStream` (`IcebergSink.java:969`):

| Mode | No equality fields | Equality fields set |
| --- | --- | --- |
| `none` | No shuffle | `keyBy(EqualityFieldKeySelector)` |
| `hash` | `keyBy(PartitionKeySelector)` (or none if unpartitioned) | Unpartitioned: key by equality fields. Partitioned: key by partition, and every partition source column must be in the primary key. |
| `range` | Range-shuffle by sort order / statistics | Falls back to `keyBy(EqualityFieldKeySelector)` |

**Why it matters:** each writer subtask keeps its own in-memory map of keys written this
checkpoint. If two versions of one key went to different subtasks, both would append a row and
neither would see the other. Duplicates would then survive until the next checkpoint's equality
delete.

## 4. The per-row logic

### 4.1 Writer selection — `RowDataTaskWriterFactory.create()` (`sink/RowDataTaskWriterFactory.java:186`)

- No equality fields: `UnpartitionedWriter` or `RowDataPartitionedFanoutWriter`. These only
  append data files.
- Equality fields: `UnpartitionedDeltaWriter` or `PartitionedDeltaWriter`. Both extend
  `BaseDeltaTaskWriter`. The partitioned one keeps one `RowDataDeltaWriter` per partition key
  (`PartitionedDeltaWriter.java:71`).
- In upsert mode the equality-delete file schema is **only the key columns**. An upsert stream
  has no `-U` before-image, so the other column values of the old row are unknown.

### 4.2 RowKind switch — `BaseDeltaTaskWriter.write()` (`sink/BaseDeltaTaskWriter.java:81`)

| RowKind | Upsert mode | CDC (changelog) mode |
| --- | --- | --- |
| `+I` INSERT | `deleteKey(key)`, then `write(row)` | `write(row)` |
| `+U` UPDATE_AFTER | `deleteKey(key)`, then `write(row)` | `write(row)` |
| `-U` UPDATE_BEFORE | ignored (the `+U` already deleted the key) | `delete(row)` (full row) |
| `-D` DELETE | `deleteKey(key)` | `delete(row)` (full row) |

Upsert mode issues a delete-by-key on **every** insert. That makes it safe when the source does
not know whether a key exists, but it emits a delete for every row. This is the main source of
delete-file growth at scale.

### 4.3 Delete routing — `BaseEqualityDeltaWriter` (`core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java:179`)

State: `insertedRowMap` maps each key to `(data file path, row offset)` for rows written by this
writer **since the last checkpoint**. It is cleared on `close()` at each checkpoint.

```
write(row):                         (BaseTaskWriter.java:228)
    previous = insertedRowMap.put(key, currentPath+offset)
    if previous != null: writePosDelete(previous)    # same key twice in one checkpoint
    dataWriter.write(row)

delete(row) / deleteKey(key):       (BaseTaskWriter.java:291 / :303)
    previous = insertedRowMap.remove(key)
    if previous != null: writePosDelete(previous)    # cheap: exact file + offset known
    else:                eqDeleteWriter.write(row|key)  # key lives in an older snapshot
```

Where the position delete goes:

| Format version | Position delete written as |
| --- | --- |
| V2 | Position-delete file via `WrappedPositionDeleteWriter` (`DeleteGranularity.FILE` from Flink) |
| V3 | Deletion vector via `PartitioningDVWriter` (Puffin), created in `BaseTaskWriter.java:115` |

Equality deletes are written the same way in V2 and V3. Deletion vectors **only replace position
deletes for keys rewritten within one checkpoint**. Deletes of keys from earlier checkpoints are
still equality deletes. `PartitioningDVWriter` is built with `p -> null` as the "previous deletes"
loader, so it does not merge with DVs already in the table.

### 4.4 Worked example (upsert, key = `id`, one checkpoint, empty table at start)

| # | Input | Action | Files produced |
| --- | --- | --- | --- |
| 1 | `+I (1,'a')` | `deleteKey(1)`: not in map → eq-delete `{1}`; write row at offset 0 | data: `(1,a)`; eq-delete: `{1}` |
| 2 | `+I (1,'b')` | `deleteKey(1)`: in map → pos-delete offset 0; write row at offset 1 | data: `(1,b)`; pos-delete/DV: offset 0 |
| 3 | `-D (1)` | `deleteKey(1)`: in map → pos-delete offset 1 | pos-delete/DV: offset 1 |

Committed as one snapshot. Readers see no row for id 1. The eq-delete `{1}` from step 1 does not
hide rows written in the same commit, because equality deletes apply only to data files with a
**lower** sequence number. That is why in-checkpoint duplicates must use position deletes.

## 5. Checkpoint → files → commit

1. **Barrier.** `IcebergSinkWriter.prepareCommit()` (`sink/IcebergSinkWriter.java:99`) calls
   `writer.complete()`. It closes all files and returns a `WriteResult`: data files, delete files
   and referenced data files.
2. **Aggregate.** `IcebergWriteAggregator.prepareSnapshotPreBarrier()`
   (`sink/IcebergWriteAggregator.java:98`) runs with parallelism 1. It collects all subtasks'
   results and writes temporary manifests for that checkpoint. It emits one `IcebergCommittable`.
3. **Commit.** `IcebergCommitter.commit()` (`sink/IcebergCommitter.java:126`) runs after Flink
   confirms the checkpoint. `commitDeltaTxn` (`:265`) does the following:
   - No delete files → a single `AppendFiles`.
   - Otherwise → **one `RowDelta` per pending checkpoint, in order**. They are never merged,
     because checkpoint N+1's equality deletes must get a higher sequence number than checkpoint
     N's data.
   - No conflict validation is needed. Equality deletes apply to all older data, and position
     deletes only target files added in the same commit.
4. **Exactly-once.** Each snapshot is tagged with `flink.max-committed-checkpoint-id`,
   `flink.job-id` and `flink.operator-id`. After a restart, the committer skips checkpoints already
   in the table (`SinkUtil.getMaxCommittedCheckpointId`).

The legacy `FlinkSink` does the same work with `IcebergStreamWriter` →
`IcebergFilesCommitter`, where one operator at parallelism 1 both aggregates and commits.

## 6. Operator topology

| Sink | Operators |
| --- | --- |
| `IcebergSink` (Sink V2) | `keyBy` → `IcebergSinkWriter` (N) → `IcebergWriteAggregator` (1) → `IcebergCommitter` (1) |
| `FlinkSink` (legacy) | `keyBy` → `IcebergStreamWriter` (N) → `IcebergFilesCommitter` (1) |

Optional post-commit: `IcebergSink` can attach `TableMaintenance` tasks to the same job.

## 7. Read-side cost and maintenance

- Each equality-delete file applies to **every older data file in its partition**, so readers must
  load and apply it. In upsert mode one is written each checkpoint per writer and partition, so at
  high throughput they pile up quickly.
- Fix them with Flink maintenance (`maintenance/api/`):
  - `ConvertEqualityDeletes`: turns equality deletes into position deletes or DVs.
  - `RewriteDataFiles`: compacts data files and applies deletes.
  - `ExpireSnapshots`, `DeleteOrphanFiles`: clean up old snapshots and files.
- Deeper discussion: Parts 4–5 of `iceberg-build-and-actions-guide.md`.

## 8. Code map

| Concern | File |
| --- | --- |
| SQL entry, PRIMARY KEY → equality fields | `IcebergTableSink.java` |
| Upsert / CDC options | `FlinkWriteOptions.java`, `FlinkWriteConf.java` |
| Sink builder, validation, distribution | `sink/IcebergSink.java` |
| Writer selection, `useDv` | `sink/RowDataTaskWriterFactory.java` |
| RowKind switch | `sink/BaseDeltaTaskWriter.java` |
| Partition routing | `sink/PartitionedDeltaWriter.java`, `sink/UnpartitionedDeltaWriter.java` |
| Pos-vs-equality delete decision | `core/.../io/BaseTaskWriter.java` (`BaseEqualityDeltaWriter`) |
| Deletion vector writer | `core/.../io/PartitioningDVWriter.java` |
| Checkpoint flush | `sink/IcebergSinkWriter.java` |
| Manifest aggregation | `sink/IcebergWriteAggregator.java` |
| Commit, exactly-once | `sink/IcebergCommitter.java`, `sink/SinkUtil.java` |
| Legacy sink | `sink/FlinkSink.java`, `IcebergStreamWriter.java`, `IcebergFilesCommitter.java` |

## 9. How to test

Run from the repo root. Flink 2.3 is the default version, so no `-DflinkVersions` flag is needed.

| What | Test class | Notes |
| --- | --- | --- |
| SQL upsert end to end | `org.apache.iceberg.flink.TestFlinkUpsert` | Parquet/Avro/ORC × format V2+ × streaming/batch |
| SQL changelog (CDC) | `org.apache.iceberg.flink.TestChangeLogTable` | +I/-U/+U/-D through SQL |
| Sink V2 upsert and CDC | `org.apache.iceberg.flink.sink.TestIcebergSinkV2` | `testUpsertOnIdKey`, `testChangeLogOnIdKey`, `testDeleteStats`, ... |
| Legacy sink upsert and CDC | `org.apache.iceberg.flink.sink.TestFlinkIcebergSinkV2` | Shared cases in `TestFlinkIcebergSinkV2Base` |
| Commit and recovery | `org.apache.iceberg.flink.sink.TestIcebergFilesCommitter` | Checkpoint ordering, restart |
| Equality-delete conversion | `org.apache.iceberg.flink.maintenance.api.TestConvertEqualityDeletes` | Maintenance path |
| Core delta writer | `org.apache.iceberg.io.TestTaskEqualityDeltaWriter` (in `:iceberg-data`) | Pos-vs-eq delete decision |

```bash
# One upsert test method (verified: 18/18 parameter combinations pass, ~35s)
./gradlew :iceberg-flink:iceberg-flink-2.3:test \
  --tests "org.apache.iceberg.flink.TestFlinkUpsert.testUpsertAndQuery"

# All upsert + CDC sink tests
./gradlew :iceberg-flink:iceberg-flink-2.3:test \
  --tests "org.apache.iceberg.flink.TestFlinkUpsert" \
  --tests "org.apache.iceberg.flink.TestChangeLogTable" \
  --tests "org.apache.iceberg.flink.sink.TestIcebergSinkV2" \
  --tests "org.apache.iceberg.flink.sink.TestFlinkIcebergSinkV2"

# Core delta writer
./gradlew :iceberg-data:test --tests "org.apache.iceberg.io.TestTaskEqualityDeltaWriter"

# Results
open flink/v2.3/flink/build/reports/tests/test/index.html
```

To watch which delete files a change produces, add a test to `TestIcebergSinkV2` modeled on
`testDeleteStats`. Then inspect `table.currentSnapshot().summary()`: `added-equality-delete-files`,
`added-position-delete-files` and `added-dvs`.
