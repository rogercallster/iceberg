# Deletion Vectors, and Why Flink Upsert Floods a Table With Delete Files

As of 2026-10-10. Code references point to this repo (branch identical to `main`).

| Diagram | File |
| --- | --- |
| What a deletion vector is, and how it's stored, read and replaced | [`deletion-vector-structure.mmd`](deletion-vector-structure.mmd) |
| Why upsert produces so many delete files | [`flink-upsert-delete-fanout.mmd`](flink-upsert-delete-fanout.mmd) |

**Short answer:** Flink upsert doesn't literally write one delete *file* per row. It writes one
equality-delete **row** per input row, into one equality-delete file **per writer subtask per
partition per checkpoint**. With the default `write.distribution-mode=none`, every subtask writes to
every partition. Once subtasks × partitions approaches the rows per checkpoint, each file holds
about one row, and that's what you're seeing. Deletion vectors don't help here: they replace only
*position* deletes, while upsert's main output is *equality* deletes.

---

# Part 1 — What a deletion vector is

## 1.1 Definition

A **deletion vector (DV)** is a bitmap of deleted **row positions** for **exactly one data file**.
Bit *i* set means "row *i* of that file is deleted". DVs were added in format **v3**. In v3 they are
the only allowed form of position delete: `MergingSnapshotProducer` rejects position-delete files
with "Must use DVs for position deletes in V3".

```
data file A:   pos 0   1   2   3   4   5   6   7
deleted?           0   1   0   1   0   0   1   0     DV(A) = {1, 3, 6}, cardinality 3
```

The data file is never rewritten. Readers skip rows whose bit is set.

## 1.2 On-disk format (verified in `core/`)

| Level | What | Code |
| --- | --- | --- |
| Bitmap | `RoaringPositionBitmap`: 64-bit positions, split into a high 32-bit key plus a 32-bit `RoaringBitmap` per key. Runs, arrays and bitsets compress well. | `core/.../deletes/RoaringPositionBitmap.java` |
| Blob bytes | 4-byte length (big-endian), 4-byte magic `D1 D3 39 64`, portable Roaring bytes, CRC-32 (big-endian). Delta Lake–compatible layout. | `BitmapPositionDeleteIndex.serialize()` (`:130`) |
| Puffin blob | Type `deletion-vector-v1`, with properties `referenced-data-file` and `cardinality` | `BaseDVFileWriter.toBlob()` |
| Puffin file | Magic `PFA1`, blobs, then a JSON footer listing offsets and lengths. **One Puffin file per writer flush, holding the DVs for many data files.** | `BaseDVFileWriter.close()` |
| Manifest entry | **One `DeleteFile` per DV**: content `POSITION_DELETES`, format `PUFFIN`, the shared Puffin path, `referenced_data_file`, `content_offset`, `content_size_in_bytes`, `record_count` | `BaseDVFileWriter.createDV()` |

So "number of DVs" (manifest entries) and "number of Puffin files" (objects in storage) are
different numbers. Snapshot summaries report DVs as `added-dvs`.

## 1.3 Lifecycle

1. **Write.** `BaseDVFileWriter.delete(path, pos)` sets bits in an in-memory bitmap per data file.
   `close()` writes every bitmap into a single Puffin file.
2. **Plan.** `DeleteFileIndex` matches each data file to **its single DV** by
   `referenced_data_file`. It fails with "Can't index multiple DVs" if a file has two.
3. **Read.** The reader seeks to `content_offset`, reads `content_size_in_bytes`, deserializes the
   bitmap, and skips matching rows.
4. **Update.** A later delete on the same data file must **merge** with the existing DV:
   `loadPreviousDeletes(path)` returns the old bitmap, the two are OR-ed, and the old DV is recorded as
   rewritten and removed from table state. `validateAddedDVs` catches two writers adding a DV for the
   same file concurrently.

## 1.4 DV vs. v2 position-delete file vs. equality delete

| | v2 position-delete file | v3 deletion vector | Equality delete (v2 and v3) |
| --- | --- | --- | --- |
| Stores | `(file_path, pos)` rows | Bitmap of positions | Key column values |
| Scope | Any number of data files | Exactly one data file | Every older data file in the partition |
| Per data file | Any number of files can target it | At most one | Unbounded |
| Read cost | Read and filter by `file_path` | One seek plus bitmap lookup | Load keys into memory, compare each row |
| Writer must know | File path and row position | File path and row position | Only the key |

**The key limitation:** a DV can be written only when the writer knows **which file and row
position** holds the row to delete. A streaming upsert writer doesn't know that for rows written in
earlier checkpoints, so it falls back to equality deletes.

---

# Part 2 — What Flink upsert actually writes

## 2.1 The rule (`BaseDeltaTaskWriter.write`, `BaseTaskWriter.BaseEqualityDeltaWriter`)

In upsert mode every `+I` and `+U` first calls `deleteKey(key)`, because the source can't say
whether the key already exists. Then:

- Key **was written earlier in this checkpoint** (it's in `insertedRowMap`) → **position delete**.
  In v2 this goes to a position-delete file; in v3, to a DV.
- Key **not seen this checkpoint** (brand new, or written in an older checkpoint) → **equality-delete
  row** with the key columns.

Every row of a brand-new key still produces an equality-delete row. The writer can't tell "new"
apart from "exists in an older snapshot".

## 2.2 Measured in this repo

I ran a temporary test against the real `RowDataTaskWriterFactory` with `upsert=true`, Parquet, one
writer subtask and 10,000 keys, then deleted the test. Checkpoint 1 writes 10,000 new keys. Checkpoint
2 upserts each key twice.

| Format | Layout | Step | Input rows | Data files | Eq-delete files | Eq-delete rows | Position deletes (entries / physical files) | Position delete rows |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| v2 | unpartitioned | ckpt 1, new keys | 10,000 | 1 | 1 | **10,000** | 0 / 0 | 0 |
| v2 | unpartitioned | ckpt 2, keys ×2 | 20,000 | 1 | 1 | 10,000 | 1 / 1 (Parquet) | 10,000 |
| v3 | unpartitioned | ckpt 1, new keys | 10,000 | 1 | 1 | **10,000** | 0 / 0 | 0 |
| v3 | unpartitioned | ckpt 2, keys ×2 | 20,000 | 1 | 1 | 10,000 | 1 / 1 (Puffin) | 10,000 |
| v2 | 100 partitions | ckpt 1, new keys | 10,000 | 100 | **100** | 10,000 | 0 / 0 | 0 |
| v2 | 100 partitions | ckpt 2, keys ×2 | 20,000 | 100 | **100** | 10,000 | 100 / **100** | 10,000 |
| v3 | 100 partitions | ckpt 1, new keys | 10,000 | 100 | **100** | 10,000 | 0 / 0 | 0 |
| v3 | 100 partitions | ckpt 2, keys ×2 | 20,000 | 100 | **100** | 10,000 | 100 DVs / **1** Puffin | 10,000 |

What this shows:

1. **One equality-delete row per input row** for keys not seen this checkpoint, including keys that
   never existed (ckpt 1).
2. **One equality-delete file per partition per writer per checkpoint**, no matter how many rows it
   holds.
3. In v3, position deletes collapse into **one Puffin file** per writer (versus 100 Parquet files in
   v2). Equality deletes are **unchanged** between v2 and v3.

## 2.3 Why it becomes "one delete file per row" in production

Each writer subtask holds one `RowDataDeltaWriter` per partition it touches, each with its own
equality-delete file (`PartitionedDeltaWriter`). With the default `write.distribution-mode=none` and
equality fields set, `IcebergSink` applies `keyBy(primary key)`
(`distributeDataStreamByNoneDistributionMode`), so **every subtask receives rows for every
partition**. Per checkpoint:

```
equality-delete files  ≈  N subtasks × P partitions touched
rows per file          ≈  R rows per checkpoint / (N × P)
```

| Example | N | P | R per checkpoint | Eq-delete files per checkpoint | Rows per file |
| --- | --- | --- | --- | --- | --- |
| Small job | 4 | 10 | 100,000 | 40 | 2,500 |
| Wide partitioning | 32 | 1,000 | 50,000 | 32,000 | **~1.6** |
| Bucketed key, 1-min checkpoints | 64 | 512 | 30,000 | 32,768 | **<1** (some files hold a single row) |

These files **pile up with every checkpoint** until compaction. With 1-minute checkpoints, the wide
example adds about 1.9 M equality-delete files per hour. Each one must be applied to every older data
file in its partition, so planning and reads degrade quickly.

## 2.4 Why deletion vectors don't fix it

- The Flink writer produces DVs only for **in-checkpoint duplicates**, through `PartitioningDVWriter`
  in `BaseTaskWriter`. It's built with `p -> null` as the previous-delete loader, so it never targets
  data files from earlier commits.
- Deletes of keys from earlier checkpoints, which is most upsert traffic, remain **equality deletes**
  in v3. The writer doesn't know which old file and position holds the key.
- Turning the equality deletes into DVs requires a lookup of where each key lives. That's what
  `ConvertEqualityDeletes` does as a separate maintenance step.

## 2.5 What to do about it

| Lever | Effect | Trade-off |
| --- | --- | --- |
| `write.distribution-mode=hash` with every partition column in the PRIMARY KEY | `keyBy(partition)`: each partition goes to one subtask, so files ≈ **P** instead of N × P | Skew if a few partitions are hot |
| Turn **upsert off** when the source emits a real changelog (Debezium or Flink CDC with `-U` / `-D`) | CDC mode writes **no delete for plain `+I`**; deletes only on real updates and deletes | Delete rows contain full rows, not just keys; the source must be a correct changelog |
| Longer checkpoint interval | Fewer commits; more rows per file | Higher latency; bigger `insertedRowMap` in memory |
| Fewer writer subtasks, or coarser partitioning | Smaller N × P | Lower write throughput or less pruning |
| Maintenance: `ConvertEqualityDeletes`, then `RewriteDataFiles` (`flink/.../maintenance/api/`, attachable to `IcebergSink`) | Converts equality deletes into DVs, then compacts | Extra compute; needs a lock (`JdbcLockFactory` / `ZkLockFactory`) |
| Format v3 | Position deletes become one Puffin file per writer, not one Parquet file per data file | **No change** to equality-delete count |

**Recommended order for this branch:** use hash distribution with partition columns in the key,
turn upsert off if the source changelog allows it, then attach `ConvertEqualityDeletes` and
`RewriteDataFiles` as post-commit maintenance. Measure with each snapshot's summary
(`added-equality-delete-files`, `added-dvs`, `added-position-delete-files`).

## 2.6 Code map

| Concern | File |
| --- | --- |
| RowKind switch, `deleteKey` on every upsert | `flink/v2.3/flink/src/main/java/org/apache/iceberg/flink/sink/BaseDeltaTaskWriter.java:81` |
| One delta writer per partition | `.../sink/PartitionedDeltaWriter.java:71` |
| Default distribution `none` → `keyBy(primary key)` | `.../flink/FlinkWriteConf.java:163`, `.../sink/IcebergSink.java:969` |
| Position vs. equality decision, `insertedRowMap` | `core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java:228–307` |
| DV writer created with `p -> null` | `core/.../io/BaseTaskWriter.java:115` |
| DV Puffin layout, one `DeleteFile` per DV | `core/src/main/java/org/apache/iceberg/deletes/BaseDVFileWriter.java` |
| DV bitmap serialization | `core/.../deletes/BitmapPositionDeleteIndex.java:130` |
| v3 must use DVs; concurrent DV validation | `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:309`, `:869` |
| One DV per data file at planning | `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:573` |
