# SlateDB Architecture

This document provides a visual overview of how the major components and structs
in the `slatedb` crate relate to each other — what contains what, what creates what,
and how data flows through the system.

## High-Level Component Overview

```mermaid
graph TB
    Db["<b>Db</b><br/>(public handle, Clone)"]
    Db -->|"Arc"| DbInner

    subgraph DbInner["<b>DbInner</b> (core engine)"]
        direction TB
        State["Arc&lt;RwLock&lt;DbState&gt;&gt;"]
        TS["Arc&lt;TableStore&gt;"]
        MF["Arc&lt;MemtableFlusher&gt;"]
        WBM["Arc&lt;WalBufferManager&gt;"]
        RDR["Reader"]
        ORC["Arc&lt;DbOracle&gt;"]
        TXM["Arc&lt;TransactionManager&gt;"]
        SNM["Arc&lt;SnapshotManager&gt;"]
        CLK["Arc&lt;MonotonicClock&gt;"]
        STS["DbStats"]
        STM["DbStatusManager"]
        CFG["Settings"]
    end

    Db -->|"Arc"| TaskExecutor["MessageHandlerExecutor"]

    TaskExecutor -->|spawns| WriteBatchTask["WriteBatchEventHandler"]
    TaskExecutor -->|spawns| WalFlushTask["WalFlushHandler"]
    TaskExecutor -->|spawns| CompactorTask["Compactor"]
    TaskExecutor -->|spawns| GCTask["GarbageCollector"]
    TaskExecutor -->|spawns| FlushPipeline["Flush Pipeline<br/>(FlushTracker + Uploader + ManifestWriter)"]
```

## DbState & Manifest

```mermaid
graph TB
    DbState["<b>DbState</b>"]
    DbState -->|contains| WKV["WritableKVTable<br/>(current mutable memtable)"]
    DbState -->|"Arc (COW)"| COW["COWDbState"]

    WKV -->|"Arc"| KVTable["<b>KVTable</b><br/>SkipMap&lt;SequencedKey, RowEntry&gt;"]
    KVTable -->|tracks| SeqTracker1["SequenceTracker"]

    COW -->|contains| ImmQ["VecDeque&lt;Arc&lt;ImmutableMemtable&gt;&gt;"]
    COW -->|contains| DM["DirtyObject&lt;Manifest&gt;"]

    ImmQ -->|each| IMM["<b>ImmutableMemtable</b>"]
    IMM -->|"Arc"| KVTable2["KVTable"]
    IMM -->|tracks| SeqTracker2["SequenceTracker"]

    DM --> Manifest["<b>Manifest</b>"]
    Manifest -->|contains| Core["ManifestCore"]
    Manifest -->|contains| ExtDBs["Vec&lt;ExternalDb&gt;"]

    Core -->|contains| L0["VecDeque&lt;SsTableView&gt;<br/>(L0 SSTs)"]
    Core -->|contains| SR["Vec&lt;SortedRun&gt;<br/>(compacted)"]
    Core -->|contains| CKP["Vec&lt;Checkpoint&gt;"]
    Core -->|contains| SeqTracker3["SequenceTracker"]

    SR -->|each contains| SRViews["Vec&lt;SsTableView&gt;"]

    L0 -->|each| STV["<b>SsTableView</b>"]
    SRViews -->|each| STV
    STV -->|contains| STH["<b>SsTableHandle</b>"]
    STH -->|contains| STID["SsTableId<br/>(Wal(u64) | Compacted(Ulid))"]
    STH -->|contains| STI["SsTableInfo<br/>(key range, offsets, codec, filter)"]
```

## Core Types

```mermaid
graph LR
    RE["<b>RowEntry</b><br/>key: Bytes<br/>seq: u64<br/>create_ts, expire_ts"]
    RE -->|contains| VD["<b>ValueDeletable</b><br/>Value(Bytes)<br/>Merge(Bytes)<br/>Tombstone"]

    KV["<b>KeyValue</b><br/>(public, no tombstones)"]
    RE -.->|"From"| KV

    WB["<b>WriteBatch</b><br/>BTreeMap&lt;SequencedKey, WriteOp&gt;"]
    WB -->|contains| WO["<b>WriteOp</b><br/>Put | Delete | Merge"]
    WO -.->|converts to| RE

    SK["<b>SequencedKey</b><br/>user_key: Bytes<br/>seq: u64"]
```

## TableStore & SST Format

```mermaid
graph TB
    TSt["<b>TableStore</b>"]
    TSt -->|contains| OS["ObjectStores"]
    TSt -->|contains| SSF["SsTableFormat"]
    TSt -->|contains| PR["PathResolver"]
    TSt -->|"Option&lt;Arc&gt;"| DBC["dyn DbCache"]

    TSt -->|"creates (build)"| ESB["<b>EncodedSsTableBuilder</b>"]
    TSt -->|"creates (stream)"| ESW["<b>EncodedSsTableWriter</b>"]
    TSt -->|"creates (WAL)"| EWSB["EncodedWalSsTableBuilder"]

    ESB -->|contains| BBwS["BlockBuilderWithStats"]
    BBwS -->|contains| BB["BlockBuilder<br/>(V1 | V2)"]
    ESB -->|contains| FB["Vec&lt;FilterBuilder&gt;"]
    ESB -->|produces| EST["<b>EncodedSsTable</b>"]

    ESW -->|contains| ESB2["EncodedSsTableBuilder"]
    ESW -->|produces| STH2["SsTableHandle"]

    EST -->|contains| Blocks["VecDeque&lt;EncodedSsTableBlock&gt;"]
    EST -->|contains| STI2["SsTableInfo"]
    EST -->|contains| IDX["SsTableIndexOwned"]
    EST -->|contains| NF["Arc&lt;[NamedFilter]&gt;"]
    EST -->|contains| Stats["Option&lt;SstStats&gt;"]

    Blocks -->|each| ESBlk["<b>EncodedSsTableBlock</b><br/>offset, Block, encoded_bytes"]
    ESBlk -->|contains| Blk["<b>Block</b><br/>data: Bytes<br/>offsets: Vec&lt;u16&gt;"]

    SSF -->|contains| FP["Vec&lt;Arc&lt;dyn FilterPolicy&gt;&gt;"]
    FP -.->|creates| BFB["BloomFilterBuilder"]
    BFB -.->|builds| BF["BloomFilter"]
```

## Iterator Chain (Read Path)

```mermaid
graph TB
    subgraph "User-Facing"
        DBI["<b>DbIterator</b><br/>(returned by scan)"]
        GI["<b>GetIterator</b><br/>(used by get)"]
    end

    DBI -->|wraps| OuterMI["MergeIterator (outer)"]

    OuterMI -->|source| WBI["WriteBatchIterator<br/>(uncommitted txn writes)"]
    OuterMI -->|source| MemMI["MergeIterator (memtables)"]
    OuterMI -->|source| L0MI["MergeIterator (L0 SSTs)"]
    OuterMI -->|source| SRMI["MergeIterator (sorted runs)"]

    MemMI -->|each| MTI["MemTableIteratorInner<br/>(over SkipMap range)"]

    L0MI -->|each| SSI1["SstIterator"]

    SRMI -->|each| SRI["<b>SortedRunIterator</b>"]
    SRI -->|creates per-SST| SSI2["SstIterator"]

    SSI1 --> SSIDel["SstIteratorDelegate"]
    SSI2 --> SSIDel
    SSIDel -->|"point lookup"| FI["FilterIterator<br/>(bloom filter skip)"]
    SSIDel -->|"range scan"| ISI["InternalSstIterator"]
    FI -->|wraps| ISI

    ISI -->|"creates per-block"| DBlkI["DataBlockIterator<br/>(V1 | V2)"]
    DBlkI -->|V1| BI["BlockIterator"]
    DBlkI -->|V2| BI2["BlockIteratorV2"]

    OuterMI -.->|optionally wrapped by| MOI["MergeOperatorIterator"]
    MOI -.->|optionally wrapped by| RetI["RetentionIterator"]

    subgraph "Core Trait"
        REI["<b>RowEntryIterator</b> (trait)<br/>init / next / seek → RowEntry"]
    end

    MTI -.->|impl| REI
    SSI1 -.->|impl| REI
    SRI -.->|impl| REI
    OuterMI -.->|impl| REI
    DBlkI -.->|impl| REI
```

## Memtable Flush Pipeline

```mermaid
graph LR
    subgraph "MemtableFlusher"
        FT["<b>FlushTracker</b><br/>(MessageHandler)"]
        UL["<b>Uploader</b><br/>(N parallel UploadHandlers)"]
        MW["<b>ManifestWriter</b><br/>(ManifestWriterHandler)"]
    end

    IMM2["ImmutableMemtable"] -->|"frozen by DbInner"| FT

    FT -->|"dispatches UploadJob"| UL
    UL -->|"UploadJob { imm, sst_id }"| UH["UploadHandler"]
    UH -->|"builds SST via TableStore"| EST2["EncodedSsTable"]
    UH -->|"uploads SST"| ObjStore["Object Store"]
    UH -->|"UploadedMemtable<br/>{ imm, SsTableHandle }"| FT

    FT -->|"forwards UploadedMemtable"| MW
    MW -->|"writes to"| FM["FenceableManifest"]
    FM -->|"persists"| ManifestStore["ManifestStore"]
    MW -->|"FlushResult { durable_seq }"| FT
    FT -->|"advances oracle<br/>+ marks durable"| Oracle["DbOracle"]
```

## Compactor

```mermaid
graph TB
    Comp["<b>Compactor</b>"]
    Comp -->|creates| CEH["CompactorEventHandler"]

    CEH -->|contains| CSW["CompactorStateWriter"]
    CEH -->|uses| Sched["dyn CompactionScheduler"]
    CEH -->|uses| Exec["dyn CompactionExecutor"]

    CSW -->|wraps| CS["<b>CompactorState</b>"]
    CS -->|contains| DM2["DirtyObject&lt;Manifest&gt;"]
    CS -->|contains| DC["DirtyObject&lt;Compactions&gt;"]
    DC -->|contains| CC["CompactionsCore"]
    CC -->|"BTreeMap"| Compaction["<b>Compaction</b>"]
    Compaction -->|contains| CSpec["CompactionSpec<br/>sources: Vec&lt;SourceId&gt;<br/>destination: u32"]
    Compaction -->|contains| OutSSTs["output_ssts: Vec&lt;SsTableHandle&gt;"]

    Exec -->|impl| TCE["TokioCompactionExecutorInner"]
    TCE -->|uses| TS2["TableStore"]
    TCE -->|uses| MS2["ManifestStore"]
    TCE -->|"creates iterators"| MergeIter["MergeIterator<br/>over SortedRunIterator / SstIterator"]
    TCE -->|"writes SSTs via"| ESW2["EncodedSsTableWriter"]
    TCE -->|"produces"| NewSR["new SortedRun"]
```

## WAL Buffer

```mermaid
graph TB
    WBMGR["<b>WalBufferManager</b>"]
    WBMGR -->|contains| WBMI["WalBufferManagerInner"]
    WBMGR -->|uses| TS3["Arc&lt;TableStore&gt;"]
    WBMGR -->|uses| MC["Arc&lt;MonotonicClock&gt;"]

    WBMI -->|current| WBuf["<b>WalBuffer</b><br/>VecDeque&lt;RowEntry&gt;"]
    WBMI -->|frozen| ImmWals["VecDeque&lt;(u64, Arc&lt;WalBuffer&gt;)&gt;"]
    WBMI -->|notifies| FlushTx["SafeSender&lt;WalFlushWork&gt;"]
    WBMI -->|uses| ORC2["Arc&lt;DbOracle&gt;"]

    WFH["<b>WalFlushHandler</b><br/>(background task)"]
    WFH -->|reads from| WBMGR
    WFH -->|"builds WAL SST"| EWSB2["EncodedWalSsTableBuilder"]
    WFH -->|"uploads to"| ObjStore2["Object Store"]
```

## Garbage Collector

```mermaid
graph TB
    GC["<b>GarbageCollector</b><br/>(MessageHandler&lt;GcMessage&gt;)"]
    GC -->|contains| ManGC["ManifestGcTask"]
    GC -->|contains| WalGC["WalGcTask"]
    GC -->|contains| CompGC["CompactedGcTask"]
    GC -->|contains| CompsGC["CompactionsGcTask"]
    GC -->|contains| DetGC["DetachGcTask"]

    ManGC -->|uses| MS3["ManifestStore"]
    WalGC -->|uses| MS4["ManifestStore"]
    WalGC -->|uses| TS4["TableStore"]
    CompGC -->|uses| MS5["ManifestStore"]
    CompGC -->|uses| CmpS["CompactionsStore"]
    CompGC -->|uses| TS5["TableStore"]
    CompsGC -->|uses| CmpS2["CompactionsStore"]
    DetGC -->|uses| MS6["ManifestStore"]

    GC -->|tracks| GCSt["GcStats"]
```

## Transactions, Snapshots & Oracle

```mermaid
graph TB
    DbOracle["<b>DbOracle</b><br/>last_seq, last_committed_seq<br/>last_durable_seq"]

    TxMgr["<b>TransactionManager</b>"]
    TxMgr -->|uses| DbOracle
    TxMgr -->|"tracks"| ActiveTxns["HashMap&lt;Uuid, TransactionState&gt;"]
    TxMgr -->|"tracks"| RecentCommit["VecDeque&lt;TransactionState&gt;"]

    SnapMgr["<b>SnapshotManager</b>"]
    SnapMgr -->|uses| DbOracle
    SnapMgr -->|"tracks"| ActiveSnaps["HashMap&lt;Uuid, u64&gt;"]

    DbTxn["<b>DbTransaction</b><br/>(public)"]
    DbTxn -->|"Arc"| TxMgr
    DbTxn -->|"Arc"| DbInner2["DbInner"]
    DbTxn -->|contains| WB2["WriteBatch"]
    DbTxn -->|contains| IL["IsolationLevel<br/>(Snapshot | SerializableSnapshot)"]

    DbSnap["<b>DbSnapshot</b><br/>(public)"]
    DbSnap -->|"Arc"| DbInner3["DbInner"]
    DbSnap -->|"registered in"| SnapMgr

    TxState["<b>TransactionState</b><br/>started_seq, write_keys<br/>read_keys, read_ranges"]
```

## Read-Only Reader (DbReader)

```mermaid
graph TB
    DbReader["<b>DbReader</b><br/>(public, read-only instance)"]
    DbReader -->|"Arc"| DRI["DbReaderInner"]

    DRI -->|contains| RDR2["Reader"]
    DRI -->|contains| DRO["DbReaderOracle"]
    DRI -->|contains| TS6["Arc&lt;TableStore&gt;"]
    DRI -->|contains| MS7["Arc&lt;ManifestStore&gt;"]
    DRI -->|"RwLock"| CPS["CheckpointState"]

    CPS -->|contains| CKP2["Checkpoint"]
    CPS -->|contains| Man2["Manifest"]
    CPS -->|contains| ImmQ2["VecDeque&lt;Arc&lt;ImmutableMemtable&gt;&gt;"]

    DbReader -->|spawns| MP["ManifestPoller<br/>(background refresh)"]
```

## Cache Layer

```mermaid
graph TB
    subgraph "In-Memory Cache (DbCache trait)"
        SC["<b>SplitCache</b><br/>block_cache + meta_cache"]
        SC -->|each| DCW["DbCacheWrapper<br/>(scoped + stats)"]
        DCW -->|wraps| Impl["moka / foyer / foyer_hybrid"]
    end

    CK["<b>CachedKey</b><br/>scope_id, sst_id, block_id"]
    CE["<b>CachedEntry</b>"]
    CE -->|variant| CBlk["Block"]
    CE -->|variant| CIdx["SsTableIndexOwned"]
    CE -->|variant| CFlt["NamedFilter"]
    CE -->|variant| CSst["SstStats"]

    subgraph "Disk Cache (ObjectStore wrapper)"
        COS["<b>CachedObjectStore</b>"]
        COS -->|wraps| InnerOS["dyn ObjectStore"]
        COS -->|uses| LCS["dyn LocalCacheStorage"]
        COS -->|uses| AP["dyn AdmissionPicker"]
    end

    StoreProvider["<b>DefaultStoreProvider</b>"]
    StoreProvider -->|creates| TSt2["TableStore (with DbCache)"]
    StoreProvider -->|creates| MSt2["ManifestStore"]
```

## Store Layer

```mermaid
graph TB
    DSP["<b>DefaultStoreProvider</b>"]
    DSP -->|creates| TS7["<b>TableStore</b>"]
    DSP -->|creates| MS8["<b>ManifestStore</b>"]

    TS7 -->|uses| ObjStores["<b>ObjectStores</b><br/>sst_store + wal_store"]
    TS7 -->|uses| SSFmt["SsTableFormat"]
    TS7 -->|uses| Cache["Option&lt;Arc&lt;dyn DbCache&gt;&gt;"]

    MS8 -->|backed by| SeqStore["dyn SequencedStorageProtocol"]

    FM2["<b>FenceableManifest</b><br/>(epoch-fenced writes)"]
    FM2 -->|wraps| SM["StoredManifest"]
    SM -->|wraps| MS8
```

## Background Task Dispatch

```mermaid
graph TB
    MHE["<b>MessageHandlerExecutor</b><br/>(task pool)"]

    MHE -->|spawns| MD1["MessageDispatcher&lt;WriteBatchMessage&gt;"]
    MHE -->|spawns| MD2["MessageDispatcher&lt;WalFlushWork&gt;"]
    MHE -->|spawns| MD3["MessageDispatcher&lt;TrackerMessage&gt;"]
    MHE -->|spawns| MD4["MessageDispatcher&lt;UploadJob&gt;"]
    MHE -->|spawns| MD5["MessageDispatcher&lt;ManifestWriterCommand&gt;"]
    MHE -->|spawns| MD6["MessageDispatcher&lt;CompactorMessage&gt;"]
    MHE -->|spawns| MD7["MessageDispatcher&lt;GcMessage&gt;"]

    MD1 -->|handles| WBH["WriteBatchEventHandler"]
    MD2 -->|handles| WFH2["WalFlushHandler"]
    MD3 -->|handles| FT2["FlushTracker"]
    MD4 -->|handles| UH2["UploadHandler (×N)"]
    MD5 -->|handles| MWH["ManifestWriterHandler"]
    MD6 -->|handles| CEH2["CompactorEventHandler"]
    MD7 -->|handles| GC2["GarbageCollector"]

    MH["<b>MessageHandler&lt;T&gt;</b> (trait)<br/>handle() + tickers() + notifiers()"]
    WBH -.->|impl| MH
    WFH2 -.->|impl| MH
    FT2 -.->|impl| MH
    UH2 -.->|impl| MH
    MWH -.->|impl| MH
    CEH2 -.->|impl| MH
    GC2 -.->|impl| MH
```

## Data Flow Summary

```
Write Path:
  Db::put/merge/delete
    → WriteBatch → WriteBatchMessage → WriteBatchEventHandler
    → RowEntry written to KVTable (memtable) + WalBuffer
    → WalFlushHandler flushes WalBuffer → WAL SST on object store
    → DbInner freezes memtable → ImmutableMemtable
    → FlushTracker → Uploader builds SST → uploads
    → ManifestWriter updates manifest with new L0 SsTableView

Read Path:
  Db::get/scan
    → Reader builds iterator chain
    → MergeIterator over: memtable + imm memtables + L0 SSTs + sorted runs
    → SstIterator (with bloom filter skip) → Block decoding
    → MergeOperatorIterator resolves merge operands
    → DbIterator yields KeyValue to user

Compaction:
  Compactor polls manifest for new L0 SSTs
    → CompactionScheduler decides what to compact
    → CompactionExecutor merges SSTs via MergeIterator
    → Writes new SortedRun SSTs via EncodedSsTableWriter
    → Updates manifest (removes old L0s, adds new sorted runs)

Garbage Collection:
  GarbageCollector runs periodic tasks
    → WalGcTask: removes old WAL SSTs no longer needed
    → CompactedGcTask: removes SSTs replaced by compaction
    → ManifestGcTask: removes old manifest versions
    → CompactionsGcTask: removes old compaction state files
    → DetachGcTask: detaches expired external DB references
```
