//! Integration test for pinned SST filters and indexes (RFC 0037).
//!
//! Every pinning mode must return the same results and record the same
//! filter verdicts. With pinning on, point lookups read no filter or index
//! from object storage once the pin is filled. A randomized test checks every
//! mode, on a writer with a live compactor and on readers, against a model.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, DbReaderOptions, FlushOptions, FlushType, Settings,
    SizeTieredCompactionSchedulerOptions,
};
use slatedb::db_stats::{
    FILTER_KIND_LABEL, FILTER_KIND_POINT, PINNED_SST_COUNT, PIN_LEVEL_L0, PIN_LEVEL_LABEL,
    SST_FILTER_NEGATIVE_COUNT, SST_FILTER_POSITIVE_COUNT,
};
use slatedb::instrumented_object_store_stats::REQUEST_COUNT as OBJECT_STORE_REQUEST_COUNT;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
use slatedb::{CompactorBuilder, Db, DbReader, MetadataPinning, SstBlockSize};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use tokio::runtime::Runtime;

const SSTS: usize = 4;
const KEYS_PER_SST: usize = 8;

/// Keys for one SST, as `k<digit>-<sst>`. Every SST spans `k0` to `k7`, so
/// key-range pruning never excludes an SST and every candidate SST is
/// decided by its filter.
fn sst_keys(sst: usize) -> Vec<Vec<u8>> {
    (0..KEYS_PER_SST)
        .map(|i| format!("k{i}-{sst}").into_bytes())
        .collect()
}

/// Keys inside every SST's range that no SST holds.
fn absent_keys() -> Vec<Vec<u8>> {
    (0..KEYS_PER_SST)
        .map(|i| format!("k{i}-x").into_bytes())
        .collect()
}

fn counter(recorder: &DefaultMetricsRecorder, name: &str, labels: &[(&str, &str)]) -> u64 {
    recorder
        .snapshot()
        .by_name_and_labels(name, labels)
        .map(|m| match m.value {
            MetricValue::Counter(v) => v,
            ref other => panic!("expected counter, got {other:?}"),
        })
        .unwrap_or(0)
}

fn up_down(recorder: &DefaultMetricsRecorder, name: &str, labels: &[(&str, &str)]) -> i64 {
    recorder
        .snapshot()
        .by_name_and_labels(name, labels)
        .map(|m| match m.value {
            MetricValue::UpDownCounter(v) => v,
            ref other => panic!("expected up-down counter, got {other:?}"),
        })
        .unwrap_or(0)
}

/// Total object-store GET-shaped requests against the main store.
fn main_store_gets(recorder: &DefaultMetricsRecorder) -> u64 {
    ["get", "get_range", "get_ranges", "head"]
        .iter()
        .map(|api| {
            counter(
                recorder,
                OBJECT_STORE_REQUEST_COUNT,
                &[
                    ("component", "db"),
                    ("store_type", "main"),
                    ("op", "get"),
                    ("api", api),
                ],
            )
        })
        .sum()
}

/// `(positives, negatives)` recorded for point queries.
fn filter_verdicts(recorder: &DefaultMetricsRecorder) -> (u64, u64) {
    let labels = [(FILTER_KIND_LABEL, FILTER_KIND_POINT)];
    (
        counter(recorder, SST_FILTER_POSITIVE_COUNT, &labels),
        counter(recorder, SST_FILTER_NEGATIVE_COUNT, &labels),
    )
}

/// Builds a DB holding [`SSTS`] L0 SSTs with no block cache, so every unpinned
/// metadata read is an object-store request.
async fn open_db(path: &str, mode: MetadataPinning) -> (Db, Arc<DefaultMetricsRecorder>) {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::builder(path, store)
        .with_settings(Settings {
            flush_interval: None,
            min_filter_keys: 0,
            compactor_options: None,
            metadata_pinning: mode,
            ..Settings::default()
        })
        .with_metrics_recorder(recorder.clone())
        .with_db_cache_disabled()
        .build()
        .await
        .expect("failed to build db");
    for sst in 0..SSTS {
        for key in sst_keys(sst) {
            db.put(&key, &key).await.expect("put failed");
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("flush failed");
    }
    (db, recorder)
}

async fn lookups(db: &Db) -> Vec<Option<Bytes>> {
    let mut results = Vec::new();
    for sst in 0..SSTS {
        for key in sst_keys(sst) {
            results.push(db.get(&key).await.expect("get failed"));
        }
    }
    for key in absent_keys() {
        results.push(db.get(&key).await.expect("get failed"));
    }
    results
}

struct Run {
    results: Vec<Option<Bytes>>,
    verdicts: (u64, u64),
    gets: u64,
    pinned_l0: i64,
}

async fn run(mode: MetadataPinning) -> Run {
    let (db, recorder) = open_db(&format!("/pinned/{mode:?}"), mode).await;
    let gets_before = main_store_gets(&recorder);
    let results = lookups(&db).await;
    let run = Run {
        results,
        verdicts: filter_verdicts(&recorder),
        gets: main_store_gets(&recorder) - gets_before,
        pinned_l0: up_down(
            &recorder,
            PINNED_SST_COUNT,
            &[(PIN_LEVEL_LABEL, PIN_LEVEL_L0)],
        ),
    };
    db.close().await.expect("close failed");
    run
}

#[tokio::test]
async fn pinning_modes_agree_on_results_and_filter_verdicts() {
    let none = run(MetadataPinning::None).await;
    let l0 = run(MetadataPinning::L0).await;
    let all = run(MetadataPinning::All).await;

    assert!(none
        .results
        .iter()
        .take(SSTS * KEYS_PER_SST)
        .all(Option::is_some));
    assert!(none
        .results
        .iter()
        .skip(SSTS * KEYS_PER_SST)
        .all(Option::is_none));
    assert_eq!(l0.results, none.results);
    assert_eq!(all.results, none.results);

    // Each present key lives in exactly one SST, so every mode records one
    // positive verdict per present key. Negative verdicts depend on how far
    // the walk looks ahead, so they are only required to exist.
    let present = (SSTS * KEYS_PER_SST) as u64;
    assert_eq!(none.verdicts.0, present);
    assert_eq!(l0.verdicts.0, present);
    assert_eq!(all.verdicts.0, present);
    assert!(none.verdicts.1 > 0 && l0.verdicts.1 > 0 && all.verdicts.1 > 0);

    assert_eq!(none.pinned_l0, 0);
    assert_eq!(l0.pinned_l0, SSTS as i64);
    assert_eq!(all.pinned_l0, SSTS as i64);

    // The writer pins each flushed SST with the filters and index it built,
    // so a pinned lookup reads only the data block of the positive SST.
    let data_block_reads = (SSTS * KEYS_PER_SST) as u64;
    assert_eq!(l0.gets, data_block_reads);
    assert_eq!(all.gets, data_block_reads);
    assert!(none.gets > data_block_reads);
}

const MODES: [MetadataPinning; 3] = [
    MetadataPinning::None,
    MetadataPinning::L0,
    MetadataPinning::All,
];

/// Writes draw keys from this many suffixes. Lookups also probe a few
/// suffixes past it, which no write touches, so filters see absent keys that
/// fall inside SST key ranges.
const KEY_ALPHABET: u8 = 32;
const PROBE_ALPHABET: u8 = KEY_ALPHABET + 8;

fn model_key(suffix: u8) -> Bytes {
    Bytes::from(vec![b'k', suffix])
}

#[derive(Debug, Clone)]
enum ModelOp {
    Put(u8, Bytes),
    Delete(u8),
    /// Flush the memtable to a new L0 SST.
    Flush,
    /// Flush, then wait for the compactor to drain L0 into sorted runs.
    Compact,
}

fn model_ops(max_ops: usize) -> impl Strategy<Value = Vec<ModelOp>> {
    // Values large enough that a sorted run's live keys fill several 1KiB
    // blocks, so compaction splits a run across SSTs.
    let value = proptest::collection::vec(any::<u8>(), 64..=256).prop_map(Bytes::from);
    let op = prop_oneof![
        6 => (0..KEY_ALPHABET, value).prop_map(|(k, v)| ModelOp::Put(k, v)),
        2 => (0..KEY_ALPHABET).prop_map(ModelOp::Delete),
        2 => Just(ModelOp::Flush),
        2 => Just(ModelOp::Compact),
    ];
    proptest::collection::vec(op, 1..=max_ops)
}

const MODEL_PATH: &str = "/pinned/model";

/// A writer with an in-process compactor. Each compaction applies the
/// compactor's manifest through the writer's `merge_remote_manifest`, and
/// with `trivial_move` on, a compaction of disjoint L0 SSTs moves them into
/// a sorted run without rewriting them.
async fn open_compacting_db(
    store: Arc<dyn ObjectStore>,
    mode: MetadataPinning,
    trivial_move: bool,
) -> Db {
    let settings = Settings {
        manifest_poll_interval: Duration::from_millis(10),
        // Every SST gets a filter, however few keys it holds.
        min_filter_keys: 0,
        l0_max_ssts: 10_000,
        l0_max_ssts_per_key: 10_000,
        metadata_pinning: mode,
        ..Settings::default()
    };
    let compactor_options = CompactorOptions {
        poll_interval: Duration::from_millis(1),
        commit_compacted_interval: Duration::from_millis(1),
        enable_trivial_move: trivial_move,
        scheduler_options: SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 1,
            ..Default::default()
        }
        .into(),
        worker: Some(CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(1),
            // Small output SSTs give sorted runs several SSTs each.
            max_sst_size: 64,
            ..Default::default()
        }),
        ..Default::default()
    };
    Db::builder(MODEL_PATH, store.clone())
        .with_settings(settings)
        .with_sst_block_size(SstBlockSize::Block1Kib)
        .with_compactor_builder(
            CompactorBuilder::new(MODEL_PATH, store)
                .with_scheduler_supplier(Arc::new(SizeTieredCompactionSchedulerSupplier::new()))
                .with_options(compactor_options),
        )
        .build()
        .await
        .expect("failed to build db")
}

async fn flush_memtable(db: &Db) {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("flush failed");
}

// A wall-clock wait is fine here: this test does not run under DST.
#[allow(clippy::disallowed_methods)]
async fn compact_l0(db: &Db) {
    flush_memtable(db).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !db.manifest().l0().is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("compactor did not drain L0 within 30s");
}

enum Target<'a> {
    Db(&'a Db),
    Reader(&'a DbReader),
}

impl Target<'_> {
    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self {
            Target::Db(db) => db.get(key).await,
            Target::Reader(reader) => reader.get(key).await,
        }
        .expect("get failed")
    }

    async fn scan_all(&self) -> Vec<(Bytes, Bytes)> {
        let mut iter = match self {
            Target::Db(db) => db.scan(..).await,
            Target::Reader(reader) => reader.scan(..).await,
        }
        .expect("scan failed");
        let mut rows = Vec::new();
        while let Some(kv) = iter.next().await.expect("scan next failed") {
            rows.push((kv.key, kv.value));
        }
        rows
    }
}

async fn assert_matches_model(target: Target<'_>, model: &BTreeMap<Bytes, Bytes>, label: &str) {
    // Twice: the first lookup of an SST may fill its pin, and the second
    // reads through it.
    for pass in 0..2 {
        for suffix in 0..PROBE_ALPHABET {
            let key = model_key(suffix);
            assert_eq!(
                target.get(&key).await,
                model.get(&key).cloned(),
                "{label}: get of {key:?}, pass {pass}"
            );
        }
    }
    let expected: Vec<(Bytes, Bytes)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(target.scan_all().await, expected, "{label}: scan");
}

/// Applies a random write sequence, with flushes and compactions, to a writer
/// in a random pinning mode and to a `BTreeMap`. Then the writer and a reader
/// in every pinning mode must agree with the model on every `get` and on a
/// full scan.
#[test]
fn pinning_modes_match_a_model() {
    // Each case opens a writer, a compactor and three readers, so keep the
    // case count low.
    let config = Config {
        cases: 16,
        source_file: Some(file!()),
        ..Config::default()
    };
    let mut runner = TestRunner::new(config);
    let runtime = Runtime::new().expect("failed to build runtime");

    runner
        .run(
            &(model_ops(60), 0..MODES.len(), any::<bool>()),
            |(ops, writer_mode, trivial_move)| {
                let writer_mode = MODES[writer_mode];
                runtime.block_on(async {
                    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                    let db = open_compacting_db(store.clone(), writer_mode, trivial_move).await;
                    let mut model = BTreeMap::new();
                    for op in &ops {
                        match op {
                            ModelOp::Put(suffix, value) => {
                                let key = model_key(*suffix);
                                db.put(&key, value).await.expect("put failed");
                                model.insert(key, value.clone());
                            }
                            ModelOp::Delete(suffix) => {
                                let key = model_key(*suffix);
                                db.delete(&key).await.expect("delete failed");
                                model.remove(&key);
                            }
                            ModelOp::Flush => flush_memtable(&db).await,
                            ModelOp::Compact => compact_l0(&db).await,
                        }
                    }
                    let label = format!("writer {writer_mode:?}, trivial move {trivial_move}");
                    assert_matches_model(Target::Db(&db), &model, &label).await;

                    flush_memtable(&db).await;
                    for mode in MODES {
                        let reader = DbReader::builder(MODEL_PATH, store.clone())
                            .with_options(DbReaderOptions {
                                metadata_pinning: mode,
                                ..DbReaderOptions::default()
                            })
                            .build()
                            .await
                            .expect("failed to build reader");
                        let label = format!("reader {mode:?}, {label}");
                        assert_matches_model(Target::Reader(&reader), &model, &label).await;
                        reader.close().await.expect("reader close failed");
                    }
                    db.close().await.expect("close failed");
                });
                Ok(())
            },
        )
        .expect("a pinning mode disagreed with the model");
}
