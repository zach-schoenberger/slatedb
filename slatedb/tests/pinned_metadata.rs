//! Integration test for pinned SST filters and indexes (RFC 0037).
//!
//! Every pinning mode must return the same results and record the same
//! filter verdicts. With pinning on, point lookups read no filter or index
//! from object storage once the pin is filled.

use std::sync::Arc;

use bytes::Bytes;
use slatedb::config::{FlushOptions, FlushType, Settings};
use slatedb::db_stats::{
    FILTER_KIND_LABEL, FILTER_KIND_POINT, PINNED_SST_COUNT, PIN_LEVEL_L0, PIN_LEVEL_LABEL,
    SST_FILTER_NEGATIVE_COUNT, SST_FILTER_POSITIVE_COUNT,
};
use slatedb::instrumented_object_store_stats::REQUEST_COUNT as OBJECT_STORE_REQUEST_COUNT;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, MetadataPinning};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};

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
