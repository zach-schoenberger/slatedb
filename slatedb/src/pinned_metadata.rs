//! Pinned SST filters and indexes (RFC 0037).
//!
//! A pinned SST keeps its decoded filters and index on its
//! [`SsTableHandle`], outside the block cache, for as long as the SST is part
//! of the database state. The pin is filled only from data that is already in
//! memory: the writer fills it when it flushes an L0 SST, and a read fills it
//! the first time it reads the metadata of a pinned SST. No read is issued
//! only to pin.
//!
//! [`MetadataPinner`] decides which SSTs get a pin and carries pins across
//! manifest installs, where handles are decoded again from the manifest.

use crate::config::MetadataPinning;
use crate::db_state::{SortedRun, SsTableHandle, SsTableId};
use crate::db_stats::{
    PINNED_SST_COUNT, PINNED_SST_METADATA_BYTES, PIN_COMPONENT_FILTER, PIN_COMPONENT_INDEX,
    PIN_COMPONENT_LABEL, PIN_LEVEL_L0, PIN_LEVEL_LABEL, PIN_LEVEL_SORTED_RUN,
};
use crate::filter_policy::NamedFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::manifest::{LsmTreeState, ManifestCore};
use slatedb_common::metrics::{MetricsRecorderHelper, UpDownCounterFn};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock};

/// The level an SST was in when it received its pin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PinLevel {
    L0,
    SortedRun,
}

struct LevelStats {
    ssts: Arc<dyn UpDownCounterFn>,
    filter_bytes: Arc<dyn UpDownCounterFn>,
    index_bytes: Arc<dyn UpDownCounterFn>,
}

impl LevelStats {
    fn new(recorder: &MetricsRecorderHelper, level: &'static str) -> Self {
        Self {
            ssts: recorder
                .up_down_counter(PINNED_SST_COUNT)
                .labels(&[(PIN_LEVEL_LABEL, level)])
                .register(),
            filter_bytes: recorder
                .up_down_counter(PINNED_SST_METADATA_BYTES)
                .labels(&[
                    (PIN_LEVEL_LABEL, level),
                    (PIN_COMPONENT_LABEL, PIN_COMPONENT_FILTER),
                ])
                .register(),
            index_bytes: recorder
                .up_down_counter(PINNED_SST_METADATA_BYTES)
                .labels(&[
                    (PIN_LEVEL_LABEL, level),
                    (PIN_COMPONENT_LABEL, PIN_COMPONENT_INDEX),
                ])
                .register(),
        }
    }
}

/// Counters for pinned metadata. Each pin adds to them when it is filled and
/// subtracts from them when it is dropped.
pub(crate) struct PinStats {
    l0: LevelStats,
    sorted_run: LevelStats,
}

impl PinStats {
    pub(crate) fn new(recorder: &MetricsRecorderHelper) -> Self {
        Self {
            l0: LevelStats::new(recorder, PIN_LEVEL_L0),
            sorted_run: LevelStats::new(recorder, PIN_LEVEL_SORTED_RUN),
        }
    }

    fn level(&self, level: PinLevel) -> &LevelStats {
        match level {
            PinLevel::L0 => &self.l0,
            PinLevel::SortedRun => &self.sorted_run,
        }
    }
}

/// The pinned filters and index of one SST.
///
/// Each cell is written once, by whichever path first holds the value in
/// memory. A later write is a no-op. No path waits on a cell.
pub(crate) struct PinnedSstMetadata {
    level: PinLevel,
    stats: Arc<PinStats>,
    filters: OnceLock<Arc<[NamedFilter]>>,
    index: OnceLock<Arc<SsTableIndexOwned>>,
}

impl PinnedSstMetadata {
    fn new(level: PinLevel, stats: Arc<PinStats>) -> Self {
        stats.level(level).ssts.increment(1);
        Self {
            level,
            stats,
            filters: OnceLock::new(),
            index: OnceLock::new(),
        }
    }

    pub(crate) fn filters(&self) -> Option<Arc<[NamedFilter]>> {
        self.filters.get().cloned()
    }

    pub(crate) fn index(&self) -> Option<Arc<SsTableIndexOwned>> {
        self.index.get().cloned()
    }

    /// Stores `filters` if the cell is empty.
    pub(crate) fn set_filters(&self, filters: Arc<[NamedFilter]>) {
        let bytes = filters_size(&filters);
        if self.filters.set(filters).is_ok() {
            self.stats.level(self.level).filter_bytes.increment(bytes);
        }
    }

    /// Stores `index` if the cell is empty.
    pub(crate) fn set_index(&self, index: Arc<SsTableIndexOwned>) {
        let bytes = index.size() as i64;
        if self.index.set(index).is_ok() {
            self.stats.level(self.level).index_bytes.increment(bytes);
        }
    }
}

fn filters_size(filters: &[NamedFilter]) -> i64 {
    filters.iter().map(|named| named.filter.size() as i64).sum()
}

impl Drop for PinnedSstMetadata {
    fn drop(&mut self) {
        let stats = self.stats.level(self.level);
        stats.ssts.increment(-1);
        if let Some(filters) = self.filters.get() {
            stats.filter_bytes.increment(-filters_size(filters));
        }
        if let Some(index) = self.index.get() {
            stats.index_bytes.increment(-(index.size() as i64));
        }
    }
}

impl Debug for PinnedSstMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedSstMetadata")
            .field("level", &self.level)
            .field("filters", &self.filters.get().is_some())
            .field("index", &self.index.get().is_some())
            .finish()
    }
}

/// Decides which SSTs get a pin and carries pins across manifest installs.
///
/// One pinner belongs to each `TableStore`. The compactor and the garbage
/// collector use a disabled pinner, because they never read through the
/// block cache.
#[derive(Clone)]
pub(crate) struct MetadataPinner {
    mode: MetadataPinning,
    stats: Arc<PinStats>,
}

impl MetadataPinner {
    pub(crate) fn new(mode: MetadataPinning, recorder: &MetricsRecorderHelper) -> Self {
        Self {
            mode,
            stats: Arc::new(PinStats::new(recorder)),
        }
    }

    /// A pinner that pins nothing.
    pub(crate) fn disabled() -> Self {
        Self::new(MetadataPinning::None, &MetricsRecorderHelper::noop())
    }

    /// Whether the mode pins SSTs that enter the state at `level`.
    pub(crate) fn selects(&self, level: PinLevel) -> bool {
        match self.mode {
            MetadataPinning::None => false,
            MetadataPinning::L0 => level == PinLevel::L0,
            MetadataPinning::All => true,
        }
    }

    /// A fresh, empty pin for an SST that enters the state at `level`, or
    /// `None` when the mode does not pin that level.
    pub(crate) fn new_pin(&self, level: PinLevel) -> Option<Arc<PinnedSstMetadata>> {
        self.selects(level)
            .then(|| Arc::new(PinnedSstMetadata::new(level, Arc::clone(&self.stats))))
    }

    /// Gives every handle in `next` its pin.
    ///
    /// Handles decoded from a manifest have no pin. For each handle in `next`
    /// that has none, this installs the pin that the same SST holds in
    /// `prev`, or a fresh pin when the mode selects the SST's level. A handle
    /// that already has a pin, such as an L0 view the writer cloned from its
    /// own state, is left alone.
    ///
    /// A trivial move puts an L0 SST into a sorted run without rewriting it.
    /// Such an SST keeps the pin it had in L0, in every mode that gave it one.
    ///
    /// The cost is one pass over the SSTs of `prev` and of `next`. Nothing
    /// outlives the call.
    pub(crate) fn carry_pins(&self, prev: Option<&ManifestCore>, next: &mut ManifestCore) {
        if self.mode == MetadataPinning::None {
            return;
        }
        let prior: HashMap<SsTableId, Arc<PinnedSstMetadata>> = prev
            .map(|prev| {
                prev.trees()
                    .flat_map(LsmTreeState::sst_views)
                    .filter_map(|view| view.sst.pinned().map(|pin| (view.sst.id, Arc::clone(pin))))
                    .collect()
            })
            .unwrap_or_default();
        let attach = |handle: &mut SsTableHandle, level: PinLevel| {
            if handle.pinned().is_some() {
                return;
            }
            let pin = match prior.get(&handle.id) {
                Some(pin) => Some(Arc::clone(pin)),
                None => self.new_pin(level),
            };
            handle.set_pinned(pin);
        };
        let pin_sorted_runs = self.selects(PinLevel::SortedRun) || !prior.is_empty();
        for tree in next.trees_mut() {
            for view in tree.l0.iter_mut() {
                attach(&mut view.sst, PinLevel::L0);
            }
            if !pin_sorted_runs {
                continue;
            }
            for run in tree.compacted.iter_mut() {
                let views = run.sst_views().iter().cloned().map(|mut view| {
                    attach(&mut view.sst, PinLevel::SortedRun);
                    view
                });
                *run = SortedRun::new(run.id, views);
            }
        }
    }
}

impl Debug for MetadataPinner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataPinner")
            .field("mode", &self.mode)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::{SsTableInfo, SsTableView};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use slatedb_common::metrics::{DefaultMetricsRecorder, MetricLevel, MetricValue};

    fn view(id: u64) -> SsTableView {
        let handle = SsTableHandle::new(
            SsTableId::from(ulid::Ulid::from_parts(id, 0)),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo::default(),
        );
        SsTableView::identity(handle)
    }

    fn core(l0: Vec<SsTableView>, runs: Vec<Vec<SsTableView>>) -> ManifestCore {
        let mut core = new_dirty_manifest().value.core;
        let tree = Arc::make_mut(&mut core.tree);
        tree.l0 = l0.into_iter().collect();
        tree.compacted = runs
            .into_iter()
            .enumerate()
            .map(|(id, views)| SortedRun::new(id as u32, views))
            .collect();
        core
    }

    fn pinner(mode: MetadataPinning) -> (MetadataPinner, Arc<DefaultMetricsRecorder>) {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), MetricLevel::default());
        (MetadataPinner::new(mode, &helper), recorder)
    }

    fn pinned_ssts(recorder: &DefaultMetricsRecorder, level: &str) -> i64 {
        recorder
            .snapshot()
            .by_name_and_labels(PINNED_SST_COUNT, &[(PIN_LEVEL_LABEL, level)])
            .map(|m| match m.value {
                MetricValue::UpDownCounter(v) => v,
                ref other => panic!("expected up-down counter, got {other:?}"),
            })
            .unwrap_or(0)
    }

    fn l0_pins(core: &ManifestCore) -> Vec<bool> {
        core.tree
            .l0
            .iter()
            .map(|v| v.sst.pinned().is_some())
            .collect()
    }

    fn run_pins(core: &ManifestCore) -> Vec<bool> {
        core.tree
            .compacted
            .iter()
            .flat_map(|run| run.sst_views().iter())
            .map(|v| v.sst.pinned().is_some())
            .collect()
    }

    #[test]
    fn carry_pins_gives_fresh_pins_only_to_selected_levels() {
        for (mode, l0_expected, run_expected, l0_count, run_count) in [
            (MetadataPinning::None, [false, false], [false, false], 0, 0),
            (MetadataPinning::L0, [true, true], [false, false], 2, 0),
            (MetadataPinning::All, [true, true], [true, true], 2, 2),
        ] {
            let (pinner, recorder) = pinner(mode);
            let mut next = core(vec![view(1), view(2)], vec![vec![view(3), view(4)]]);

            pinner.carry_pins(None, &mut next);

            assert_eq!(l0_pins(&next), l0_expected, "{mode:?}");
            assert_eq!(run_pins(&next), run_expected, "{mode:?}");
            assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), l0_count, "{mode:?}");
            assert_eq!(
                pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN),
                run_count,
                "{mode:?}"
            );
        }
    }

    #[test]
    fn carry_pins_carries_pins_by_sst_id_and_frees_retired_pins() {
        let (pinner, recorder) = pinner(MetadataPinning::All);
        let mut prev = core(vec![view(1)], vec![vec![view(2)]]);
        pinner.carry_pins(None, &mut prev);
        let prev_l0_pin = Arc::clone(prev.tree.l0[0].sst.pinned().unwrap());
        prev_l0_pin.set_filters(Arc::from([]));

        // SST 1 stays live and moves to a sorted run; SST 2 is retired; SST 3 is new.
        let mut next = core(vec![view(3)], vec![vec![view(1)]]);
        pinner.carry_pins(Some(&prev), &mut next);

        let carried = next.tree.compacted[0].sst_views()[0].sst.pinned().unwrap();
        assert!(Arc::ptr_eq(carried, &prev_l0_pin));
        assert!(carried.filters().is_some());
        assert!(next.tree.l0[0].sst.pinned().unwrap().filters().is_none());
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), 2);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 1);

        drop(prev);
        drop(prev_l0_pin);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), 2);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 0);

        drop(next);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), 0);
    }

    #[test]
    fn carry_pins_keeps_l0_pin_after_trivial_move_in_l0_mode() {
        let (pinner, _) = pinner(MetadataPinning::L0);
        let mut prev = core(vec![view(1)], vec![]);
        pinner.carry_pins(None, &mut prev);

        let mut next = core(vec![], vec![vec![view(1), view(2)]]);
        pinner.carry_pins(Some(&prev), &mut next);

        assert_eq!(run_pins(&next), [true, false]);
    }

    #[test]
    fn carry_pins_leaves_existing_pins_alone() {
        let (pinner, _) = pinner(MetadataPinning::All);
        let mut existing = view(1);
        let pin = pinner.new_pin(PinLevel::L0).unwrap();
        existing.sst.set_pinned(Some(Arc::clone(&pin)));
        let mut next = core(vec![existing], vec![]);

        pinner.carry_pins(None, &mut next);

        assert!(Arc::ptr_eq(next.tree.l0[0].sst.pinned().unwrap(), &pin));
    }

    #[test]
    fn disabled_pinner_pins_nothing() {
        let pinner = MetadataPinner::disabled();
        assert!(pinner.new_pin(PinLevel::L0).is_none());
        assert!(pinner.new_pin(PinLevel::SortedRun).is_none());
    }
}
