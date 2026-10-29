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
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
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
/// subtracts from them when it is dropped. An empty pin counts nowhere.
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
    /// Whether this pin is counted in `pinned_sst_count`, which happens
    /// when the first of its cells is filled.
    counted: AtomicBool,
}

impl PinnedSstMetadata {
    fn new(level: PinLevel, stats: Arc<PinStats>) -> Self {
        Self {
            level,
            stats,
            filters: OnceLock::new(),
            index: OnceLock::new(),
            counted: AtomicBool::new(false),
        }
    }

    fn count_once(&self) {
        if !self.counted.swap(true, Ordering::Relaxed) {
            self.stats.level(self.level).ssts.increment(1);
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
        if self.filters.set(filters).is_ok() {
            let bytes = self.filters.get().map_or(0, |f| filters_size(f));
            self.stats.level(self.level).filter_bytes.increment(bytes);
            self.count_once();
        }
    }

    /// Stores `index` if the cell is empty.
    pub(crate) fn set_index(&self, index: Arc<SsTableIndexOwned>) {
        if self.index.set(index).is_ok() {
            let bytes = self.index.get().map_or(0, |i| i.size() as i64);
            self.stats.level(self.level).index_bytes.increment(bytes);
            self.count_once();
        }
    }
}

fn filters_size(filters: &[NamedFilter]) -> i64 {
    filters.iter().map(|named| named.filter.size() as i64).sum()
}

impl Drop for PinnedSstMetadata {
    fn drop(&mut self) {
        let stats = self.stats.level(self.level);
        if *self.counted.get_mut() {
            stats.ssts.increment(-1);
        }
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
    /// `prev` or elsewhere in `next`, or a fresh pin when the mode selects the
    /// SST's level. An SST therefore has one pin however many views of it
    /// exist. A handle that already has a pin, such as an L0 view the writer
    /// cloned from its own state, is left alone.
    ///
    /// L0 views are visited before sorted-run views, so an SST that has views
    /// in both gets an L0 pin whatever the order of the trees.
    ///
    /// A trivial move puts an L0 SST into a sorted run without rewriting it.
    /// Such an SST keeps the pin it had in L0, in every mode that gave it one.
    ///
    /// The cost is one pass over the SSTs of `prev` and two over `next`. A
    /// sorted run is rebuilt only when one of its handles gains a pin, so in
    /// `L0` mode a run is rebuilt only when it holds a trivially moved SST.
    /// Nothing outlives the call.
    pub(crate) fn carry_pins(&self, prev: Option<&ManifestCore>, next: &mut ManifestCore) {
        if self.mode == MetadataPinning::None {
            return;
        }
        // One pin per SST id. A pin already on a handle in `next` is the one
        // later views of that SST share.
        let mut pins: HashMap<SsTableId, Arc<PinnedSstMetadata>> = HashMap::new();
        for core in prev.into_iter().chain(std::iter::once(&*next)) {
            for view in core.trees().flat_map(LsmTreeState::sst_views) {
                if let Some(pin) = view.sst.pinned() {
                    pins.insert(view.sst.id, Arc::clone(pin));
                }
            }
        }
        for tree in next.trees_mut() {
            for view in tree.l0.iter_mut() {
                self.attach(&mut pins, &mut view.sst, PinLevel::L0);
            }
        }
        for tree in next.trees_mut() {
            for run in tree.compacted.iter_mut() {
                // Rebuilding a run copies every view, so skip a run that
                // gains no pin.
                if !run
                    .sst_views()
                    .iter()
                    .any(|view| self.gains_pin(&pins, &view.sst, PinLevel::SortedRun))
                {
                    continue;
                }
                let views: Vec<_> = run
                    .sst_views()
                    .iter()
                    .cloned()
                    .map(|mut view| {
                        self.attach(&mut pins, &mut view.sst, PinLevel::SortedRun);
                        view
                    })
                    .collect();
                *run = SortedRun::new(run.id, views);
            }
        }
    }

    fn gains_pin(
        &self,
        pins: &HashMap<SsTableId, Arc<PinnedSstMetadata>>,
        handle: &SsTableHandle,
        level: PinLevel,
    ) -> bool {
        handle.pinned().is_none() && (self.selects(level) || pins.contains_key(&handle.id))
    }

    fn attach(
        &self,
        pins: &mut HashMap<SsTableId, Arc<PinnedSstMetadata>>,
        handle: &mut SsTableHandle,
        level: PinLevel,
    ) {
        if handle.pinned().is_some() {
            return;
        }
        let pin = match pins.entry(handle.id) {
            Entry::Occupied(entry) => Some(Arc::clone(entry.get())),
            Entry::Vacant(entry) => self.new_pin(level).map(|pin| Arc::clone(entry.insert(pin))),
        };
        handle.set_pinned(pin);
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

    /// Fills the filters of every pin in `core`.
    fn fill(core: &ManifestCore) {
        for view in core.trees().flat_map(LsmTreeState::sst_views) {
            if let Some(pin) = view.sst.pinned() {
                pin.set_filters(Arc::from([]));
            }
        }
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
            // Empty pins are not counted.
            assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), 0, "{mode:?}");
            assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 0, "{mode:?}");

            fill(&next);

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
        fill(&prev);
        let prev_l0_pin = Arc::clone(prev.tree.l0[0].sst.pinned().unwrap());

        // SST 1 stays live and moves to a sorted run; SST 2 is retired; SST 3 is new.
        let mut next = core(vec![view(3)], vec![vec![view(1)]]);
        pinner.carry_pins(Some(&prev), &mut next);

        let carried = next.tree.compacted[0].sst_views()[0].sst.pinned().unwrap();
        assert!(Arc::ptr_eq(carried, &prev_l0_pin));
        assert!(carried.filters().is_some());
        assert!(next.tree.l0[0].sst.pinned().unwrap().filters().is_none());
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), 1);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 1);

        drop(prev);
        drop(prev_l0_pin);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), 1);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 0);

        fill(&next);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), 2);
        drop(next);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), 0);
    }

    #[test]
    fn carry_pins_gives_one_pin_per_sst() {
        for mode in [MetadataPinning::L0, MetadataPinning::All] {
            let (pinner, recorder) = pinner(mode);
            // SST 5 has a view in L0 and in two runs, as projections allow.
            // All three views share one pin, counted under L0.
            let mut next = core(vec![view(5)], vec![vec![view(5)], vec![view(5), view(6)]]);

            pinner.carry_pins(None, &mut next);

            let l0_pin = next.tree.l0[0].sst.pinned().unwrap();
            let shared = next
                .tree
                .compacted
                .iter()
                .flat_map(|run| run.sst_views().iter())
                .filter(|v| v.sst.id == next.tree.l0[0].sst.id)
                .map(|v| v.sst.pinned().unwrap());
            for pin in shared {
                assert!(Arc::ptr_eq(pin, l0_pin), "{mode:?}");
            }
            l0_pin.set_filters(Arc::from([]));
            assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_L0), 1, "{mode:?}");
            assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 0, "{mode:?}");
        }
    }

    #[test]
    fn carry_pins_shares_a_pin_already_in_next() {
        let (pinner, _) = pinner(MetadataPinning::All);
        let mut existing = view(1);
        let pin = pinner.new_pin(PinLevel::L0).unwrap();
        existing.sst.set_pinned(Some(Arc::clone(&pin)));
        // The L0 view is pinned already; the run's view of the same SST is not.
        let mut next = core(vec![existing], vec![vec![view(1)]]);

        pinner.carry_pins(None, &mut next);

        let run_pin = next.tree.compacted[0].sst_views()[0].sst.pinned().unwrap();
        assert!(Arc::ptr_eq(run_pin, &pin));
    }

    #[test]
    fn pinned_sst_count_counts_a_pin_once_when_first_filled() {
        let (pinner, recorder) = pinner(MetadataPinning::All);
        let pin = pinner.new_pin(PinLevel::SortedRun).unwrap();
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 0);

        pin.set_filters(Arc::from([]));
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 1);
        pin.set_filters(Arc::from([]));
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 1);

        drop(pin);
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 0);

        // An empty pin leaves nothing to subtract.
        drop(pinner.new_pin(PinLevel::SortedRun));
        assert_eq!(pinned_ssts(&recorder, PIN_LEVEL_SORTED_RUN), 0);
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
    fn carry_pins_rebuilds_only_runs_that_gain_a_pin() {
        let (pinner, _) = pinner(MetadataPinning::L0);
        let mut prev = core(vec![view(1)], vec![]);
        pinner.carry_pins(None, &mut prev);

        // Run 0 holds no pinned SST. Run 1 holds SST 1, trivially moved.
        let mut next = core(vec![], vec![vec![view(2), view(3)], vec![view(1)]]);
        let untouched = next.tree.compacted[0].sst_views().as_ptr();
        let rebuilt = next.tree.compacted[1].sst_views().as_ptr();
        pinner.carry_pins(Some(&prev), &mut next);

        assert_eq!(next.tree.compacted[0].sst_views().as_ptr(), untouched);
        assert_ne!(next.tree.compacted[1].sst_views().as_ptr(), rebuilt);
        assert_eq!(run_pins(&next), [false, false, true]);
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
