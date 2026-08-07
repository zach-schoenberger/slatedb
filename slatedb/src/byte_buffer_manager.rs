use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use log::{info, warn};
use tokio::sync::Notify;

use crate::utils::format_bytes_si;

/// Tracks and enforces a global memory budget for in-flight write data.
///
/// Writers must acquire a [`ByteBufferPermit`] before submitting a batch.
/// The permit is attached to the active memtable and released when that
/// memtable is dropped after being flushed to L0, thereby freeing the
/// budget for new writes.
#[derive(Clone)]
pub struct ByteBufferManager {
    inner: Arc<ByteBudgetSemaphore>,
    pub(crate) high_watermark: usize,
}

impl ByteBufferManager {
    /// Creates a new write-buffer manager with the given byte budget.
    pub fn new(capacity: usize, high_watermark: usize) -> Self {
        Self {
            inner: Arc::new(ByteBudgetSemaphore::new(capacity)),
            high_watermark,
        }
    }

    /// Creates an unbounded write-buffer manager that never applies backpressure.
    ///
    /// Use this for read-only paths (e.g. WAL replay, empty sentinel tables) where
    /// memory accounting is unnecessary but the API requires a `ByteBufferManager`.
    pub fn unbounded() -> Self {
        Self::new(usize::MAX, usize::MAX)
    }

    /// Unconditionally reserves `num_bytes` without waiting.
    /// The bytes are tracked by the budget (so `available()` reflects them)
    /// but the call never blocks, even if the budget is fully exhausted.
    ///
    /// Use this for paths like WAL replay where the data is already in
    /// memory and must be accounted for, but blocking would deadlock
    /// because forward progress is needed to free the budget.
    pub fn force_acquire(&self, num_bytes: usize) -> ByteBufferPermit {
        self.inner.force_acquire(num_bytes);
        ByteBufferPermit {
            reserved_bytes: AtomicUsize::new(num_bytes),
            semaphore: Arc::clone(&self.inner),
        }
    }

    /// Reserves `num_bytes`, blocking while allocated bytes are at or above the
    /// high watermark. Once below, the bytes are reserved atomically (no TOCTOU
    /// gap), and the returned permit releases them on drop.
    ///
    /// `on_block` is invoked immediately before each park while the reservation
    /// waits, receiving `true` only on the first park. It is *not* called when
    /// the bytes are reserved without waiting, so callers can act on backpressure
    /// only when a write actually has to wait.
    pub async fn acquire(&self, num_bytes: usize, on_block: impl Fn(bool)) -> ByteBufferPermit {
        let high_watermark = self.high_watermark;
        let semaphore = &self.inner;
        let blocked = semaphore
            .acquire(num_bytes, high_watermark, |first| {
                if first {
                    warn!(
                        "write buffer at capacity; blocking write [allocated={}, high_watermark={}, requested={}]",
                        format_bytes_si(semaphore.allocated() as u64),
                        format_bytes_si(high_watermark as u64),
                        format_bytes_si(num_bytes as u64),
                    );
                }
                on_block(first);
            })
            .await;
        if blocked {
            info!(
                "write buffer drained; write unblocked [allocated={}]",
                format_bytes_si(semaphore.allocated() as u64),
            );
        }
        ByteBufferPermit {
            reserved_bytes: AtomicUsize::new(num_bytes),
            semaphore: Arc::clone(&self.inner),
        }
    }

    pub fn force_expand(&self, permit: &ByteBufferPermit, num_bytes: usize) {
        self.inner.force_acquire(num_bytes);
        permit
            .reserved_bytes
            .fetch_add(num_bytes, Ordering::Relaxed);
    }

    /// Returns the number of unreserved bytes remaining in the budget.
    pub fn available(&self) -> usize {
        self.inner.available()
    }

    /// Returns the total byte budget capacity.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Returns the total number of bytes currently allocated (outstanding).
    pub fn allocated(&self) -> usize {
        self.inner.allocated()
    }

    /// Returns `true` if allocated bytes have reached or exceeded the high
    /// watermark, indicating that writers should apply backpressure.
    pub fn at_capacity(&self) -> bool {
        self.inner.allocated() >= self.high_watermark
    }

    /// Waits until `allocated_bytes` drops below the high watermark.
    ///
    /// This does **not** reserve any bytes — it only waits for the condition
    /// to be met and then returns. Because no reservation is made, the
    /// caller must be prepared for `allocated_bytes` to climb back above
    /// the high watermark immediately after this future resolves (TOCTOU).
    ///
    /// Use this for backpressure signaling where you want to wait until
    /// memory pressure has eased without holding budget during the wait.
    pub async fn await_capacity(&self) {
        self.inner
            .wait_for_allocated_below(self.high_watermark)
            .await;
    }
}

/// An RAII guard representing a reserved portion of the write-buffer budget.
///
/// Dropping the permit returns its reserved bytes to the parent
/// [`ByteBufferManager`]. Multiple permits can be consolidated via
/// [`merge`](Self::merge) so that a single drop releases the combined
/// reservation.
#[derive(Debug)]
pub struct ByteBufferPermit {
    semaphore: Arc<ByteBudgetSemaphore>,
    reserved_bytes: AtomicUsize,
}

impl ByteBufferPermit {
    /// Returns the number of bytes currently reserved by this permit.
    pub fn size(&self) -> usize {
        self.reserved_bytes.load(Ordering::Relaxed)
    }

    /// Merges another permit into this one, consuming `other` without
    /// releasing its tracked bytes back to the buffer budget. The combined
    /// byte budget is released when `self` is dropped.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` were acquired from different
    /// `ByteBufferManager` instances.
    pub fn merge(&self, other: &Self) {
        assert!(
            Arc::ptr_eq(&self.semaphore, &other.semaphore),
            "merging permits from different semaphore instances"
        );

        let mut other_bytes = other.reserved_bytes.load(Ordering::Relaxed);
        loop {
            match other.reserved_bytes.compare_exchange_weak(
                other_bytes,
                0,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    break;
                }
                Err(cur) => {
                    other_bytes = cur;
                }
            }
        }

        self.reserved_bytes
            .fetch_add(other_bytes, Ordering::Relaxed);
    }

    /// Splits `num_bytes` off this permit into a new permit. If fewer than
    /// `num_bytes` remain, takes only what is available (never underflows).
    pub fn take(&self, num_bytes: usize) -> Self {
        let taken = loop {
            let current = self.reserved_bytes.load(Ordering::Relaxed);
            let take = current.min(num_bytes);
            match self.reserved_bytes.compare_exchange_weak(
                current,
                current - take,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break take,
                Err(_) => continue,
            }
        };
        Self {
            reserved_bytes: AtomicUsize::new(taken),
            semaphore: self.semaphore.clone(),
        }
    }

    // pub fn force_acquire(&self, num_bytes: usize) {
    //     self.reserved_bytes.fetch_add(num_bytes, Ordering::Relaxed);
    //     self.semaphore.force_acquire(num_bytes);
    // }
}

impl Drop for ByteBufferPermit {
    fn drop(&mut self) {
        let reserved = self.reserved_bytes.load(Ordering::Relaxed);
        if reserved > 0 {
            self.semaphore.release(reserved);
        }
    }
}

/// Decrements `waiter_cnt` when dropped, ensuring cancellation safety.
struct WaiterGuard<'a> {
    semaphore: &'a ByteBudgetSemaphore,
}

impl<'a> WaiterGuard<'a> {
    fn new(semaphore: &'a ByteBudgetSemaphore) -> Self {
        semaphore.waiter_cnt.fetch_add(1, Ordering::Release);
        Self { semaphore }
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.semaphore.waiter_cnt.fetch_sub(1, Ordering::Release);
    }
}

#[derive(Debug)]
struct ByteBudgetSemaphore {
    notify: Notify,
    allocated_bytes: AtomicUsize,
    waiter_cnt: AtomicUsize,
    capacity: usize,
}

impl ByteBudgetSemaphore {
    /// Creates a new semaphore with the given total byte capacity.
    fn new(capacity: usize) -> Self {
        Self {
            notify: Notify::new(),
            allocated_bytes: AtomicUsize::new(0),
            waiter_cnt: AtomicUsize::new(0),
            capacity,
        }
    }

    /// Unconditionally adds `num_bytes` to the allocated count without
    /// waiting. This can push `allocated_bytes` above `capacity`.
    fn force_acquire(&self, num_bytes: usize) {
        self.allocated_bytes.fetch_add(num_bytes, Ordering::Release);
    }

    /// Releases `num_bytes` back to the budget and wakes any blocked
    /// acquirers or capacity waiters.
    ///
    /// Waiters may be parked on the high watermark (not `capacity`), so every
    /// release with outstanding waiters must notify — gating on `capacity`
    /// alone can miss wakes when `high_watermark != capacity`.
    ///
    /// # Panics
    ///
    /// Panics if `num_bytes` exceeds the currently allocated count.
    fn release(&self, num_bytes: usize) {
        let prev = self.allocated_bytes.fetch_sub(num_bytes, Ordering::AcqRel);
        assert!(
            prev >= num_bytes,
            "cannot release more bytes than were reserved"
        );

        if self.waiter_cnt.load(Ordering::Acquire) > 0 {
            self.notify.notify_waiters();
        }
    }

    /// Returns the number of unreserved bytes (capacity minus allocated),
    /// clamped to zero when over-allocated via `force_acquire`.
    fn available(&self) -> usize {
        let current = self.allocated_bytes.load(Ordering::Acquire);
        self.capacity.saturating_sub(current)
    }

    /// Returns the total number of bytes currently allocated.
    fn allocated(&self) -> usize {
        self.allocated_bytes.load(Ordering::Acquire)
    }

    /// Blocks until allocated bytes are below `watermark`, then atomically
    /// reserves `num_bytes`. Reserving before returning closes the TOCTOU gap
    /// that `wait_for_allocated_below` leaves open. May push allocated above
    /// `watermark` (each caller reserves its full request once below).
    ///
    /// `on_block` fires immediately before *every* park while the reservation
    /// waits, receiving `true` only on the first park. Firing on each park lets
    /// callers re-assert relief (e.g. re-request a flush) rather than relying on
    /// a single edge-triggered signal. It never fires when the bytes are
    /// reserved without waiting. Returns `true` if it parked at least once.
    async fn acquire(&self, num_bytes: usize, watermark: usize, on_block: impl Fn(bool)) -> bool {
        let mut allocated = self.allocated_bytes.load(Ordering::Acquire);

        // Fast path: reserve without parking when already below the watermark.
        while allocated < watermark {
            match self.allocated_bytes.compare_exchange_weak(
                allocated,
                allocated + num_bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return false,
                Err(cur) => allocated = cur,
            }
        }

        // Slow path: park until a release drops allocated below the watermark,
        // re-checking (with enable-before-check ordering) to avoid lost wakeups.
        // `on_block` fires each time we are truly about to wait, so backpressure
        // relief is re-asserted on every park, not just the first.
        let _guard = WaiterGuard::new(self);
        let notify_fut = self.notify.notified();
        tokio::pin!(notify_fut);
        let mut parked = false;
        loop {
            notify_fut.as_mut().enable();
            allocated = self.allocated_bytes.load(Ordering::Acquire);
            if allocated < watermark {
                match self.allocated_bytes.compare_exchange_weak(
                    allocated,
                    allocated + num_bytes,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return parked,
                    Err(_) => continue,
                }
            }
            on_block(!parked);
            parked = true;
            notify_fut.as_mut().await;
            notify_fut.set(self.notify.notified());
        }
    }

    /// Blocks until allocated bytes drop below `num_bytes`. Does not reserve
    /// any capacity — callers must handle TOCTOU races.
    async fn wait_for_allocated_below(&self, num_bytes: usize) {
        if self.allocated_bytes.load(Ordering::Acquire) < num_bytes {
            return;
        }

        let _guard = WaiterGuard::new(self);

        // Enable-before-check (same pattern as `acquire`) so a release that
        // lands between the initial load and `enable` cannot be missed.
        let notify_fut = self.notify.notified();
        tokio::pin!(notify_fut);
        loop {
            notify_fut.as_mut().enable();
            if self.allocated_bytes.load(Ordering::Acquire) < num_bytes {
                break;
            }
            notify_fut.as_mut().await;
            notify_fut.set(self.notify.notified());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    // ---------------------------------------------------------------
    // ByteBufferManager tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_new_manager_has_full_budget() {
        let mgr = ByteBufferManager::new(1024, 0);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_force_acquire_reduces_available() {
        let mgr = ByteBufferManager::new(1024, 0);
        let _permit = mgr.force_acquire(100);
        assert_eq!(mgr.available(), 924);
    }

    #[tokio::test]
    async fn test_force_acquire_entire_budget() {
        let mgr = ByteBufferManager::new(256, 0);
        let permit = mgr.force_acquire(256);
        assert_eq!(mgr.available(), 0);
        assert_eq!(permit.size(), 256);
    }

    #[tokio::test]
    async fn test_drop_permit_restores_budget() {
        let mgr = ByteBufferManager::new(1024, 0);
        let permit = mgr.force_acquire(300);
        assert_eq!(mgr.available(), 724);
        drop(permit);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_multiple_force_acquires() {
        let mgr = ByteBufferManager::new(1024, 0);
        let p1 = mgr.force_acquire(200);
        let p2 = mgr.force_acquire(300);
        assert_eq!(mgr.available(), 524);
        assert_eq!(p1.size(), 200);
        assert_eq!(p2.size(), 300);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::size
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_permit_size() {
        let mgr = ByteBufferManager::new(1024, 0);
        let permit = mgr.force_acquire(42);
        assert_eq!(permit.size(), 42);
    }

    #[tokio::test]
    async fn test_take_splits_bytes() {
        let mgr = ByteBufferManager::new(1024, 0);
        let permit = mgr.force_acquire(100);
        let taken = permit.take(40);
        assert_eq!(taken.size(), 40);
        assert_eq!(permit.size(), 60);
        drop(taken);
        assert_eq!(mgr.allocated(), 60);
        drop(permit);
        assert_eq!(mgr.allocated(), 0);
    }

    #[tokio::test]
    async fn test_take_saturates_when_request_exceeds_reserved() {
        let mgr = ByteBufferManager::new(1024, 0);
        let permit = mgr.force_acquire(50);
        let taken = permit.take(100);
        assert_eq!(taken.size(), 50);
        assert_eq!(permit.size(), 0);
        drop(taken);
        drop(permit);
        assert_eq!(mgr.allocated(), 0);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::merge
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_merge_combines_sizes() {
        let mgr = ByteBufferManager::new(1024, 0);
        let p1 = mgr.force_acquire(100);
        let p2 = mgr.force_acquire(200);

        p1.merge(&p2);
        assert_eq!(p1.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    async fn test_merge_drops_release_combined() {
        let mgr = ByteBufferManager::new(1024, 0);
        let p1 = mgr.force_acquire(100);
        let p2 = mgr.force_acquire(200);

        p1.merge(&p2);
        drop(p2);
        drop(p1);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_merge_other_drops_without_releasing() {
        let mgr = ByteBufferManager::new(1024, 0);
        let p1 = mgr.force_acquire(100);
        let p2 = mgr.force_acquire(200);

        // After merge, dropping the consumed permit should not double-release.
        p1.merge(&p2);
        // p2's reserved_bytes are zeroed; dropping it won't release anything.
        drop(p2);
        assert_eq!(p1.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    #[should_panic(expected = "merging permits from different semaphore instances")]
    async fn test_merge_different_managers_panics() {
        let mgr1 = ByteBufferManager::new(1024, 0);
        let mgr2 = ByteBufferManager::new(1024, 0);
        let p1 = mgr1.force_acquire(10);
        let p2 = mgr2.force_acquire(10);

        p1.merge(&p2);
    }

    // ---------------------------------------------------------------
    // Drop
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_drop_zero_sized_permit_is_safe() {
        let mgr = ByteBufferManager::new(1024, 0);
        let p1 = mgr.force_acquire(100);
        let p2 = mgr.force_acquire(100);

        // Merge p1 into p2, zeroing p1.
        p2.merge(&p1);
        assert_eq!(p1.size(), 0);

        // Dropping a zeroed permit should not affect the budget.
        drop(p1);
        assert_eq!(mgr.available(), 824);

        drop(p2);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_drop_after_merge_releases_all() {
        let mgr = ByteBufferManager::new(1024, 0);
        let p1 = mgr.force_acquire(100);
        let p2 = mgr.force_acquire(200);
        let p3 = mgr.force_acquire(300);

        p1.merge(&p2);
        p1.merge(&p3);
        assert_eq!(p1.size(), 600);

        drop(p2);
        drop(p3);
        drop(p1);
        assert_eq!(mgr.available(), 1024);
    }

    // ---------------------------------------------------------------
    // acquire tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_acquire_reserves_when_below_watermark() {
        let mgr = ByteBufferManager::new(1024, 500);
        let permit = mgr.acquire(100, |_| {}).await;
        assert_eq!(permit.size(), 100);
        assert_eq!(mgr.allocated(), 100);
    }

    #[tokio::test]
    async fn test_acquire_reserves_atomically() {
        // Once below the watermark, acquire reserves before returning, so
        // allocated reflects the reservation immediately (no TOCTOU gap).
        let mgr = ByteBufferManager::new(1024, 200);
        let _p = mgr.force_acquire(100);
        let permit = mgr.acquire(50, |_| {}).await;
        assert_eq!(mgr.allocated(), 150);
        assert_eq!(permit.size(), 50);
    }

    #[tokio::test]
    async fn test_acquire_blocks_when_at_watermark() {
        let mgr = ByteBufferManager::new(1024, 500);
        let _permit = mgr.force_acquire(500);

        // allocated=500, high_watermark=500 => should block (not strictly below).
        let result = timeout(Duration::from_millis(50), mgr.acquire(10, |_| {})).await;
        assert!(result.is_err(), "acquire should have blocked");
    }

    #[tokio::test]
    async fn test_acquire_unblocks_after_release() {
        let mgr = ByteBufferManager::new(1024, 500);
        let permit = mgr.force_acquire(600);

        let mgr_clone = mgr.clone();
        let handle = tokio::spawn(async move { mgr_clone.acquire(50, |_| {}).await });

        // Give the spawned task a moment to park.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Release enough to drop below the high watermark.
        drop(permit);

        let acquired = timeout(Duration::from_millis(100), handle)
            .await
            .expect("acquire should have completed")
            .expect("task should not panic");
        assert_eq!(acquired.size(), 50);
    }

    #[tokio::test]
    async fn test_acquire_does_not_invoke_on_block_when_reserved_immediately() {
        let mgr = ByteBufferManager::new(1024, 500);
        let called = Arc::new(AtomicUsize::new(0));
        let called_clone = Arc::clone(&called);

        // allocated=0 < watermark=500 => reserves without parking, so the
        // backpressure callback must not fire.
        let _permit = mgr
            .acquire(100, move |_| {
                called_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await;
        assert_eq!(called.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_acquire_invokes_on_block_once_per_park() {
        // A reservation that parks exactly once invokes on_block exactly once
        // (with first=true) and does not fire again on the wake-up that reserves.
        let mgr = ByteBufferManager::new(1024, 500);
        let permit = mgr.force_acquire(600);
        let called = Arc::new(AtomicUsize::new(0));
        let first_flags = Arc::new(std::sync::Mutex::new(Vec::new()));

        let mgr_clone = mgr.clone();
        let called_clone = Arc::clone(&called);
        let first_flags_clone = Arc::clone(&first_flags);
        let handle = tokio::spawn(async move {
            mgr_clone
                .acquire(50, move |first| {
                    called_clone.fetch_add(1, Ordering::Relaxed);
                    first_flags_clone.lock().unwrap().push(first);
                })
                .await
        });

        // Let the task park and observe backpressure.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(called.load(Ordering::Relaxed), 1);
        assert_eq!(first_flags.lock().unwrap().as_slice(), &[true]);

        // Unblock; the single-park case does not fire again on wake-up.
        drop(permit);
        timeout(Duration::from_millis(100), handle)
            .await
            .expect("acquire should have completed")
            .expect("task should not panic");
        assert_eq!(called.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_acquire_invokes_on_block_each_park_when_reparked() {
        // A reservation that must park more than once invokes on_block before
        // every park, with first=true only on the first. This is what lets a
        // blocked writer re-request relief on each park rather than once.
        let mgr = ByteBufferManager::new(1024, 500);
        let big = mgr.force_acquire(500);
        let small = mgr.force_acquire(100); // allocated = 600, at capacity
        let first_flags = Arc::new(std::sync::Mutex::new(Vec::new()));

        let mgr_clone = mgr.clone();
        let first_flags_clone = Arc::clone(&first_flags);
        let handle = tokio::spawn(async move {
            mgr_clone
                .acquire(10, move |first| {
                    first_flags_clone.lock().unwrap().push(first);
                })
                .await
        });

        // First park.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(first_flags.lock().unwrap().as_slice(), &[true]);

        // Drop only the small permit: allocated 600 -> 500, still >= watermark,
        // but below capacity so waiters are notified. The waiter wakes, still
        // cannot reserve, and parks again -> on_block fires with first=false.
        drop(small);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(first_flags.lock().unwrap().as_slice(), &[true, false]);

        // Drop the rest: allocated -> 0, the waiter reserves and returns.
        drop(big);
        timeout(Duration::from_millis(100), handle)
            .await
            .expect("acquire should have completed")
            .expect("task should not panic");
    }

    // ---------------------------------------------------------------
    // await_capacity tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_await_capacity_returns_immediately_when_below() {
        let mgr = ByteBufferManager::new(1024, 200);
        let _permit = mgr.force_acquire(100);

        // allocated=100, high_watermark=200 => should return immediately
        let result = timeout(Duration::from_millis(50), mgr.await_capacity()).await;
        assert!(result.is_ok(), "should not have timed out");
    }

    #[tokio::test]
    async fn test_await_capacity_returns_immediately_when_zero() {
        let mgr = ByteBufferManager::new(1024, 1);

        // allocated=0, high_watermark=1 => should return immediately
        let result = timeout(Duration::from_millis(50), mgr.await_capacity()).await;
        assert!(result.is_ok(), "should not have timed out");
    }

    #[tokio::test]
    async fn test_await_capacity_blocks_when_at_threshold() {
        let mgr = ByteBufferManager::new(1024, 500);
        let _permit = mgr.force_acquire(500);

        // allocated=500, high_watermark=500 => should block (not strictly below)
        let result = timeout(Duration::from_millis(50), mgr.await_capacity()).await;
        assert!(result.is_err(), "should have timed out");
    }

    #[tokio::test]
    async fn test_await_capacity_blocks_when_above() {
        let mgr = ByteBufferManager::new(1024, 500);
        let _permit = mgr.force_acquire(600);

        // allocated=600, high_watermark=500 => should block
        let result = timeout(Duration::from_millis(50), mgr.await_capacity()).await;
        assert!(result.is_err(), "should have timed out");
    }

    #[tokio::test]
    async fn test_await_capacity_unblocks_after_release() {
        let mgr = ByteBufferManager::new(1024, 500);
        let permit = mgr.force_acquire(600);

        let mgr_clone = mgr.clone();
        let handle = tokio::spawn(async move {
            mgr_clone.await_capacity().await;
        });

        // Give the spawned task a moment to park.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Release enough to drop below the high watermark.
        drop(permit);

        let result = timeout(Duration::from_millis(100), handle).await;
        assert!(result.is_ok(), "await_capacity should have completed");
    }

    #[tokio::test]
    async fn test_await_capacity_observes_release_before_park() {
        // Regression: wait_for_allocated_below must re-read allocated after
        // enable(). Otherwise a release between the initial load and park is
        // missed (no waiter was registered yet, so release does not notify)
        // and the waiter hangs forever.
        for _ in 0..200 {
            let mgr = ByteBufferManager::new(1024, 500);
            let permit = mgr.force_acquire(600);
            let mgr_clone = mgr.clone();
            let handle = tokio::spawn(async move {
                mgr_clone.await_capacity().await;
            });
            drop(permit);
            timeout(Duration::from_millis(200), handle)
                .await
                .expect("await_capacity missed a wakeup")
                .expect("task should not panic");
        }
    }

    #[tokio::test]
    async fn test_await_capacity_does_not_reserve_bytes() {
        let mgr = ByteBufferManager::new(1024, 200);
        let _permit = mgr.force_acquire(100);

        mgr.await_capacity().await;

        // After wait returns, available should be unchanged (no reservation made).
        assert_eq!(mgr.available(), 924);
    }
}
