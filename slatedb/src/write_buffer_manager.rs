use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use tokio::sync::Notify;

#[derive(Clone)]
pub struct WriteBufferManager {
    buffer_permits: Arc<WBMSemaphore>,
}

impl WriteBufferManager {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer_permits: Arc::new(WBMSemaphore::new(buffer_size)),
        }
    }

    pub async fn acquire_buffer(&self, buffer_size: usize) -> WriteBufferPermit {
        // acquire_many_owned translates back to usize. i do not know why they used u32 here.
        self.buffer_permits
            .clone()
            .acquire_permit(buffer_size)
            .await
    }

    pub fn available(&self) -> usize {
        self.buffer_permits.available()
    }
}

pub struct WriteBufferPermit {
    semaphore: Arc<WBMSemaphore>,
    permits: AtomicUsize,
}

impl WriteBufferPermit {
    pub fn size(&self) -> usize {
        self.permits.load(Ordering::Relaxed)
    }

    /// Merges another permit into this one, consuming `other` without
    /// releasing its tracked bytes back to the buffer budget. The combined
    /// byte budget is released when `self` is dropped.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` were acquired from different
    /// `WriteBufferManager` instances.
    pub fn merge(&self, other: Self) {
        assert!(
            Arc::ptr_eq(&self.semaphore, &other.semaphore),
            "merging permits from different semaphore instances"
        );
        self.permits
            .fetch_add(other.permits.load(Ordering::Relaxed), Ordering::Relaxed);
        other.permits.store(0, Ordering::Relaxed);
    }

    /// Releases `n` bytes back to the buffer budget, shrinking this permit.
    /// After this call, `self.size()` is reduced by `n`.
    ///
    /// # Panics
    ///
    /// Panics if `n` is greater than the number of bytes currently held.
    pub fn release(&self, n: usize) {
        let size = self.size();
        assert!(
            n <= size,
            "cannot release {n} bytes when only {size} are held"
        );
        self.semaphore.release(n);
        self.permits.fetch_sub(n, Ordering::Relaxed);
    }

    /// Acquires `n` additional bytes from the buffer budget, growing this
    /// permit. Waits asynchronously until enough budget is available.
    /// After this call, `self.size()` is increased by `n`.
    pub async fn acquire(&self, n: usize) {
        self.semaphore.acquire(n).await;
        self.permits.fetch_add(n, Ordering::Relaxed);
    }

    /// Resizes this permit to track exactly `new` bytes, acquiring or
    /// releasing bytes as needed. If `new` equals the current size,
    /// this is a no-op.
    ///
    /// When growing, waits asynchronously until enough buffer budget is
    /// available. When shrinking, bytes are released back to the budget
    /// immediately.
    pub async fn resize(&self, new: usize) {
        let size = self.size();
        match new.cmp(&size) {
            std::cmp::Ordering::Less => self.release(size - new),
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => self.acquire(new - size).await,
        }
    }
}

impl Drop for WriteBufferPermit {
    fn drop(&mut self) {
        let permits = self.permits.load(Ordering::Relaxed);
        if permits > 0 {
            self.semaphore.release(permits);
        }
    }
}

struct WBMSemaphore {
    notify: Notify,
    permits: AtomicUsize,
    max_permits: usize,
}

impl WBMSemaphore {
    fn new(permits: usize) -> Self {
        Self {
            notify: Notify::new(),
            permits: AtomicUsize::new(0),
            max_permits: permits,
        }
    }

    async fn acquire(&self, permits: usize) {
        let mut allocated = self.permits.load(Ordering::Relaxed);
        let notify_fut = self.notify.notified();
        tokio::pin!(notify_fut);
        notify_fut.as_mut().enable();

        loop {
            if allocated < self.max_permits {
                match self.permits.compare_exchange_weak(
                    allocated,
                    allocated + permits,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        break;
                    }
                    Err(cur) => {
                        allocated = cur;
                    }
                }
            } else {
                notify_fut.as_mut().await;
                notify_fut.set(self.notify.notified());
                allocated = self.permits.load(Ordering::Relaxed);
            }
        }
    }

    fn release(&self, permits: usize) {
        let allocated = self.permits.fetch_sub(permits, Ordering::Relaxed);
        if (allocated - permits) < self.max_permits {
            self.notify.notify_one();
        }
    }

    fn available(&self) -> usize {
        let allocated = self.permits.load(Ordering::Relaxed);
        if allocated < self.max_permits {
            self.max_permits - allocated
        } else {
            0
        }
    }

    async fn acquire_permit(self: Arc<Self>, permits: usize) -> WriteBufferPermit {
        self.acquire(permits).await;
        WriteBufferPermit {
            permits: AtomicUsize::new(permits),
            semaphore: self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    // ---------------------------------------------------------------
    // WriteBufferManager tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_new_manager_has_full_budget() {
        let mgr = WriteBufferManager::new(1024);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_acquire_reduces_available() {
        let mgr = WriteBufferManager::new(1024);
        let _permit = mgr.acquire_buffer(100).await;
        assert_eq!(mgr.available(), 924);
    }

    #[tokio::test]
    async fn test_acquire_entire_budget() {
        let mgr = WriteBufferManager::new(256);
        let permit = mgr.acquire_buffer(256).await;
        assert_eq!(mgr.available(), 0);
        assert_eq!(permit.size(), 256);
    }

    #[tokio::test]
    async fn test_drop_permit_restores_budget() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(300).await;
        assert_eq!(mgr.available(), 724);
        drop(permit);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_multiple_acquires() {
        let mgr = WriteBufferManager::new(1024);
        let p1 = mgr.acquire_buffer(200).await;
        let p2 = mgr.acquire_buffer(300).await;
        assert_eq!(mgr.available(), 524);
        assert_eq!(p1.size(), 200);
        assert_eq!(p2.size(), 300);
    }

    #[tokio::test]
    async fn test_acquire_blocks_when_budget_exhausted() {
        let mgr = WriteBufferManager::new(100);
        let _permit = mgr.acquire_buffer(100).await;

        // A second acquire should block because the budget is exhausted.
        let result = timeout(Duration::from_millis(50), mgr.acquire_buffer(1)).await;
        assert!(result.is_err(), "acquire should have timed out");
    }

    #[tokio::test]
    async fn test_acquire_unblocks_after_drop() {
        let mgr = WriteBufferManager::new(100);
        let permit = mgr.acquire_buffer(100).await;

        let mgr_clone = mgr.clone();
        let handle = tokio::spawn(async move { mgr_clone.acquire_buffer(50).await });

        // Give the spawned task a moment to park on the semaphore.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Free the budget.
        drop(permit);

        let p2 = timeout(Duration::from_millis(100), handle)
            .await
            .expect("should have completed")
            .expect("task should not panic");
        assert_eq!(p2.size(), 50);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::size
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_permit_size() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(42).await;
        assert_eq!(permit.size(), 42);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::merge
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_merge_combines_sizes() {
        let mgr = WriteBufferManager::new(1024);
        let p1 = mgr.acquire_buffer(100).await;
        let p2 = mgr.acquire_buffer(200).await;

        p1.merge(p2);
        assert_eq!(p1.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    async fn test_merge_drops_release_combined() {
        let mgr = WriteBufferManager::new(1024);
        let p1 = mgr.acquire_buffer(100).await;
        let p2 = mgr.acquire_buffer(200).await;

        p1.merge(p2);
        drop(p1);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_merge_other_drops_without_releasing() {
        let mgr = WriteBufferManager::new(1024);
        let p1 = mgr.acquire_buffer(100).await;
        let p2 = mgr.acquire_buffer(200).await;

        // After merge, dropping the consumed permit should not double-release.
        p1.merge(p2);
        // p2 was consumed by merge; only p1 exists. Its size should be 300.
        assert_eq!(p1.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    #[should_panic(expected = "merging permits from different semaphore instances")]
    async fn test_merge_different_managers_panics() {
        let mgr1 = WriteBufferManager::new(1024);
        let mgr2 = WriteBufferManager::new(1024);
        let p1 = mgr1.acquire_buffer(10).await;
        let p2 = mgr2.acquire_buffer(10).await;

        p1.merge(p2);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::release
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_release_returns_bytes_to_budget() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(500).await;

        permit.release(200);
        assert_eq!(permit.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    async fn test_release_all_bytes() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(500).await;

        permit.release(500);
        assert_eq!(permit.size(), 0);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_release_zero_is_noop() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(500).await;

        permit.release(0);
        assert_eq!(permit.size(), 500);
        assert_eq!(mgr.available(), 524);
    }

    #[tokio::test]
    #[should_panic(expected = "cannot release 600 bytes when only 500 are held")]
    async fn test_release_more_than_held_panics() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(500).await;

        permit.release(600);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::acquire
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_acquire_grows_permit() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(100).await;

        permit.acquire(200).await;
        assert_eq!(permit.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    async fn test_acquire_blocks_until_budget_available() {
        let mgr = WriteBufferManager::new(100);
        let p1 = mgr.acquire_buffer(80).await;
        let p2 = mgr.acquire_buffer(20).await;

        // p2 wants 50 more but only 0 are available — should block.
        let p2_ref = &p2;
        let result = timeout(Duration::from_millis(50), p2_ref.acquire(50)).await;
        assert!(result.is_err(), "acquire should have timed out");

        // Release some bytes from p1.
        p1.release(50);

        // Now the acquire should succeed.
        timeout(Duration::from_millis(100), p2.acquire(50))
            .await
            .expect("acquire should have completed");
        assert_eq!(p2.size(), 70);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::resize
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_resize_grow() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(100).await;

        permit.resize(400).await;
        assert_eq!(permit.size(), 400);
        assert_eq!(mgr.available(), 624);
    }

    #[tokio::test]
    async fn test_resize_shrink() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(400).await;

        permit.resize(100).await;
        assert_eq!(permit.size(), 100);
        assert_eq!(mgr.available(), 924);
    }

    #[tokio::test]
    async fn test_resize_same_size_is_noop() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(400).await;

        permit.resize(400).await;
        assert_eq!(permit.size(), 400);
        assert_eq!(mgr.available(), 624);
    }

    #[tokio::test]
    async fn test_resize_to_zero() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(400).await;

        permit.resize(0).await;
        assert_eq!(permit.size(), 0);
        assert_eq!(mgr.available(), 1024);
    }

    // ---------------------------------------------------------------
    // Drop
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_drop_zero_sized_permit_is_safe() {
        let mgr = WriteBufferManager::new(1024);
        let permit = mgr.acquire_buffer(100).await;
        permit.release(100);
        assert_eq!(permit.size(), 0);
        drop(permit);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_drop_after_merge_releases_all() {
        let mgr = WriteBufferManager::new(1024);
        let p1 = mgr.acquire_buffer(100).await;
        let p2 = mgr.acquire_buffer(200).await;
        let p3 = mgr.acquire_buffer(300).await;

        p1.merge(p2);
        p1.merge(p3);
        assert_eq!(p1.size(), 600);

        drop(p1);
        assert_eq!(mgr.available(), 1024);
    }
}
