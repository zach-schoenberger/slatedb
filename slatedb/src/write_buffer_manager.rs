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
        if buffer_size == 0 {
            return WriteBufferPermit {
                permits: AtomicUsize::new(0),
                semaphore: Arc::clone(&self.buffer_permits),
            };
        }

        Arc::clone(&self.buffer_permits)
            .acquire_permit(buffer_size)
            .await
    }

    /// Unconditionally acquires `buffer_size` bytes without waiting.
    /// The bytes are tracked by the budget (so `available()` reflects them)
    /// but the call never blocks, even if the budget is fully exhausted.
    ///
    /// Use this for paths like WAL replay where the data is already in
    /// memory and must be accounted for, but blocking would deadlock
    /// because forward progress is needed to free the budget.
    pub fn force_acquire_buffer(&self, buffer_size: usize) -> WriteBufferPermit {
        self.buffer_permits.force_acquire(buffer_size);
        WriteBufferPermit {
            permits: AtomicUsize::new(buffer_size),
            semaphore: Arc::clone(&self.buffer_permits),
        }
    }

    pub fn available(&self) -> usize {
        self.buffer_permits.available()
    }
}

#[derive(Debug)]
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
    pub fn merge(&self, other: &Self) {
        assert!(
            Arc::ptr_eq(&self.semaphore, &other.semaphore),
            "merging permits from different semaphore instances"
        );

        let mut allocated = other.permits.load(Ordering::Relaxed);
        loop {
            match other.permits.compare_exchange_weak(
                allocated,
                0,
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
        }

        self.permits.fetch_add(allocated, Ordering::Relaxed);
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

#[derive(Debug)]
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

    /// Unconditionally adds `permits` to the allocated count without
    /// waiting. This can push `allocated` above `max_permits`.
    fn force_acquire(&self, permits: usize) {
        self.permits.fetch_add(permits, Ordering::Relaxed);
    }

    fn release(&self, permits: usize) {
        let mut allocated = self.permits.load(Ordering::Relaxed);
        loop {
            assert!(
                allocated >= permits,
                "cannot release more permits than were requested"
            );
            match self.permits.compare_exchange_weak(
                allocated,
                allocated - permits,
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
        }

        if (allocated - permits) < self.max_permits {
            self.notify.notify_waiters();
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

        p1.merge(&p2);
        assert_eq!(p1.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    async fn test_merge_drops_release_combined() {
        let mgr = WriteBufferManager::new(1024);
        let p1 = mgr.acquire_buffer(100).await;
        let p2 = mgr.acquire_buffer(200).await;

        p1.merge(&p2);
        drop(p2);
        drop(p1);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_merge_other_drops_without_releasing() {
        let mgr = WriteBufferManager::new(1024);
        let p1 = mgr.acquire_buffer(100).await;
        let p2 = mgr.acquire_buffer(200).await;

        // After merge, dropping the consumed permit should not double-release.
        p1.merge(&p2);
        // p2's permits are zeroed; dropping it won't release anything.
        drop(p2);
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

        p1.merge(&p2);
    }

    // ---------------------------------------------------------------
    // Drop
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_drop_zero_sized_permit_is_safe() {
        let mgr = WriteBufferManager::new(1024);
        let p1 = mgr.acquire_buffer(100).await;
        let p2 = mgr.acquire_buffer(100).await;

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
        let mgr = WriteBufferManager::new(1024);
        let p1 = mgr.acquire_buffer(100).await;
        let p2 = mgr.acquire_buffer(200).await;
        let p3 = mgr.acquire_buffer(300).await;

        p1.merge(&p2);
        p1.merge(&p3);
        assert_eq!(p1.size(), 600);

        drop(p2);
        drop(p3);
        drop(p1);
        assert_eq!(mgr.available(), 1024);
    }
}
