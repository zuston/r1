use crate::metric::{GAUGE_MEMORY_ALLOCATED, GAUGE_MEMORY_CAPACITY, GAUGE_MEMORY_USED};
use crate::store::mem::capacity::CapacitySnapshot;
use anyhow::Result;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Clone)]
pub struct MemoryBudget {
    capacity: i64,
    inner: Arc<RwLock<BudgetInner>>,
}

#[derive(Default)]
struct BudgetInner {
    allocated: i64,
    used: i64,
    allocation_inc_counter: i64,
}

impl MemoryBudget {
    pub(crate) fn new(capacity: i64) -> MemoryBudget {
        GAUGE_MEMORY_CAPACITY.set(capacity);
        MemoryBudget {
            capacity,
            inner: Default::default(),
        }
    }

    pub fn snapshot(&self) -> CapacitySnapshot {
        let capacity = self.capacity;
        let inner = self.inner.read();
        let allocated = inner.allocated;
        let used = inner.used;
        drop(inner);
        (capacity, allocated, used).into()
    }

    pub fn require_allocated(&self, size: i64) -> Result<(bool, i64)> {
        let capacity = self.capacity;

        let mut inner = self.inner.write();
        let allocated = inner.allocated;
        let used = inner.used;

        let remaining = capacity - allocated - used;
        if remaining < size {
            Ok((false, -1))
        } else {
            inner.allocated += size;
            GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
            inner.allocation_inc_counter += 1;
            Ok((true, inner.allocation_inc_counter))
        }
    }

    pub fn move_allocated_to_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.write();
        let allocated = inner.allocated;

        let mut desc = size;
        if allocated < size {
            desc = allocated;
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        inner.used += desc;
        GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
        GAUGE_MEMORY_USED.set(inner.used);
        Ok(true)
    }

    pub fn dec_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.write();
        if inner.used < size {
            inner.used = 0;
        } else {
            inner.used -= size;
        }
        GAUGE_MEMORY_USED.set(inner.used);
        Ok(true)
    }

    pub fn dec_allocated(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.write();
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use crate::metric::{GAUGE_MEMORY_ALLOCATED, GAUGE_MEMORY_USED};
    use crate::store::mem::budget::MemoryBudget;

    #[test]
    fn basic() -> anyhow::Result<()> {
        let memory_budget = MemoryBudget::new(100);

        /// case1: reject the overflow allocation size
        let (succeed, id) = memory_budget.require_allocated(120)?;
        assert!(!succeed);

        /// case2: pass the legal allocation size
        let (succeed, _) = memory_budget.require_allocated(50)?;
        assert!(succeed);

        let snapshot = memory_budget.snapshot();
        assert_eq!(100, snapshot.capacity());
        assert_eq!(0, snapshot.used());
        assert_eq!(50, snapshot.allocated());

        /// case3: allocation to used
        memory_budget.move_allocated_to_used(50)?;

        let snapshot = memory_budget.snapshot();
        assert_eq!(100, snapshot.capacity());
        assert_eq!(50, snapshot.used());
        assert_eq!(0, snapshot.allocated());

        /// case4: release the used
        memory_budget.dec_used(50)?;
        let snapshot = memory_budget.snapshot();
        assert_eq!(100, snapshot.capacity());
        assert_eq!(0, snapshot.used());
        assert_eq!(0, snapshot.allocated());

        /// case5: check the metrics
        assert_eq!(0, GAUGE_MEMORY_ALLOCATED.get());
        assert_eq!(0, GAUGE_MEMORY_USED.get());

        Ok(())
    }
}
