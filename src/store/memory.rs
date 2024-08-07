// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::app::ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE;
use crate::app::{
    PartitionedUId, PurgeDataContext, ReadingIndexViewContext, ReadingViewContext,
    RegisterAppContext, ReleaseBufferContext, RequireBufferContext, WritingViewContext,
};
use crate::config::{MemoryStoreConfig, StorageType};
use crate::error::WorkerError;
use crate::metric::TOTAL_MEMORY_USED;
use crate::readable_size::ReadableSize;
use crate::store::{
    Block, DataSegment, PartitionedMemoryData, RequireBufferResponse, ResponseData,
    ResponseDataIndex, SpillWritingViewContext, Store,
};
use crate::*;
use async_trait::async_trait;
use bytes::BytesMut;
use dashmap::DashMap;

use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasherDefault;

use std::str::FromStr;

use crate::store::mem::budget::MemoryBudget;
use crate::store::mem::buffer::MemoryBuffer;
use crate::store::mem::capacity::CapacitySnapshot;
use crate::store::mem::ticket::TicketManager;
use croaring::Treemap;
use fxhash::{FxBuildHasher, FxHasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct MemoryStore {
    state: DashMap<PartitionedUId, Arc<MemoryBuffer>, BuildHasherDefault<FxHasher>>,
    budget: MemoryBudget,
    // key: app_id, value: allocated memory size
    memory_capacity: i64,
    in_flush_buffer_size: AtomicU64,
    runtime_manager: RuntimeManager,
    ticket_manager: TicketManager,
}

unsafe impl Send for MemoryStore {}
unsafe impl Sync for MemoryStore {}

impl MemoryStore {
    // only for test cases
    pub fn new(max_memory_size: i64) -> Self {
        let budget = MemoryBudget::new(max_memory_size);
        let runtime_manager: RuntimeManager = Default::default();

        let budget_clone = budget.clone();
        let release_allocated_func =
            move |size: i64| budget_clone.dec_allocated(size).map_or(false, |v| v);

        let ticket_manager =
            TicketManager::new(5 * 60, 10, release_allocated_func, runtime_manager.clone());
        MemoryStore {
            budget,
            state: DashMap::with_hasher(FxBuildHasher::default()),
            memory_capacity: max_memory_size,
            ticket_manager,
            in_flush_buffer_size: Default::default(),
            runtime_manager,
        }
    }

    pub fn from(conf: MemoryStoreConfig, runtime_manager: RuntimeManager) -> Self {
        let capacity = ReadableSize::from_str(&conf.capacity).unwrap();
        let budget = MemoryBudget::new(capacity.as_bytes() as i64);

        let budget_clone = budget.clone();
        let release_allocated_func =
            move |size: i64| budget_clone.dec_allocated(size).map_or(false, |v| v);

        let ticket_manager =
            TicketManager::new(5 * 60, 10, release_allocated_func, runtime_manager.clone());

        /// the dashmap shard that will effect the lookup performance.
        let shard_amount = conf.dashmap_shard_amount.unwrap_or(96);
        let dashmap = DashMap::with_hasher_and_shard_amount(FxBuildHasher::default(), shard_amount);

        MemoryStore {
            state: dashmap,
            budget: MemoryBudget::new(capacity.as_bytes() as i64),
            memory_capacity: capacity.as_bytes() as i64,
            ticket_manager,
            in_flush_buffer_size: Default::default(),
            runtime_manager,
        }
    }

    pub fn memory_snapshot(&self) -> Result<CapacitySnapshot> {
        Ok(self.budget.snapshot())
    }

    pub fn get_capacity(&self) -> Result<i64> {
        Ok(self.memory_capacity)
    }

    pub fn calculate_usage_ratio(&self) -> f32 {
        let snapshot = self.budget.snapshot();
        (snapshot.used() + snapshot.allocated()
            - self.in_flush_buffer_size.load(Ordering::SeqCst) as i64) as f32
            / snapshot.capacity() as f32
    }

    pub fn inc_inflight(&self, size: u64) {
        self.in_flush_buffer_size.fetch_add(size, Ordering::SeqCst);
    }

    pub fn dec_inflight(&self, size: u64) {
        self.in_flush_buffer_size.fetch_sub(size, Ordering::SeqCst);
    }

    pub fn dec_used(&self, size: i64) -> Result<bool> {
        self.budget.dec_used(size)
    }

    pub fn dec_allocated(&self, size: i64) -> Result<bool> {
        self.budget.dec_allocated(size)
    }

    pub fn calculate_spilled_blocks(
        &self,
        mem_target_len: i64,
    ) -> HashMap<PartitionedUId, Arc<MemoryBuffer>> {
        // 1. sort by the staging size.
        // 2. get the spill buffers until reaching the single max batch size

        let snapshot = self.budget.snapshot();
        let required_spilled_size = snapshot.used() - mem_target_len;
        if required_spilled_size <= 0 {
            return HashMap::new();
        }

        let mut sorted_tree_map = BTreeMap::new();

        let buffers = self.state.clone().into_read_only();
        for buffer in buffers.iter() {
            let key = buffer.0;
            let memory_buf = buffer.1;
            let staging_size = memory_buf.staging_size().unwrap();
            let valset = sorted_tree_map
                .entry(staging_size)
                .or_insert_with(|| vec![]);
            valset.push(key);
        }

        let mut spill_staging_size = 0;
        let mut spill_candidates = HashMap::new();

        let iter = sorted_tree_map.iter().rev();
        'outer: for (size, vals) in iter {
            for pid in vals {
                if spill_staging_size >= required_spilled_size {
                    break 'outer;
                }
                spill_staging_size += *size;
                let partition_uid = (*pid).clone();
                let buffer = self.get_underlying_partition_buffer(*pid);
                spill_candidates.insert(partition_uid, buffer);
            }
        }

        info!(
            "[Spill] expected spill size: {}, real: {}",
            &required_spilled_size, &spill_staging_size
        );
        spill_candidates
    }

    pub fn get_partitioned_buffer_size(&self, uid: &PartitionedUId) -> Result<u64> {
        let buffer = self.get_underlying_partition_buffer(uid);
        Ok(buffer.total_size()? as u64)
    }

    pub async fn clear_spilled_memory_buffer(
        &self,
        uid: PartitionedUId,
        flight_id: u64,
        flight_len: u64,
    ) -> Result<()> {
        let buffer = self.get_or_create_memory_buffer(uid);
        buffer.clear(flight_id, flight_len)?;
        Ok(())
    }

    pub fn get_or_create_memory_buffer(&self, uid: PartitionedUId) -> Arc<MemoryBuffer> {
        let buffer = self
            .state
            .entry(uid)
            .or_insert_with(|| Arc::new(MemoryBuffer::new()));
        buffer.clone()
    }

    fn get_underlying_partition_buffer(&self, pid: &PartitionedUId) -> Arc<MemoryBuffer> {
        self.state.get(pid).unwrap().clone()
    }

    pub(crate) fn read_partial_data_with_max_size_limit_and_filter<'a>(
        &'a self,
        blocks: Vec<&'a Block>,
        fetched_size_limit: i64,
        serialized_expected_task_ids_bitmap: Option<Treemap>,
    ) -> (Vec<&Block>, i64) {
        let mut fetched = vec![];
        let mut fetched_size = 0;

        for block in blocks {
            if let Some(ref filter) = serialized_expected_task_ids_bitmap {
                if !filter.contains(block.task_attempt_id as u64) {
                    continue;
                }
            }
            if fetched_size >= fetched_size_limit {
                break;
            }
            fetched_size += block.length as i64;
            fetched.push(block);
        }

        (fetched, fetched_size)
    }
}

#[async_trait]
impl Store for MemoryStore {
    fn start(self: Arc<Self>) {
        // ignore
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        let uid = ctx.uid;
        let blocks = ctx.data_blocks;
        let size = ctx.data_size;

        let buffer = self.get_or_create_memory_buffer(uid);
        buffer.append(blocks, ctx.data_size)?;

        self.budget.move_allocated_to_used(size as i64)?;
        TOTAL_MEMORY_USED.inc_by(size);

        Ok(())
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        let uid = ctx.uid;
        let buffer = self.get_or_create_memory_buffer(uid);
        let options = ctx.reading_options;
        let buffer_read_result = match options {
            MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(last_block_id, max_size) => buffer.get(
                last_block_id,
                max_size,
                ctx.serialized_expected_task_ids_bitmap,
            )?,
            _ => panic!("Should not happen."),
        };
        let size = buffer_read_result.read_len();
        let blocks = buffer_read_result.blocks();

        let mut bytes_holder = BytesMut::with_capacity(size as usize);
        let mut segments = vec![];
        let mut offset = 0;
        for block in blocks {
            let data = &block.data;
            bytes_holder.extend_from_slice(data);
            segments.push(DataSegment {
                block_id: block.block_id,
                offset,
                length: block.length,
                uncompress_length: block.uncompress_length,
                crc: block.crc,
                task_attempt_id: block.task_attempt_id,
            });
            offset += block.length as i64;
        }

        Ok(ResponseData::Mem(PartitionedMemoryData {
            shuffle_data_block_segments: segments,
            data: bytes_holder.freeze(),
        }))
    }

    async fn get_index(
        &self,
        _ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        panic!("It should not be invoked.")
    }

    async fn purge(&self, ctx: PurgeDataContext) -> Result<i64> {
        let app_id = ctx.app_id;
        let shuffle_id_option = ctx.shuffle_id;

        // remove the corresponding app's data
        let read_only_state_view = self.state.clone().into_read_only();
        let mut _removed_list = vec![];
        for entry in read_only_state_view.iter() {
            let pid = entry.0;
            if pid.app_id == app_id {
                if ctx.shuffle_id.is_some() {
                    if pid.shuffle_id == shuffle_id_option.unwrap() {
                        _removed_list.push(pid);
                    } else {
                        continue;
                    }
                } else {
                    _removed_list.push(pid);
                }
            }
        }

        let mut used = 0;
        for removed_pid in _removed_list {
            if let Some(entry) = self.state.remove(removed_pid) {
                used += entry.1.total_size()?;
            }
        }

        // free used
        self.budget.dec_used(used)?;

        info!(
            "removed used buffer size:[{}] for [{:?}], [{:?}]",
            used, app_id, shuffle_id_option
        );

        Ok(used)
    }

    async fn is_healthy(&self) -> Result<bool> {
        Ok(true)
    }

    async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        let (succeed, ticket_id) = self.budget.require_allocated(ctx.size)?;
        match succeed {
            true => {
                let require_buffer_resp = RequireBufferResponse::new(ticket_id);
                self.ticket_manager.insert(
                    ticket_id,
                    ctx.size,
                    require_buffer_resp.allocated_timestamp,
                    &ctx.uid.app_id,
                );
                Ok(require_buffer_resp)
            }
            _ => Err(WorkerError::NO_ENOUGH_MEMORY_TO_BE_ALLOCATED),
        }
    }

    async fn release_buffer(&self, ctx: ReleaseBufferContext) -> Result<i64, WorkerError> {
        let ticket_id = ctx.ticket_id;
        self.ticket_manager.delete(ticket_id)
    }

    async fn register_app(&self, _ctx: RegisterAppContext) -> Result<()> {
        Ok(())
    }

    async fn name(&self) -> StorageType {
        StorageType::MEMORY
    }

    async fn spill_insert(&self, _ctx: SpillWritingViewContext) -> Result<(), WorkerError> {
        todo!()
    }
}

pub struct MemorySnapshot {
    capacity: i64,
    allocated: i64,
    used: i64,
}

impl From<(i64, i64, i64)> for MemorySnapshot {
    fn from(value: (i64, i64, i64)) -> Self {
        MemorySnapshot {
            capacity: value.0,
            allocated: value.1,
            used: value.2,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::app::{
        PartitionedUId, PurgeDataContext, ReadingOptions, ReadingViewContext, RequireBufferContext,
        WritingViewContext,
    };

    use crate::store::memory::MemoryStore;
    use crate::store::ResponseData::Mem;

    use crate::store::{Block, PartitionedMemoryData, ResponseData, Store};

    use bytes::BytesMut;
    use core::panic;
    use std::sync::Arc;

    use anyhow::Result;
    use croaring::Treemap;

    #[test]
    fn test_read_buffer_in_flight() {
        let store = MemoryStore::new(1024);
        let runtime = store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "100".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        let writing_view_ctx = create_writing_ctx_with_blocks(10, 10, uid.clone());
        let _ = runtime.wait(store.insert(writing_view_ctx));

        let default_single_read_size = 20;

        // case1: read from -1
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            -1,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            0,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            1,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // case2: when the last_block_id doesn't exist, it should return the data like when last_block_id=-1
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            100,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            0,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            1,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // case3: read from 3
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            3,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            4,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            5,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // // case4: some data are in inflight blocks
        // let buffer = store.get_or_create_underlying_staging_buffer(uid.clone());
        // let owned = buffer.staging.to_owned();
        // buffer.staging.clear();
        // let mut idx = 0;
        // for block in owned {
        //     buffer.in_flight.insert(idx, vec![block]);
        //     idx += 1;
        // }
        // drop(buffer);
        //
        // // all data will be fetched from in_flight data
        // let mem_data = runtime.wait(get_data_with_last_block_id(
        //     default_single_read_size,
        //     3,
        //     &store,
        //     uid.clone(),
        // ));
        // assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        // assert_eq!(
        //     4,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(0)
        //         .unwrap()
        //         .block_id
        // );
        // assert_eq!(
        //     5,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(1)
        //         .unwrap()
        //         .block_id
        // );
        //
        // // case5: old data in in_flight and latest data in staging.
        // // read it from the block id 9, and read size of 30
        // let buffer = store.get_or_create_underlying_staging_buffer(uid.clone());
        // let mut buffer = buffer.lock();
        // buffer.staging.push(PartitionedDataBlock {
        //     block_id: 20,
        //     length: 10,
        //     uncompress_length: 0,
        //     crc: 0,
        //     data: BytesMut::with_capacity(10).freeze(),
        //     task_attempt_id: 0,
        // });
        // drop(buffer);
        //
        // let mem_data = runtime.wait(get_data_with_last_block_id(30, 7, &store, uid.clone()));
        // assert_eq!(3, mem_data.shuffle_data_block_segments.len());
        // assert_eq!(
        //     8,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(0)
        //         .unwrap()
        //         .block_id
        // );
        // assert_eq!(
        //     9,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(1)
        //         .unwrap()
        //         .block_id
        // );
        // assert_eq!(
        //     20,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(2)
        //         .unwrap()
        //         .block_id
        // );
        //
        // // case6: read the end to return empty result
        // let mem_data = runtime.wait(get_data_with_last_block_id(30, 20, &store, uid.clone()));
        // assert_eq!(0, mem_data.shuffle_data_block_segments.len());
    }

    async fn get_data_with_last_block_id(
        default_single_read_size: i64,
        last_block_id: i64,
        store: &MemoryStore,
        uid: PartitionedUId,
    ) -> PartitionedMemoryData {
        let ctx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                last_block_id,
                default_single_read_size,
            ),
            serialized_expected_task_ids_bitmap: Default::default(),
        };
        if let Ok(data) = store.get(ctx).await {
            match data {
                Mem(mem_data) => mem_data,
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    fn create_writing_ctx_with_blocks(
        _block_number: i32,
        single_block_size: i32,
        uid: PartitionedUId,
    ) -> WritingViewContext {
        let mut data_blocks = vec![];
        for idx in 0..=9 {
            data_blocks.push(Block {
                block_id: idx,
                length: single_block_size.clone(),
                uncompress_length: 0,
                crc: 0,
                data: BytesMut::with_capacity(single_block_size as usize).freeze(),
                task_attempt_id: 0,
            });
        }
        WritingViewContext::from(uid, data_blocks)
    }

    #[test]
    fn test_allocated_and_purge_for_memory() {
        let store = MemoryStore::new(1024 * 1024 * 1024);
        let runtime = store.runtime_manager.clone();

        let ctx = RequireBufferContext {
            uid: PartitionedUId {
                app_id: "100".to_string(),
                shuffle_id: 0,
                partition_id: 0,
            },
            size: 10000,
        };
        match runtime.default_runtime.block_on(store.require_buffer(ctx)) {
            Ok(_) => {
                let _ = runtime.default_runtime.block_on(store.purge("100".into()));
            }
            _ => panic!(),
        }

        let snapshot = store.budget.snapshot();
        assert_eq!(0, snapshot.used());
        assert_eq!(1024 * 1024 * 1024, snapshot.capacity());
    }

    #[test]
    fn test_purge() -> Result<()> {
        let store = MemoryStore::new(1024);
        let runtime = store.runtime_manager.clone();

        let app_id = "purge_app";
        let shuffle_id = 1;
        let partition = 1;

        let uid = PartitionedUId::from(app_id.to_string(), shuffle_id, partition);

        // the buffer requested

        let _buffer = runtime
            .wait(store.require_buffer(RequireBufferContext::new(uid.clone(), 40)))
            .expect("");

        let writing_ctx = WritingViewContext::from(
            uid.clone(),
            vec![Block {
                block_id: 0,
                length: 10,
                uncompress_length: 100,
                crc: 99,
                data: Default::default(),
                task_attempt_id: 0,
            }],
        );
        runtime.wait(store.insert(writing_ctx)).expect("");

        let reading_ctx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
            serialized_expected_task_ids_bitmap: Default::default(),
        };
        let data = runtime.wait(store.get(reading_ctx.clone())).expect("");
        assert_eq!(1, data.from_memory().shuffle_data_block_segments.len());

        // get weak reference to ensure purge can successfully free memory
        let weak_ref_before = store
            .state
            .get(&uid)
            .map(|entry| Arc::downgrade(&entry.value()));
        assert!(
            weak_ref_before.is_some(),
            "Failed to obtain weak reference before purge"
        );

        // partial purge for app's one shuffle data
        runtime
            .wait(store.purge(PurgeDataContext::new(app_id.to_string(), Some(shuffle_id))))
            .expect("");
        assert!(!store.state.contains_key(&PartitionedUId::from(
            app_id.to_string(),
            shuffle_id,
            partition
        )));

        // purge
        runtime.wait(store.purge(app_id.into())).expect("");
        assert!(
            weak_ref_before.clone().unwrap().upgrade().is_none(),
            "Arc should not exist after purge"
        );
        let snapshot = store.budget.snapshot();
        assert_eq!(snapshot.used(), 0);
        assert_eq!(snapshot.capacity(), 1024);
        let data = runtime.wait(store.get(reading_ctx.clone())).expect("");
        assert_eq!(0, data.from_memory().shuffle_data_block_segments.len());

        Ok(())
    }

    #[test]
    fn test_put_and_get_for_memory() {
        let store = MemoryStore::new(1024 * 1024 * 1024);
        let runtime = store.runtime_manager.clone();

        let writing_ctx = WritingViewContext::from(
            Default::default(),
            vec![
                Block {
                    block_id: 0,
                    length: 10,
                    uncompress_length: 100,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: 20,
                    uncompress_length: 200,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 1,
                },
            ],
        );
        runtime.wait(store.insert(writing_ctx)).unwrap();

        let reading_ctx = ReadingViewContext {
            uid: Default::default(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
            serialized_expected_task_ids_bitmap: Default::default(),
        };

        match runtime.wait(store.get(reading_ctx)).unwrap() {
            ResponseData::Mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 2);
                assert_eq!(data.shuffle_data_block_segments.get(0).unwrap().offset, 0);
                assert_eq!(data.shuffle_data_block_segments.get(1).unwrap().offset, 10);
            }
            _ => panic!("should not"),
        }
    }

    #[test]
    fn test_block_id_filter_for_memory() {
        let store = MemoryStore::new(1024 * 1024 * 1024);
        let runtime = store.runtime_manager.clone();

        // 1. insert 2 block
        let writing_ctx = WritingViewContext::from(
            Default::default(),
            vec![
                Block {
                    block_id: 0,
                    length: 10,
                    uncompress_length: 100,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: 20,
                    uncompress_length: 200,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 1,
                },
            ],
        );
        runtime.wait(store.insert(writing_ctx)).unwrap();

        // 2. block_ids_filter is empty, should return 2 blocks
        let mut reading_ctx = ReadingViewContext {
            uid: Default::default(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
            serialized_expected_task_ids_bitmap: Default::default(),
        };

        match runtime.wait(store.get(reading_ctx)).unwrap() {
            Mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 2);
            }
            _ => panic!("should not"),
        }

        // 3. set serialized_expected_task_ids_bitmap, and set last_block_id equals 1, should return 1 block
        let mut bitmap = Treemap::default();
        bitmap.add(1);
        reading_ctx = ReadingViewContext {
            uid: Default::default(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(0, 1000000),
            serialized_expected_task_ids_bitmap: Option::from(bitmap.clone()),
        };

        match runtime.wait(store.get(reading_ctx)).unwrap() {
            Mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 1);
                assert_eq!(data.shuffle_data_block_segments.get(0).unwrap().offset, 0);
                assert_eq!(
                    data.shuffle_data_block_segments
                        .get(0)
                        .unwrap()
                        .uncompress_length,
                    200
                );
            }
            _ => panic!("should not"),
        }
    }
}
