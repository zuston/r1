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

use crate::app::{
    PartitionedUId, PurgeDataContext, ReadingIndexViewContext, ReadingOptions, ReadingViewContext,
    RegisterAppContext, ReleaseTicketContext, RequireBufferContext, WritingViewContext,
};

use crate::config::{Config, HybridStoreConfig, StorageType};
use crate::error::WorkerError;
use crate::metric::{
    GAUGE_MEMORY_SPILL_TO_HDFS, GAUGE_MEMORY_SPILL_TO_LOCALFILE,
    MEMORY_BUFFER_SPILL_BATCH_SIZE_HISTOGRAM, TOTAL_MEMORY_BUFFER_SPILL_BYTE_SIZE,
    TOTAL_MEMORY_SPILL_TO_HDFS, TOTAL_MEMORY_SPILL_TO_LOCALFILE,
};
use crate::readable_size::ReadableSize;
#[cfg(feature = "hdfs")]
use crate::store::hdfs::HdfsStore;
use crate::store::localfile::LocalFileStore;
use crate::store::memory::MemoryStore;

use crate::store::{Persistent, RequireBufferResponse, ResponseData, ResponseDataIndex, Store};
use anyhow::{anyhow, Result};

use async_trait::async_trait;
use log::{debug, error, info, warn};
use prometheus::core::{Atomic, AtomicU64};
use std::any::Any;

use std::collections::VecDeque;
use std::ops::Deref;

use await_tree::InstrumentAwait;
use fastrace::future::FutureExt;
use fastrace::trace;
use std::str::FromStr;
use std::sync::Arc;

use crate::event_bus::EventBus;
use crate::runtime::manager::RuntimeManager;
use crate::store::mem::capacity::CapacitySnapshot;
use crate::store::spill::event_handler::SpillEventHandler;
use crate::store::spill::{SpillMessage, SpillWritingViewContext};
use tokio::sync::Mutex;
use tokio::time::Instant;

pub trait PersistentStore: Store + Persistent + Send + Sync {}
impl PersistentStore for LocalFileStore {}

#[cfg(feature = "hdfs")]
impl PersistentStore for HdfsStore {}

const DEFAULT_MEMORY_SPILL_MAX_CONCURRENCY: i32 = 20;

pub struct HybridStore {
    // Box<dyn Store> will build fail
    hot_store: Arc<MemoryStore>,

    warm_store: Option<Box<dyn PersistentStore>>,
    cold_store: Option<Box<dyn PersistentStore>>,

    config: HybridStoreConfig,

    memory_spill_lock: Mutex<()>,
    memory_spill_event_num: AtomicU64,

    memory_spill_to_cold_threshold_size: Option<u64>,
    memory_spill_max_concurrency: i32,

    runtime_manager: RuntimeManager,

    pub event_bus: EventBus<SpillMessage>,
}

unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}

impl HybridStore {
    pub fn from(config: Config, runtime_manager: RuntimeManager) -> Self {
        let store_type = &config.store_type;
        if !StorageType::contains_memory(&store_type) {
            panic!("Storage type must contains memory.");
        }

        let mut persistent_stores: VecDeque<Box<dyn PersistentStore>> = VecDeque::with_capacity(2);
        if StorageType::contains_localfile(&store_type) {
            let localfile_store =
                LocalFileStore::from(config.localfile_store.unwrap(), runtime_manager.clone());
            persistent_stores.push_back(Box::new(localfile_store));
        }

        if StorageType::contains_hdfs(&store_type) {
            #[cfg(not(feature = "hdfs"))]
            panic!("The binary is not compiled with feature of hdfs! So the storage type can't involve hdfs.");

            #[cfg(feature = "hdfs")]
            let hdfs_store = HdfsStore::from(config.hdfs_store.unwrap());
            #[cfg(feature = "hdfs")]
            persistent_stores.push_back(Box::new(hdfs_store));
        }

        let hybrid_conf = config.hybrid_store;
        let memory_spill_to_cold_threshold_size =
            match &hybrid_conf.memory_spill_to_cold_threshold_size {
                Some(v) => Some(ReadableSize::from_str(&v.clone()).unwrap().as_bytes()),
                _ => None,
            };
        let memory_spill_max_concurrency = hybrid_conf.memory_spill_max_concurrency;

        let event_bus: EventBus<SpillMessage> = EventBus::new(
            runtime_manager.dispatch_runtime.clone(),
            "HybridStoreSpill".to_string(),
            memory_spill_max_concurrency as usize,
        );

        let store = HybridStore {
            hot_store: Arc::new(MemoryStore::from(
                config.memory_store.unwrap(),
                runtime_manager.clone(),
            )),
            warm_store: persistent_stores.pop_front(),
            cold_store: persistent_stores.pop_front(),
            config: hybrid_conf,
            memory_spill_lock: Mutex::new(()),
            memory_spill_event_num: AtomicU64::new(0),
            memory_spill_to_cold_threshold_size,
            memory_spill_max_concurrency,
            runtime_manager,
            event_bus,
        };
        store
    }

    pub fn dec_spill_event_num(&self, delta: u64) {
        self.memory_spill_event_num.dec_by(delta);
    }

    fn is_memory_only(&self) -> bool {
        self.cold_store.is_none() && self.warm_store.is_none()
    }

    fn is_localfile(&self, store: &dyn Any) -> bool {
        store.is::<LocalFileStore>()
    }

    #[allow(unused)]
    fn is_hdfs(&self, store: &dyn Any) -> bool {
        #[cfg(feature = "hdfs")]
        return store.is::<HdfsStore>();

        #[cfg(not(feature = "hdfs"))]
        false
    }

    pub async fn memory_spill_to_persistent_store(
        &self,
        spill_message: SpillMessage,
    ) -> Result<String, WorkerError> {
        let mut ctx: SpillWritingViewContext = spill_message.ctx;
        let retry_cnt = spill_message.retry_cnt;

        if retry_cnt > 3 {
            let app_id = ctx.uid.app_id;
            return Err(WorkerError::SPILL_EVENT_EXCEED_RETRY_MAX_LIMIT(app_id));
        }

        let spill_size = spill_message.size;

        let warm = self
            .warm_store
            .as_ref()
            .ok_or(anyhow!("empty warm store. It should not happen"))?;
        let cold = self.cold_store.as_ref().unwrap_or(warm);

        // we should cover the following cases
        // 1. local store is unhealthy. spill to hdfs
        // 2. event flushed to localfile failed. and exceed retry max cnt, fallback to hdfs
        // 3. huge partition directly flush to hdfs

        // normal assignment
        let mut candidate_store = if warm.is_healthy().await? {
            let cold_spilled_size = self.memory_spill_to_cold_threshold_size.unwrap_or(u64::MAX);
            // if cold_spilled_size < spill_size as u64 || ctx.owned_by_huge_partition {
            if cold_spilled_size < spill_size as u64 {
                cold
            } else {
                warm
            }
        } else {
            cold
        };

        // fallback assignment. propose hdfs always is active and stable
        if retry_cnt >= 1 {
            candidate_store = cold;
        }

        let storage_type = candidate_store.name().await;

        match &storage_type {
            StorageType::LOCALFILE => {
                TOTAL_MEMORY_SPILL_TO_LOCALFILE.inc();
                GAUGE_MEMORY_SPILL_TO_LOCALFILE.inc();
            }
            StorageType::HDFS => {
                TOTAL_MEMORY_SPILL_TO_HDFS.inc();
                GAUGE_MEMORY_SPILL_TO_HDFS.inc();
            }
            _ => {}
        }

        let message = format!(
            "partition uid: {:?}, memory spilled size: {}",
            &ctx.uid, &spill_size
        );

        // Resort the blocks by task_attempt_id to support LOCAL ORDER by default.
        // This is for spark AQE.
        // ctx.data_blocks.sort_by_key(|block| block.task_attempt_id);

        // when throwing the data lost error, it should fast fail for this partition data.
        let result = candidate_store
            .spill_insert(ctx)
            .instrument_await("inserting into the persistent store, invoking [write]")
            .await;

        match &storage_type {
            StorageType::LOCALFILE => {
                GAUGE_MEMORY_SPILL_TO_LOCALFILE.dec();
            }
            StorageType::HDFS => {
                GAUGE_MEMORY_SPILL_TO_HDFS.dec();
            }
            _ => {}
        }

        let _ = result?;

        Ok(message)
    }

    pub fn inc_used(&self, size: i64) -> Result<bool> {
        self.hot_store.inc_used(size)
    }

    pub fn move_allocated_to_used_from_hot_store(&self, size: i64) -> Result<bool> {
        self.hot_store.move_allocated_to_used(size)
    }

    pub fn release_allocated_from_hot_store(&self, size: i64) -> Result<bool> {
        self.hot_store.dec_allocated(size)
    }

    pub async fn mem_snapshot(&self) -> Result<CapacitySnapshot> {
        self.hot_store.memory_snapshot()
    }

    pub async fn get_hot_store_memory_partitioned_buffer_size(
        &self,
        uid: &PartitionedUId,
    ) -> Result<u64> {
        self.hot_store.get_partitioned_buffer_size(uid)
    }

    pub fn memory_spill_event_num(&self) -> Result<u64> {
        Ok(self.memory_spill_event_num.get())
    }

    pub async fn publish_spill_event(&self, message: SpillMessage) -> Result<()> {
        MEMORY_BUFFER_SPILL_BATCH_SIZE_HISTOGRAM.observe(message.size as f64);
        TOTAL_MEMORY_BUFFER_SPILL_BYTE_SIZE.inc_by(message.size as u64);

        self.event_bus.publish(message.into()).await?;
        self.memory_spill_event_num.inc_by(1);

        Ok(())
    }

    pub async fn release_data_in_memory(
        &self,
        data_size: i64,
        message: &SpillMessage,
    ) -> Result<()> {
        let uid = &message.ctx.uid;
        self.hot_store
            .clear_spilled_memory_buffer(uid.clone(), message.flight_id, data_size as u64)
            .await?;
        self.hot_store.dec_used(data_size)?;
        self.hot_store.dec_inflight(data_size as u64);
        Ok(())
    }

    #[trace]
    pub async fn watermark_spill(&self) -> Result<()> {
        let timer = Instant::now();
        let mem_target =
            (self.hot_store.get_capacity()? as f32 * self.config.memory_spill_low_watermark) as i64;
        let buffers = self.hot_store.pickup_spill_blocks(mem_target)?;
        debug!(
            "[Spill] Getting all spill blocks. target_size:{}. it costs {}(ms)",
            mem_target,
            timer.elapsed().as_millis()
        );

        let timer = Instant::now();
        let mut flushed_size = 0u64;
        for (partition_id, buffer) in buffers {
            let spill_result = buffer.spill()?;
            let flight_len = spill_result.flight_len();
            flushed_size += flight_len;

            let writing_ctx = SpillWritingViewContext::new(partition_id, spill_result.blocks());
            let message = SpillMessage {
                ctx: writing_ctx,
                size: flight_len as i64,
                retry_cnt: 0,
                previous_spilled_storage: None,
                flight_id: spill_result.flight_id(),
            };
            if self.publish_spill_event(message).await.is_err() {
                error!("Errors on sending spill message to queue. This should not happen.");
            }
        }
        debug!(
            "[Spill] Picked up blocks that should be async flushed with {}(bytes) that costs {}(ms).",
            flushed_size,
            timer.elapsed().as_millis()
        );
        self.hot_store.inc_inflight(flushed_size);
        Ok(())
    }
}

#[async_trait]
impl Store for HybridStore {
    fn start(self: Arc<HybridStore>) {
        if self.is_memory_only() {
            return;
        }

        self.event_bus.subscribe(SpillEventHandler {
            store: self.clone(),
        });
    }

    #[trace]
    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        let store = self.hot_store.clone();
        let insert_result = store.insert(ctx).await;

        if self.is_memory_only() {
            return insert_result;
        }

        if let Ok(_) = self.memory_spill_lock.try_lock() {
            let ratio = self.hot_store.calculate_usage_ratio();
            if ratio > self.config.memory_spill_high_watermark {
                if let Err(err) = self.watermark_spill().await {
                    warn!("Errors on watermark spill. {:?}", err)
                }
            }
        }

        insert_result
    }

    #[trace]
    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        match ctx.reading_options {
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(_, _) => {
                self.hot_store.get(ctx).await
            }
            _ => self.warm_store.as_ref().unwrap().get(ctx).await,
        }
    }

    #[trace]
    async fn get_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        self.warm_store.as_ref().unwrap().get_index(ctx).await
    }

    #[trace]
    async fn purge(&self, ctx: PurgeDataContext) -> Result<i64> {
        let app_id = &ctx.app_id;
        let mut removed_size = 0i64;

        removed_size += self.hot_store.purge(ctx.clone()).await?;
        info!("Removed data of app:[{}] in hot store", app_id);
        if self.warm_store.is_some() {
            removed_size += self.warm_store.as_ref().unwrap().purge(ctx.clone()).await?;
            info!("Removed data of app:[{}] in warm store", app_id);
        }
        if self.cold_store.is_some() {
            removed_size += self.cold_store.as_ref().unwrap().purge(ctx.clone()).await?;
            info!("Removed data of app:[{}] in cold store", app_id);
        }
        Ok(removed_size)
    }

    #[trace]
    async fn is_healthy(&self) -> Result<bool> {
        async fn check_healthy(store: Option<&Box<dyn PersistentStore>>) -> Result<bool> {
            match store {
                Some(store) => store.is_healthy().await,
                _ => Ok(true),
            }
        }
        let warm = check_healthy(self.warm_store.as_ref())
            .await
            .unwrap_or(false);
        let cold = check_healthy(self.cold_store.as_ref())
            .await
            .unwrap_or(false);
        Ok(self.hot_store.is_healthy().await? && (warm || cold))
    }

    #[trace]
    async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        let uid = &ctx.uid.clone();
        self.hot_store
            .require_buffer(ctx)
            .instrument_await(format!("requiring buffers. uid: {:?}", uid))
            .await
    }

    #[trace]
    async fn release_ticket(&self, ctx: ReleaseTicketContext) -> Result<i64, WorkerError> {
        self.hot_store.release_ticket(ctx).await
    }

    #[trace]
    async fn register_app(&self, ctx: RegisterAppContext) -> Result<()> {
        self.hot_store.register_app(ctx.clone()).await?;
        if self.warm_store.is_some() {
            self.warm_store
                .as_ref()
                .unwrap()
                .register_app(ctx.clone())
                .await?;
        }
        if self.cold_store.is_some() {
            self.cold_store
                .as_ref()
                .unwrap()
                .register_app(ctx.clone())
                .await?;
        }
        Ok(())
    }

    #[trace]
    async fn name(&self) -> StorageType {
        unimplemented!()
    }

    #[trace]
    async fn spill_insert(&self, _ctx: SpillWritingViewContext) -> Result<(), WorkerError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::app::ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE;
    use crate::app::{
        PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext,
        WritingViewContext,
    };
    use crate::config::{
        Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, StorageType,
    };

    use crate::store::hybrid::HybridStore;
    use crate::store::ResponseData::Mem;
    use crate::store::{Block, ResponseData, ResponseDataIndex, Store};
    use bytes::{Buf, Bytes};

    use std::any::Any;
    use std::collections::VecDeque;

    use std::sync::Arc;
    use std::thread;

    use std::time::Duration;

    #[test]
    fn type_downcast_check() {
        trait Fruit {}

        struct Banana {}
        impl Fruit for Banana {}

        struct Apple {}
        impl Fruit for Apple {}

        fn is_apple(store: &dyn Any) -> bool {
            store.is::<Apple>()
        }

        assert_eq!(true, is_apple(&Apple {}));
        assert_eq!(false, is_apple(&Banana {}));

        let boxed_apple = Box::new(Apple {});
        assert_eq!(true, is_apple(&*boxed_apple));
        assert_eq!(false, is_apple(&boxed_apple));
    }

    #[test]
    fn test_only_memory() {
        let mut config = Config::default();
        config.memory_store = Some(MemoryStoreConfig::new("20M".to_string()));
        config.hybrid_store = HybridStoreConfig::new(0.8, 0.2, None);
        config.store_type = StorageType::MEMORY;
        let store = HybridStore::from(config, Default::default());

        let runtime = store.runtime_manager.clone();
        assert_eq!(true, runtime.wait(store.is_healthy()).unwrap());
    }

    #[test]
    fn test_vec_pop() {
        let mut stores = VecDeque::with_capacity(2);
        stores.push_back(1);
        stores.push_back(2);
        assert_eq!(1, stores.pop_front().unwrap());
        assert_eq!(2, stores.pop_front().unwrap());
        assert_eq!(None, stores.pop_front());
    }

    fn start_store(
        memory_single_buffer_max_spill_size: Option<String>,
        memory_capacity: String,
    ) -> Arc<HybridStore> {
        let data = b"hello world!";
        let _data_len = data.len();

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let mut config = Config::default();
        config.memory_store = Some(MemoryStoreConfig::new(memory_capacity));
        config.localfile_store = Some(LocalfileStoreConfig::new(vec![temp_path]));
        config.hybrid_store = HybridStoreConfig::new(0.8, 0.2, memory_single_buffer_max_spill_size);
        config.store_type = StorageType::MEMORY_LOCALFILE;

        // The hybrid store will flush the memory data to file when
        // the data reaches the number of 4
        let store = Arc::new(HybridStore::from(config, Default::default()));
        store
    }

    async fn write_some_data(
        store: Arc<HybridStore>,
        uid: PartitionedUId,
        data_len: i32,
        data: &[u8; 12],
        batch_size: i64,
    ) -> Vec<i64> {
        let mut block_ids = vec![];
        for i in 0..batch_size {
            block_ids.push(i);
            let writing_ctx = WritingViewContext::from(
                uid.clone(),
                vec![Block {
                    block_id: i,
                    length: data_len as i32,
                    uncompress_length: 100,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                }],
            );
            let _ = store.inc_used(data_len as i64);
            let _ = store.insert(writing_ctx).await;
        }

        block_ids
    }

    #[test]
    fn single_buffer_spill_test() -> anyhow::Result<()> {
        let data = b"hello world!";
        let data_len = data.len();

        let store = start_store(
            Some("1".to_string()),
            ((data_len * 10000) as i64).to_string(),
        );
        store.clone().start();

        let runtime = store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        let expected_block_ids = runtime.wait(write_some_data(
            store.clone(),
            uid.clone(),
            data_len as i32,
            data,
            100,
        ));

        thread::sleep(Duration::from_secs(1));

        // read from memory and then from localfile
        let response_data = runtime.wait(store.get(ReadingViewContext {
            uid: uid.clone(),
            reading_options: MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1024 * 1024 * 1024),
            serialized_expected_task_ids_bitmap: Default::default(),
        }))?;

        let mut accepted_block_ids = vec![];
        for segment in response_data.from_memory().shuffle_data_block_segments {
            accepted_block_ids.push(segment.block_id);
        }

        let local_index_data = runtime.wait(store.get_index(ReadingIndexViewContext {
            partition_id: uid.clone(),
        }))?;

        match local_index_data {
            ResponseDataIndex::Local(index) => {
                let mut index_bytes = index.index_data;
                while index_bytes.has_remaining() {
                    // index_bytes_holder.put_i64(next_offset);
                    // index_bytes_holder.put_i32(length);
                    // index_bytes_holder.put_i32(uncompress_len);
                    // index_bytes_holder.put_i64(crc);
                    // index_bytes_holder.put_i64(block_id);
                    // index_bytes_holder.put_i64(task_attempt_id);
                    index_bytes.get_i64();
                    index_bytes.get_i32();
                    index_bytes.get_i32();
                    index_bytes.get_i64();
                    let id = index_bytes.get_i64();
                    index_bytes.get_i64();

                    accepted_block_ids.push(id);
                }
            }
        }

        accepted_block_ids.sort();
        assert_eq!(accepted_block_ids, expected_block_ids);

        Ok(())
    }

    #[tokio::test]
    async fn get_data_from_localfile() {
        let data = b"hello world!";
        let data_len = data.len();

        let store = start_store(None, ((data_len * 1) as i64).to_string());
        store.clone().start();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        write_some_data(store.clone(), uid.clone(), data_len as i32, data, 4).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        // case1: all data has been flushed to localfile. the data in memory should be empty
        let last_block_id = -1;
        let reading_view_ctx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                last_block_id,
                data_len as i64,
            ),
            serialized_expected_task_ids_bitmap: None,
        };

        let read_data = store.get(reading_view_ctx).await;
        if read_data.is_err() {
            panic!();
        }
        let read_data = read_data.unwrap();
        match read_data {
            Mem(mem_data) => {
                assert_eq!(0, mem_data.shuffle_data_block_segments.len());
            }
            _ => panic!(),
        }

        // case2: read data from localfile
        // 1. read index file
        // 2. read data
        let index_view_ctx = ReadingIndexViewContext {
            partition_id: uid.clone(),
        };
        match store.get_index(index_view_ctx).await.unwrap() {
            ResponseDataIndex::Local(index) => {
                let mut index_data = index.index_data;
                while index_data.has_remaining() {
                    let offset = index_data.get_i64();
                    let length = index_data.get_i32();
                    let _uncompress = index_data.get_i32();
                    let _crc = index_data.get_i64();
                    let _block_id = index_data.get_i64();
                    let _task_id = index_data.get_i64();

                    let reading_view_ctx = ReadingViewContext {
                        uid: uid.clone(),
                        reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(offset, length as i64),
                        serialized_expected_task_ids_bitmap: None,
                    };
                    println!("reading. offset: {:?}. len: {:?}", offset, length);
                    let read_data = store.get(reading_view_ctx).await.unwrap();
                    match read_data {
                        ResponseData::Local(local_data) => {
                            assert_eq!(Bytes::copy_from_slice(data), local_data.data);
                        }
                        _ => panic!(),
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_localfile_disk_corrupted() {
        // when the local disk is corrupted, the data will be aborted.
        // Anyway, this partition's data should be not reserved on the memory to effect other
        // apps
    }

    #[tokio::test]
    async fn test_localfile_disk_unhealthy() {
        // when the local disk is unhealthy, the data should be flushed
        // to the cold store(like hdfs). If not having cold, it will retry again
        // then again.
    }

    #[test]
    fn test_insert_and_get_from_memory() {
        let data = b"hello world!";
        let data_len = data.len();

        let store = start_store(None, ((data_len * 1) as i64).to_string());
        let runtime = store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        runtime.wait(write_some_data(
            store.clone(),
            uid.clone(),
            data_len as i32,
            data,
            4,
        ));
        let mut last_block_id = -1;
        // read data one by one
        for idx in 0..=10 {
            let reading_view_ctx = ReadingViewContext {
                uid: uid.clone(),
                reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                    last_block_id,
                    data_len as i64,
                ),
                serialized_expected_task_ids_bitmap: Default::default(),
            };

            let read_data = runtime.wait(store.get(reading_view_ctx));
            if read_data.is_err() {
                panic!();
            }

            match read_data.unwrap() {
                Mem(mem_data) => {
                    if idx >= 4 {
                        println!(
                            "idx: {}, len: {}",
                            idx,
                            mem_data.shuffle_data_block_segments.len()
                        );
                        continue;
                    }
                    assert_eq!(Bytes::copy_from_slice(data), mem_data.data.freeze());
                    let segments = mem_data.shuffle_data_block_segments;
                    assert_eq!(1, segments.len());
                    last_block_id = segments.get(0).unwrap().block_id;
                }
                _ => panic!(),
            }
        }
    }
}
