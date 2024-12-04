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
    AppConfigOptions, AppManagerRef, DataDistribution, GetBlocksContext, PartitionedUId,
    ReadingIndexViewContext, ReadingOptions, ReadingViewContext, RemoteStorageConfig,
    ReportBlocksContext, RequireBufferContext, WritingViewContext,
};
use crate::constant::StatusCode;
use crate::error::WorkerError;
use crate::grpc::protobuf::uniffle::shuffle_server_server::ShuffleServer;
use crate::grpc::protobuf::uniffle::{
    AppHeartBeatRequest, AppHeartBeatResponse, FinishShuffleRequest, FinishShuffleResponse,
    GetLocalShuffleDataRequest, GetLocalShuffleDataResponse, GetLocalShuffleIndexRequest,
    GetLocalShuffleIndexResponse, GetMemoryShuffleDataRequest, GetMemoryShuffleDataResponse,
    GetShuffleResultForMultiPartRequest, GetShuffleResultForMultiPartResponse,
    GetShuffleResultRequest, GetShuffleResultResponse, ReportShuffleResultRequest,
    ReportShuffleResultResponse, RequireBufferRequest, RequireBufferResponse,
    SendShuffleDataRequest, SendShuffleDataResponse, ShuffleCommitRequest, ShuffleCommitResponse,
    ShuffleRegisterRequest, ShuffleRegisterResponse, ShuffleUnregisterByAppIdRequest,
    ShuffleUnregisterByAppIdResponse, ShuffleUnregisterRequest, ShuffleUnregisterResponse,
};
use crate::metric::{
    GRPC_BUFFER_REQUIRE_PROCESS_TIME, GRPC_GET_LOCALFILE_DATA_PROCESS_TIME,
    GRPC_GET_LOCALFILE_DATA_TRANSPORT_TIME, GRPC_GET_MEMORY_DATA_FREEZE_PROCESS_TIME,
    GRPC_GET_MEMORY_DATA_PROCESS_TIME, GRPC_GET_MEMORY_DATA_TRANSPORT_TIME,
    GRPC_SEND_DATA_PROCESS_TIME, GRPC_SEND_DATA_TRANSPORT_TIME,
};
use crate::reject::RejectionPolicyGateway;
use crate::store::{PartitionedData, ResponseDataIndex};
use crate::util;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use croaring::treemap::JvmSerializer;
use croaring::Treemap;
use fastrace::future::FutureExt;
use fastrace::trace;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use tonic::{Request, Response, Status};

/// Use the maximum value for HTTP/2 connection window size to avoid deadlock among multiplexed
/// streams on the same connection.
pub const MAX_CONNECTION_WINDOW_SIZE: u32 = (1 << 31) - 1;
/// Use a large value for HTTP/2 stream window size to improve the performance of remote exchange,
/// as we don't rely on this for back-pressure.
pub const STREAM_WINDOW_SIZE: u32 = 32 * 1024 * 1024; // 32 MB

pub struct DefaultShuffleServer {
    app_manager_ref: AppManagerRef,
    rejection_policy_gateway: RejectionPolicyGateway,
}

impl DefaultShuffleServer {
    pub fn from(
        app_manager_ref: AppManagerRef,
        rejection_policy_gateway: &RejectionPolicyGateway,
    ) -> DefaultShuffleServer {
        DefaultShuffleServer {
            app_manager_ref,
            rejection_policy_gateway: rejection_policy_gateway.clone(),
        }
    }
}

#[tonic::async_trait]
impl ShuffleServer for DefaultShuffleServer {
    async fn register_shuffle(
        &self,
        request: Request<ShuffleRegisterRequest>,
    ) -> Result<Response<ShuffleRegisterResponse>, Status> {
        let inner = request.into_inner();
        // todo: fast fail when hdfs is enabled but empty remote storage info.
        let remote_storage_info = inner.remote_storage.map(|x| RemoteStorageConfig::from(x));
        // todo: add more options: huge_partition_threshold. and so on...
        let app_config_option = AppConfigOptions::new(
            DataDistribution::LOCAL_ORDER,
            inner.max_concurrency_per_partition_to_write,
            remote_storage_info,
        );

        let status = match self.app_manager_ref.register(
            inner.app_id.clone(),
            inner.shuffle_id,
            app_config_option,
        ) {
            Err(e) => {
                error!(
                    "Errors on registering for app:{:?}, shuffle:{:?}. error:{:#?}",
                    &inner.app_id, &inner.shuffle_id, e
                );
                StatusCode::INTERNAL_ERROR
            }
            _ => StatusCode::SUCCESS,
        }
        .into();
        Ok(Response::new(ShuffleRegisterResponse {
            status,
            ret_msg: "".to_string(),
        }))
    }

    async fn unregister_shuffle(
        &self,
        request: Request<ShuffleUnregisterRequest>,
    ) -> Result<Response<ShuffleUnregisterResponse>, Status> {
        let request = request.into_inner();
        let shuffle_id = request.shuffle_id;
        let app_id = request.app_id;

        info!(
            "Accepted unregister shuffle info for [app:{:?}, shuffle_id:{:?}]",
            &app_id, shuffle_id
        );
        let status_code = self
            .app_manager_ref
            .unregister_shuffle(app_id.clone(), shuffle_id)
            .await
            .map_or_else(
                |e| {
                    warn!(
                        "Errors on unregister shuffle for appId:{}. shuffleId:{}. err: {:#?}",
                        &app_id, shuffle_id, e
                    );
                    StatusCode::INTERNAL_ERROR
                },
                |_| StatusCode::SUCCESS,
            );

        Ok(Response::new(ShuffleUnregisterResponse {
            status: status_code.into(),
            ret_msg: "".to_string(),
        }))
    }

    // Once unregister app accepted, the data could be purged.
    async fn unregister_shuffle_by_app_id(
        &self,
        request: Request<ShuffleUnregisterByAppIdRequest>,
    ) -> Result<Response<ShuffleUnregisterByAppIdResponse>, Status> {
        let request = request.into_inner();
        let app_id = request.app_id;

        info!("Accepted unregister app rpc. app_id: {:?}", &app_id);

        let code = self
            .app_manager_ref
            .unregister_app(app_id.clone())
            .await
            .map_or_else(
                |e| {
                    warn!(
                        "Errors on unregister shuffle for appId:{}. err: {:#?}",
                        &app_id, e
                    );
                    StatusCode::INTERNAL_ERROR
                },
                |_| StatusCode::SUCCESS,
            );

        Ok(Response::new(ShuffleUnregisterByAppIdResponse {
            status: code.into(),
            ret_msg: "".to_string(),
        }))
    }

    async fn send_shuffle_data(
        &self,
        request: Request<SendShuffleDataRequest>,
    ) -> Result<Response<SendShuffleDataResponse>, Status> {
        let timer = GRPC_SEND_DATA_PROCESS_TIME.start_timer();
        let req = request.into_inner();

        let app_id = req.app_id;
        let shuffle_id: i32 = req.shuffle_id;
        let ticket_id = req.require_buffer_id;

        let now = util::now_timestamp_as_millis();
        let created = req.timestamp as u128;
        if now > created {
            let transport_seconds = (now - created) / 1000;
            GRPC_SEND_DATA_TRANSPORT_TIME.observe(transport_seconds as f64);
            if transport_seconds > 60 {
                warn!(
                    "Potential long tail latency of transport time {}(seconds) when \
                sending shuffleData for app:{}, shuffleId:{}. This should not happen",
                    transport_seconds, &app_id, shuffle_id
                );
            }
        }

        let app_option = self.app_manager_ref.get_app(&app_id);
        if app_option.is_none() {
            warn!(
                "Reject the NO_REGISTER app: {}. This should not happen",
                &app_id
            );
            return Ok(Response::new(SendShuffleDataResponse {
                status: StatusCode::NO_REGISTER.into(),
                ret_msg: "The app is not found".to_string(),
            }));
        }

        let app = app_option.unwrap();

        let release_result = app
            .release_ticket(ticket_id)
            .instrument_await(format!(
                "releasing buffer for appId: {:?}. shuffleId: {}.",
                &app_id, shuffle_id
            ))
            .await;
        if release_result.is_err() {
            warn!(
                "No such buffer ticketId: {} for app:{} that may be evicted due to the timeout.",
                ticket_id, &app_id
            );
            return Ok(Response::new(SendShuffleDataResponse {
                status: StatusCode::NO_BUFFER.into(),
                ret_msg: "No such buffer ticket id, it may be discarded due to timeout".to_string(),
            }));
        }
        let required_len_with_ticket = release_result.unwrap();

        let mut blocks_map = HashMap::new();
        for shuffle_data in req.shuffle_data {
            let data: PartitionedData = shuffle_data.into();
            let partition_id = data.partition_id;
            let data_blocks = data.blocks;
            let blocks = blocks_map.entry(partition_id).or_insert_with(|| vec![]);
            blocks.extend(data_blocks);
        }

        let mut inserted_failure_occurs = false;
        let mut inserted_failure_error = None;
        let mut inserted_total_size = 0;

        let insert_start = util::now_timestamp_as_millis();
        let mut shuffled_blocks: Vec<_> = blocks_map.into_iter().collect();
        for (partition_id, blocks) in shuffled_blocks {
            if inserted_failure_occurs {
                continue;
            }
            let app_id_ref = app_id.clone();
            let await_tree_msg = format!("inserting data that has costed {}(ms). appId: {:?}. shuffleId: {}. partitionId: {}", util::now_timestamp_as_millis() - insert_start, &app_id_ref, shuffle_id, partition_id);
            let uid = PartitionedUId {
                app_id: app_id_ref,
                shuffle_id,
                partition_id,
            };
            let ctx = WritingViewContext::new(uid, blocks);
            let app_ref = app.clone();
            let inserted = app_ref.insert(ctx).instrument_await(await_tree_msg).await;

            if inserted.is_err() {
                let err = format!(
                    "Errors on putting data. app_id: {}, err: {:?}",
                    &app_id,
                    inserted.err()
                );
                error!("{}", &err);

                inserted_failure_error = Some(err);
                inserted_failure_occurs = true;
                continue;
            }

            let inserted_size = inserted.unwrap();
            inserted_total_size += inserted_size as i64;
        }

        let _ = app.move_allocated_used_from_budget(inserted_total_size);

        let unused_allocated_size = required_len_with_ticket - inserted_total_size;
        if unused_allocated_size != 0 {
            debug!("The required buffer size:[{:?}] has remaining allocated size:[{:?}] of unused, this should not happen",
                required_len_with_ticket, unused_allocated_size);
            if let Err(e) = app.dec_allocated_from_budget(unused_allocated_size) {
                warn!(
                    "Errors on free allocated size: {:?} for app: {:?}. err: {:#?}",
                    unused_allocated_size, &app_id, e
                );
            }
        }

        if inserted_failure_occurs {
            return Ok(Response::new(SendShuffleDataResponse {
                status: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: inserted_failure_error.unwrap(),
            }));
        }

        timer.observe_duration();
        Ok(Response::new(SendShuffleDataResponse {
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string(),
        }))
    }

    async fn get_local_shuffle_index(
        &self,
        request: Request<GetLocalShuffleIndexRequest>,
    ) -> Result<Response<GetLocalShuffleIndexResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id: i32 = req.shuffle_id;
        let partition_id = req.partition_id;
        let _partition_num = req.partition_num;
        let _partition_per_range = req.partition_num_per_range;

        let app_option = self.app_manager_ref.get_app(&app_id);

        if app_option.is_none() {
            warn!("Reject the NO_REGISTER app: {} when getting localShuffleIndex. This should not happen", &app_id);
            return Ok(Response::new(GetLocalShuffleIndexResponse {
                index_data: Default::default(),
                status: StatusCode::NO_REGISTER.into(),
                ret_msg: "App not found".to_string(),
                data_file_len: 0,
                storage_ids: vec![],
            }));
        }

        let app = app_option.unwrap();

        let partition_id = PartitionedUId::from(app_id.to_string(), shuffle_id, partition_id);
        let data_index_wrapper = app
            .list_index(ReadingIndexViewContext {
                partition_id: partition_id.clone(),
            })
            .instrument_await(format!(
                "get index from localfile. uid: {:?}",
                &partition_id
            ))
            .await;

        if data_index_wrapper.is_err() {
            let error_msg = data_index_wrapper.err();
            error!(
                "Errors on getting localfile data index for app:[{}], error: {:?}",
                &app_id, error_msg
            );
            return Ok(Response::new(GetLocalShuffleIndexResponse {
                index_data: Default::default(),
                status: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("{:?}", error_msg),
                data_file_len: 0,
                storage_ids: vec![],
            }));
        }

        match data_index_wrapper.unwrap() {
            ResponseDataIndex::Local(data_index) => {
                Ok(Response::new(GetLocalShuffleIndexResponse {
                    index_data: data_index.index_data,
                    status: StatusCode::SUCCESS.into(),
                    ret_msg: "".to_string(),
                    data_file_len: data_index.data_file_len,
                    storage_ids: vec![],
                }))
            }
        }
    }

    async fn get_local_shuffle_data(
        &self,
        request: Request<GetLocalShuffleDataRequest>,
    ) -> Result<Response<GetLocalShuffleDataResponse>, Status> {
        let timer = GRPC_GET_LOCALFILE_DATA_PROCESS_TIME.start_timer();
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id: i32 = req.shuffle_id;
        let partition_id = req.partition_id;

        let now = util::now_timestamp_as_millis();
        let created = req.timestamp as u128;
        if now > created {
            GRPC_GET_LOCALFILE_DATA_TRANSPORT_TIME.observe(((now - created) / 1000) as f64);
        }

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            warn!("Reject the NO_REGISTER app: {} when getting localShuffleData. This should not happen", &app_id);
            return Ok(Response::new(GetLocalShuffleDataResponse {
                data: Default::default(),
                status: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in this shuffle server".to_string(),
            }));
        }

        let partition_id = PartitionedUId {
            app_id: app_id.to_string(),
            shuffle_id,
            partition_id,
        };
        let data_fetched_result = app
            .unwrap()
            .select(ReadingViewContext {
                uid: partition_id.clone(),
                reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(req.offset, req.length as i64),
                serialized_expected_task_ids_bitmap: Default::default(),
            })
            .instrument_await(format!(
                "select data from localfile. uid: {:?}",
                &partition_id
            ))
            .await;

        if data_fetched_result.is_err() {
            let err_msg = data_fetched_result.err();
            error!(
                "Errors on getting localfile index for app:[{}], error: {:?}",
                &app_id, err_msg
            );
            return Ok(Response::new(GetLocalShuffleDataResponse {
                data: Default::default(),
                status: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("{:?}", err_msg),
            }));
        }

        timer.observe_duration();

        Ok(Response::new(GetLocalShuffleDataResponse {
            data: data_fetched_result.unwrap().from_local(),
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string(),
        }))
    }

    async fn get_memory_shuffle_data(
        &self,
        request: Request<GetMemoryShuffleDataRequest>,
    ) -> Result<Response<GetMemoryShuffleDataResponse>, Status> {
        let timer = GRPC_GET_MEMORY_DATA_PROCESS_TIME.start_timer();
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id: i32 = req.shuffle_id;
        let partition_id = req.partition_id;

        let now = util::now_timestamp_as_millis();
        let created = req.timestamp as u128;
        if now > created {
            let transport_seconds = (now - created) / 1000;
            GRPC_GET_MEMORY_DATA_TRANSPORT_TIME.observe(transport_seconds as f64);
            if transport_seconds > 60 {
                warn!("Potential long tail latency of transport time {}(seconds) when getting \
                memory shuffleData for app:{}, shuffleId:{}, partitionId:{}. This should not happen",
                    transport_seconds, &app_id, shuffle_id, partition_id);
            }
        }

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            warn!("Reject the NO_REGISTER app: {} when getting memoryShuffleData. This should not happen", &app_id);
            return Ok(Response::new(GetMemoryShuffleDataResponse {
                shuffle_data_block_segments: Default::default(),
                data: Default::default(),
                status: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in this shuffle server".to_string(),
            }));
        }

        let partition_id = PartitionedUId {
            app_id: app_id.to_string(),
            shuffle_id,
            partition_id,
        };

        let serialized_expected_task_ids_bitmap =
            if !req.serialized_expected_task_ids_bitmap.is_empty() {
                match Treemap::deserialize(&req.serialized_expected_task_ids_bitmap) {
                    Ok(filter) => Some(filter),
                    Err(e) => {
                        error!("Failed to deserialize: {}", e);
                        None
                    }
                }
            } else {
                None
            };

        let data_fetched_result = app
            .unwrap()
            .select(ReadingViewContext {
                uid: partition_id.clone(),
                reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                    req.last_block_id,
                    req.read_buffer_size as i64,
                ),
                serialized_expected_task_ids_bitmap,
            })
            .instrument_await(format!("select data from memory. uid: {:?}", &partition_id))
            .await;

        if data_fetched_result.is_err() {
            let error_msg = data_fetched_result.err();
            error!(
                "Errors on getting data from memory for [{}], error: {:?}",
                &app_id, error_msg
            );
            return Ok(Response::new(GetMemoryShuffleDataResponse {
                shuffle_data_block_segments: vec![],
                data: Default::default(),
                status: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("{:?}", error_msg),
            }));
        }

        let freeze_timer = GRPC_GET_MEMORY_DATA_FREEZE_PROCESS_TIME.start_timer();
        let data = data_fetched_result.unwrap().from_memory();
        let bytes = data.data.freeze();
        freeze_timer.observe_duration();

        timer.observe_duration();

        Ok(Response::new(GetMemoryShuffleDataResponse {
            shuffle_data_block_segments: data
                .shuffle_data_block_segments
                .into_iter()
                .map(|x| x.into())
                .collect(),
            data: bytes,
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string(),
        }))
    }

    async fn commit_shuffle_task(
        &self,
        _request: Request<ShuffleCommitRequest>,
    ) -> Result<Response<ShuffleCommitResponse>, Status> {
        warn!("It has not been supported of committing shuffle data");
        Ok(Response::new(ShuffleCommitResponse {
            commit_count: 0,
            status: StatusCode::INTERNAL_ERROR.into(),
            ret_msg: "Not supported".to_string(),
        }))
    }

    async fn report_shuffle_result(
        &self,
        request: Request<ReportShuffleResultRequest>,
    ) -> Result<Response<ReportShuffleResultResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;
        let partition_to_block_ids = req.partition_to_block_ids;

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(Response::new(ReportShuffleResultResponse {
                status: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in this shuffle server".to_string(),
            }));
        }
        let app = app.unwrap();

        for partition_to_block_id in partition_to_block_ids {
            let partition_id = partition_to_block_id.partition_id;
            debug!(
                "Reporting partition:{} {} blocks",
                partition_id,
                partition_to_block_id.block_ids.len()
            );
            let ctx = ReportBlocksContext {
                uid: PartitionedUId {
                    app_id: app_id.clone(),
                    shuffle_id,
                    partition_id,
                },
                blocks: partition_to_block_id.block_ids,
            };

            match app.report_block_ids(ctx).await {
                Err(e) => {
                    return Ok(Response::new(ReportShuffleResultResponse {
                        status: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: e.to_string(),
                    }))
                }
                _ => (),
            }
        }

        Ok(Response::new(ReportShuffleResultResponse {
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string(),
        }))
    }

    async fn get_shuffle_result(
        &self,
        request: Request<GetShuffleResultRequest>,
    ) -> Result<Response<GetShuffleResultResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;
        let partition_id = req.partition_id;

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(Response::new(GetShuffleResultResponse {
                status: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in this shuffle server".to_string(),
                serialized_bitmap: Default::default(),
            }));
        }

        let partition_id = PartitionedUId {
            app_id: app_id.to_string(),
            shuffle_id,
            partition_id,
        };
        let block_ids_result = app.unwrap().get_block_ids(GetBlocksContext {
            uid: partition_id.clone(),
        });

        if block_ids_result.is_err() {
            let err_msg = block_ids_result.err();
            error!(
                "Errors on getting shuffle block ids for app:[{}], error: {:?}",
                &app_id, err_msg
            );
            return Ok(Response::new(GetShuffleResultResponse {
                status: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("{:?}", err_msg),
                serialized_bitmap: Default::default(),
            }));
        }

        Ok(Response::new(GetShuffleResultResponse {
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string(),
            serialized_bitmap: block_ids_result.unwrap(),
        }))
    }

    async fn get_shuffle_result_for_multi_part(
        &self,
        request: Request<GetShuffleResultForMultiPartRequest>,
    ) -> Result<Response<GetShuffleResultForMultiPartResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            warn!(
                "The app of {:?} has not been registered or been removed that should not happen!",
                &app_id
            );
            return Ok(Response::new(GetShuffleResultForMultiPartResponse {
                status: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in this shuffle server".to_string(),
                serialized_bitmap: Default::default(),
            }));
        }
        let app = app.unwrap();

        let mut bitmap_result = Treemap::default();
        for partition_id in req.partitions {
            let block_ids_bitmap_result = app.get_block_ids_bitmap(GetBlocksContext {
                uid: PartitionedUId {
                    app_id: app_id.clone(),
                    shuffle_id,
                    partition_id,
                },
            });
            if block_ids_bitmap_result.is_err() {
                let err_msg = block_ids_bitmap_result.err();
                error!(
                    "Errors on getting shuffle block ids by multipart way of app:[{}], error: {:?}",
                    &app_id, err_msg
                );
                return Ok(Response::new(GetShuffleResultForMultiPartResponse {
                    status: StatusCode::INTERNAL_ERROR.into(),
                    ret_msg: format!("{:?}", err_msg),
                    serialized_bitmap: Default::default(),
                }));
            }

            for block_id in block_ids_bitmap_result.unwrap().iter() {
                bitmap_result.add(block_id);
            }
        }

        let serialization = bitmap_result.serialize().unwrap();
        Ok(Response::new(GetShuffleResultForMultiPartResponse {
            status: 0,
            ret_msg: "".to_string(),
            serialized_bitmap: Bytes::from(serialization),
        }))
    }

    async fn finish_shuffle(
        &self,
        _request: Request<FinishShuffleRequest>,
    ) -> Result<Response<FinishShuffleResponse>, Status> {
        info!("Accepted unregister shuffle info....");
        Ok(Response::new(FinishShuffleResponse {
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string(),
        }))
    }

    async fn require_buffer(
        &self,
        request: Request<RequireBufferRequest>,
    ) -> Result<Response<RequireBufferResponse>, Status> {
        let timer = GRPC_BUFFER_REQUIRE_PROCESS_TIME.start_timer();
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(Response::new(RequireBufferResponse {
                require_buffer_id: 0,
                status: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in this shuffle server".to_string(),
            }));
        }

        let partition_id = PartitionedUId {
            app_id,
            shuffle_id,
            // ignore this.
            partition_id: 1,
        };
        let app = app
            .unwrap()
            .require_buffer(RequireBufferContext {
                uid: partition_id.clone(),
                size: req.require_size as i64,
            })
            .instrument_await(format!("require buffer. uid: {:?}", &partition_id))
            .await;

        let res = match app {
            Ok(required_buffer_res) => (
                StatusCode::SUCCESS,
                required_buffer_res.ticket_id,
                "".to_string(),
            ),
            Err(WorkerError::MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION) => (
                StatusCode::NO_BUFFER_FOR_HUGE_PARTITION,
                -1i64,
                "".to_string(),
            ),
            Err(err) => (StatusCode::NO_BUFFER, -1i64, format!("{:?}", err)),
        };

        timer.observe_duration();

        Ok(Response::new(RequireBufferResponse {
            require_buffer_id: res.1,
            status: res.0.into(),
            ret_msg: res.2,
        }))
    }

    async fn app_heartbeat(
        &self,
        request: Request<AppHeartBeatRequest>,
    ) -> Result<Response<AppHeartBeatResponse>, Status> {
        let app_id = request.into_inner().app_id;
        info!("Accepted heartbeat for app: {:?}", &app_id);

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(Response::new(AppHeartBeatResponse {
                status: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in this shuffle server".to_string(),
            }));
        }

        let app = app.unwrap();
        let _ = app.heartbeat();

        Ok(Response::new(AppHeartBeatResponse {
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string(),
        }))
    }
}
