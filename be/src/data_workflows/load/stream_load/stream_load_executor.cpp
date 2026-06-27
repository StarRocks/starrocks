// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/stream_load/stream_load_executor.cpp

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

#include "data_workflows/load/stream_load/stream_load_executor.h"

#include <fmt/format.h>

#include <string_view>

#include "base/auth/auth_info.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config_ingest_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "common/system/master_info.h"
#include "common/util/thrift_client_cache.h"
#include "compute_env/load/stream_load_context.h"
#include "compute_env/load/stream_load_metrics.h"
#include "gen_cpp/FrontendService.h"
#include "gutil/walltime.h"
#include "platform/thrift_rpc_helper.h"
#include "storage/non_retryable_load_errors.h"

namespace starrocks {

#ifdef BE_TEST
TLoadTxnBeginResult k_stream_load_begin_result;
TLoadTxnCommitResult k_stream_load_commit_result;
TLoadTxnRollbackResult k_stream_load_rollback_result;
#endif

static Status commit_txn_internal(const TLoadTxnCommitRequest& request, int32_t rpc_timeout_ms,
                                  TLoadTxnCommitResult* result);
static StatusOr<TTransactionStatus::type> get_txn_status(const AuthInfo& auth, std::string_view db,
                                                         std::string_view table, int64_t txn_id);
static bool wait_txn_visible_until(const AuthInfo& auth, std::string_view db, std::string_view table, int64_t txn_id,
                                   int64_t deadline);
static void set_need_rollback(StreamLoadContext* ctx, StreamLoadExecutor* executor);

Status StreamLoadExecutor::begin_txn(StreamLoadContext* ctx) {
    StreamLoadMetrics::instance()->txn_begin_request_total.increment(1);

    TLoadTxnBeginRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.label = ctx->label;
    auto backend_id = get_backend_id();
    if (backend_id.has_value()) {
        request.__set_backend_id(backend_id.value());
    }

    // set timestamp
    request.__set_timestamp(GetCurrentTimeMicros());
    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    }
    request.__set_request_id(ctx->id.to_thrift());

    TNetworkAddress master_addr = get_master_address();
    TLoadTxnBeginResult result;
#ifndef BE_TEST
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->loadTxnBegin(result, request); }));
#else
    result = k_stream_load_begin_result;
#endif
    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "begin transaction failed, errmsg=" << status.message() << ctx->brief();
        if (result.__isset.job_status) {
            ctx->existing_job_status = result.job_status;
        }
        return status;
    }
    ctx->txn_id = result.txnId;
    set_need_rollback(ctx, this);
    ctx->load_deadline_sec = UnixSeconds() + result.timeout;

    return Status::OK();
}

Status StreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    StreamLoadMetrics::instance()->txn_commit_request_total.increment(1);

    TLoadTxnCommitRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.sync = true;
    request.commitInfos = std::move(ctx->commit_infos);
    request.__isset.commitInfos = true;
    request.failInfos = std::move(ctx->fail_infos);
    request.__isset.failInfos = true;
    int32_t rpc_timeout_ms = ctx->calc_put_and_commit_rpc_timeout_ms();
    request.__set_thrift_rpc_timeout_ms(rpc_timeout_ms);

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = attachment;
        request.__isset.txnCommitAttachment = true;
    }

    int retry = 0;
    TLoadTxnCommitResult result;
    while (true) {
        RETURN_IF_ERROR(commit_txn_internal(request, rpc_timeout_ms, &result));
        Status st(result.status);
        if (st.ok()) {
            ctx->clear_need_rollback();
            return st;
        } else if (st.is_publish_timeout()) {
            ctx->clear_need_rollback();
            bool visible =
                    wait_txn_visible_until(ctx->auth, request.db, request.tbl, request.txnId, ctx->load_deadline_sec);
            return visible ? Status::OK() : st;
        } else if (st.is_eagain()) {
            LOG(WARNING) << "commit transaction " << request.txnId << " failed, will retry after sleeping "
                         << result.retry_interval_ms << "ms. errmsg=" << st.message();
            std::this_thread::sleep_for(std::chrono::milliseconds(result.retry_interval_ms));
        } else if (st.is_time_out()) {
            if (++retry > 1) {
                set_need_rollback(ctx, this);
                return st;
            }
            LOG(WARNING) << "commit transaction " << request.txnId << " failed, will retry. errmsg=" << st.message();
            if (ctx->load_deadline_sec > 0) {
                rpc_timeout_ms = (ctx->load_deadline_sec - UnixSeconds()) * 1000;
            }
        } else {
            set_need_rollback(ctx, this);
            return st;
        }
    }
}

Status commit_txn_internal(const TLoadTxnCommitRequest& request, int32_t rpc_timeout_ms, TLoadTxnCommitResult* result) {
    TNetworkAddress master_addr = get_master_address();
#ifndef BE_TEST
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->loadTxnCommit(*result, request); },
            rpc_timeout_ms));
#else
    *result = k_stream_load_commit_result;
#endif
    return Status::OK();
}

StatusOr<TTransactionStatus::type> get_txn_status(const AuthInfo& auth, std::string_view db, std::string_view table,
                                                  int64_t txn_id) {
    TNetworkAddress master_addr = get_master_address();
    TGetLoadTxnStatusRequest request;
    TGetLoadTxnStatusResult result;

    set_request_auth(&request, auth);
    request.db = db;
    request.tbl = table;
    request.txnId = txn_id;

    auto st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->getLoadTxnStatus(result, request); },
            config::stream_load_thrift_rpc_timeout_ms);
    if (!st.ok()) {
        return st;
    } else {
        return result.status;
    }
}

bool wait_txn_visible_until(const AuthInfo& auth, std::string_view db, std::string_view table, int64_t txn_id,
                            int64_t deadline) {
    while (deadline > UnixSeconds()) {
        auto wait_seconds = std::min((int64_t)config::get_txn_status_internal_sec, deadline - UnixSeconds());
        LOG(WARNING) << "transaction is not visible now, will wait " << wait_seconds
                     << " seconds before retrieving the status again, txn_id: " << txn_id;
        // The following sleep might introduce delay to the commit and publish total time
        sleep(wait_seconds);
        auto status_or = get_txn_status(auth, db, table, txn_id);
        if (!status_or.ok()) {
            return false;
        } else if (status_or.value() == TTransactionStatus::VISIBLE) {
            return true;
        } else if (status_or.value() == TTransactionStatus::COMMITTED) {
            continue;
        } else {
            return false;
        }
    }
    return false;
}

Status StreamLoadExecutor::prepare_txn(StreamLoadContext* ctx) {
    StreamLoadMetrics::instance()->txn_commit_request_total.increment(1);

    TLoadTxnCommitRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.sync = true;
    request.commitInfos = std::move(ctx->commit_infos);
    request.__isset.commitInfos = true;
    request.failInfos = std::move(ctx->fail_infos);
    request.__isset.failInfos = true;
    int32_t rpc_timeout_ms = ctx->calc_put_and_commit_rpc_timeout_ms();
    request.__set_thrift_rpc_timeout_ms(rpc_timeout_ms);
    if (ctx->prepared_timeout_second != -1) {
        request.__set_prepared_timeout_second(ctx->prepared_timeout_second);
    }

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = attachment;
        request.__isset.txnCommitAttachment = true;
    }

    TNetworkAddress master_addr = get_master_address();
    TLoadTxnCommitResult result;
#ifndef BE_TEST
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->loadTxnPrepare(result, request); },
            rpc_timeout_ms));
#else
    // Add sync point for testing prepared_timeout_second setting
    TEST_SYNC_POINT_CALLBACK("StreamLoadExecutor::prepare_txn::rpc", &request);
    result = k_stream_load_commit_result;
#endif
    // Return if this transaction is prepare successful; otherwise, we need try
    // to rollback this transaction.
    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "prepare transaction failed, errmsg=" << status.message() << ctx->brief();
        return status;
    }
    // commit success, set need_rollback to false
    ctx->clear_need_rollback();
    return Status::OK();
}

Status StreamLoadExecutor::rollback_txn(StreamLoadContext* ctx) {
    StreamLoadMetrics::instance()->txn_rollback_request_total.increment(1);

    TNetworkAddress master_addr = get_master_address();
    TLoadTxnRollbackRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.commitInfos = std::move(ctx->commit_infos);
    request.__isset.commitInfos = true;
    request.failInfos = std::move(ctx->fail_infos);
    request.__isset.failInfos = true;
    request.__set_reason(std::string(ctx->status.message()));

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = attachment;
        request.__isset.txnCommitAttachment = true;
    }

    TLoadTxnRollbackResult result;
#ifndef BE_TEST
    auto rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->loadTxnRollback(result, request); });
    if (!rpc_st.ok()) {
        LOG(WARNING) << "transaction rollback failed. errmsg=" << rpc_st.message() << ctx->brief();
        return rpc_st;
    }
    if (result.status.status_code != TStatusCode::TXN_NOT_EXISTS) {
        return result.status;
    }
#else
    result = k_stream_load_rollback_result;
#endif
    return Status::OK();
}

bool StreamLoadExecutor::collect_load_stat(StreamLoadContext* ctx, TTxnCommitAttachment* attach) {
    switch (ctx->load_type) {
    case TLoadType::MINI_LOAD: {
        attach->loadType = TLoadType::MINI_LOAD;

        TMiniLoadTxnCommitAttachment ml_attach;
        ml_attach.loadedRows = ctx->number_loaded_rows;
        ml_attach.filteredRows = ctx->number_filtered_rows;
        if (!ctx->error_url.empty()) {
            ml_attach.__set_errorLogUrl(ctx->error_url);
        }

        attach->mlTxnCommitAttachment = ml_attach;
        attach->__isset.mlTxnCommitAttachment = true;
        break;
    }
    case TLoadType::MANUAL_LOAD: {
        attach->loadType = TLoadType::MANUAL_LOAD;

        TManualLoadTxnCommitAttachment manual_load_attach;
        manual_load_attach.__set_loadedRows(ctx->number_loaded_rows);
        manual_load_attach.__set_filteredRows(ctx->number_filtered_rows);
        manual_load_attach.__set_receivedBytes(ctx->receive_bytes);
        manual_load_attach.__set_loadedBytes(ctx->loaded_bytes);
        manual_load_attach.__set_unselectedRows(ctx->number_unselected_rows);
        manual_load_attach.__set_beginTxnTime(ctx->begin_txn_cost_nanos / 1000 / 1000);
        manual_load_attach.__set_planTime(ctx->stream_load_put_cost_nanos / 1000 / 1000);
        manual_load_attach.__set_receiveDataTime(ctx->total_received_data_cost_nanos / 1000 / 1000);
        if (!ctx->error_url.empty()) {
            manual_load_attach.__set_errorLogUrl(ctx->error_url);
        }

        attach->manualLoadTxnCommitAttachment = manual_load_attach;
        attach->__isset.manualLoadTxnCommitAttachment = true;
        break;
    }
    case TLoadType::ROUTINE_LOAD: {
        attach->loadType = TLoadType::ROUTINE_LOAD;

        TRLTaskTxnCommitAttachment rl_attach;
        rl_attach.jobId = ctx->job_id;
        rl_attach.id = ctx->id.to_thrift();
        rl_attach.__set_loadedRows(ctx->number_loaded_rows);
        rl_attach.__set_filteredRows(ctx->number_filtered_rows);
        rl_attach.__set_unselectedRows(ctx->number_unselected_rows);
        rl_attach.__set_receivedBytes(ctx->receive_bytes);
        rl_attach.__set_loadedBytes(ctx->loaded_bytes);
        rl_attach.__set_loadCostMs(ctx->load_cost_nanos / 1000 / 1000);
        if (!ctx->status.ok() && is_non_retryable_load_error(ctx->status.message())) {
            rl_attach.__set_nonRetryable(true);
        }

        attach->rlTaskTxnCommitAttachment = rl_attach;
        attach->__isset.rlTaskTxnCommitAttachment = true;
        break;
    }
    default:
        // unknown load type, should not happen
        return false;
    }

    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        TRLTaskTxnCommitAttachment& rl_attach = attach->rlTaskTxnCommitAttachment;
        rl_attach.loadSourceType = TLoadSourceType::KAFKA;

        TKafkaRLTaskProgress kafka_progress;
        kafka_progress.partitionCmtOffset = ctx->kafka_info->cmt_offset;
        kafka_progress.partitionCmtOffsetTimestamp = ctx->kafka_info->cmt_offset_timestamp;
        kafka_progress.__isset.partitionCmtOffsetTimestamp = true;

        rl_attach.kafkaRLTaskProgress = kafka_progress;
        rl_attach.__isset.kafkaRLTaskProgress = true;
        if (!ctx->error_url.empty()) {
            rl_attach.__set_errorLogUrl(ctx->error_url);
        }
        return true;
    }
    case TLoadSourceType::PULSAR: {
        TRLTaskTxnCommitAttachment& rl_attach = attach->rlTaskTxnCommitAttachment;
        rl_attach.loadSourceType = TLoadSourceType::PULSAR;

        TPulsarRLTaskProgress pulsar_progress;
        pulsar_progress.partitionBacklogNum = ctx->pulsar_info->partition_backlog;

        rl_attach.pulsarRLTaskProgress = pulsar_progress;
        rl_attach.__isset.pulsarRLTaskProgress = true;
        if (!ctx->error_url.empty()) {
            rl_attach.__set_errorLogUrl(ctx->error_url);
        }
        return true;
    }
    default:
        return true;
    }
    return false;
}

void set_need_rollback(StreamLoadContext* ctx, StreamLoadExecutor* executor) {
    ctx->set_need_rollback(
            [executor](StreamLoadContext* rollback_ctx) { return executor->rollback_txn(rollback_ctx); });
}

} // namespace starrocks
