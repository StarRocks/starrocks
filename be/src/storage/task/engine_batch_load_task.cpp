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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_batch_load_task.cpp

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

#include "storage/task/engine_batch_load_task.h"

#include <fmt/format.h>

#include <ctime>
#include <iostream>
#include <string>

#include "gen_cpp/AgentService_types.h"
#include "runtime/current_thread.h"
#include "storage/lake/spark_load.h"
#include "storage/olap_common.h"
#include "storage/push_handler.h"
#include "storage/storage_engine.h"
#include "util/defer_op.h"
#include "util/pretty_printer.h"
#include "util/starrocks_metrics.h"

using apache::thrift::ThriftDebugString;
using std::list;
using std::string;
using std::vector;

namespace starrocks {

EngineBatchLoadTask::EngineBatchLoadTask(TPushReq& push_req, std::vector<TTabletInfo>* tablet_infos, int64_t signature,
                                         AgentStatus* res_status, MemTracker* mem_tracker)
        : _push_req(push_req), _tablet_infos(tablet_infos), _signature(signature), _res_status(res_status) {
    _mem_tracker = std::make_unique<MemTracker>(-1, "BatchLoad", mem_tracker);
}

EngineBatchLoadTask::~EngineBatchLoadTask() = default;

Status EngineBatchLoadTask::execute() {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker.get());

    AgentStatus status = STARROCKS_SUCCESS;
    if (_push_req.push_type == TPushType::LOAD_V2) {
        status = _init();

        if (status == STARROCKS_SUCCESS) {
            uint32_t retry_time = 0;
            while (retry_time < PUSH_MAX_RETRY) {
                status = _process();

                if (status == STARROCKS_PUSH_HAD_LOADED) {
                    LOG(WARNING) << "transaction exists when realtime push, "
                                    "but unfinished, do not report to fe, signature: "
                                 << _signature;
                    break; // not retry anymore
                }
                // Internal error, need retry
                if (status == STARROCKS_ERROR) {
                    LOG(WARNING) << "push internal error, need retry.signature: " << _signature;
                    retry_time += 1;
                } else {
                    break;
                }
            }
        }
    } else if (_push_req.push_type == TPushType::DELETE) {
        Status delete_data_status = _delete_data(_push_req, _tablet_infos);
        if (!delete_data_status.ok()) {
            LOG(WARNING) << "delete data failed. status: " << delete_data_status << ", signature: " << _signature;
            status = STARROCKS_ERROR;
        }
    } else {
        status = STARROCKS_TASK_REQUEST_ERROR;
    }
    *_res_status = status;
    return Status::OK();
}

AgentStatus EngineBatchLoadTask::_init() {
    AgentStatus status = STARROCKS_SUCCESS;

    if (_is_init) {
        VLOG(3) << "has been inited";
        return status;
    }

    // not need to check these conditions for lake tablet
    if (_push_req.tablet_type != TTabletType::TABLET_TYPE_LAKE) {
        // Check replica exist
        TabletSharedPtr tablet;
        tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_push_req.tablet_id);
        if (tablet == nullptr) {
            LOG(WARNING) << "get tables failed. "
                         << "tablet_id: " << _push_req.tablet_id << ", schema_hash: " << _push_req.schema_hash;
            return STARROCKS_PUSH_INVALID_TABLE;
        }

        // check disk capacity
        if (tablet->data_dir()->capacity_limit_reached(_push_req.__isset.http_file_size)) {
            return STARROCKS_DISK_REACH_CAPACITY_LIMIT;
        }
    }

    DCHECK(!_push_req.__isset.http_file_path);
    _is_init = true;
    return status;
}

AgentStatus EngineBatchLoadTask::_process() {
    AgentStatus status = STARROCKS_SUCCESS;
    if (!_is_init) {
        LOG(WARNING) << "has not init yet. tablet_id: " << _push_req.tablet_id;
        return STARROCKS_ERROR;
    }

    if (status == STARROCKS_SUCCESS) {
        // Load delta file
        time_t push_begin = time(nullptr);
        Status push_status = _push(_push_req, _tablet_infos);
        time_t push_finish = time(nullptr);
        LOG(INFO) << "Push finish, cost time: " << (push_finish - push_begin);
        if (push_status.is_already_exist()) {
            status = STARROCKS_PUSH_HAD_LOADED;
        } else if (!push_status.ok()) {
            status = STARROCKS_ERROR;
        }
    }

    return status;
}

Status EngineBatchLoadTask::_push(const TPushReq& request, std::vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to process push. "
              << " txn_id: " << request.transaction_id << " tablet_id=" << request.tablet_id
              << ", version=" << request.version;

    if (tablet_info_vec == nullptr) {
        LOG(WARNING) << "The input tablet_info_vec is a null pointer";
        StarRocksMetrics::instance()->push_requests_fail_total.increment(1);
        return Status::InternalError("The input tablet_info_vec is a null pointer");
    }

    PushType type = PUSH_NORMAL_V2;

    int64_t duration_ns = 0;
    int64_t write_bytes = 0;
    int64_t write_rows = 0;
    DCHECK(request.__isset.transaction_id);
    SCOPED_RAW_TIMER(&duration_ns);

    Status res;

    if (request.tablet_type == TTabletType::TABLET_TYPE_LAKE) {
        auto tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
        auto tablet_or = tablet_manager->get_tablet(request.tablet_id);
        if (!tablet_or.ok()) {
            LOG(WARNING) << "Fail to load file. res=" << res << ", txn_id: " << request.transaction_id
                         << ", tablet=" << request.tablet_id
                         << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
            StarRocksMetrics::instance()->push_requests_fail_total.increment(1);
            return tablet_or.status();
        }

        auto tablet = tablet_or.value();

        lake::SparkLoadHandler handler;
        res = handler.process_streaming_ingestion(tablet, request, type, tablet_info_vec);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to load file. res=" << res << ", txn_id: " << request.transaction_id
                         << ", tablet=" << tablet.id()
                         << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
            StarRocksMetrics::instance()->push_requests_fail_total.increment(1);
        } else {
            LOG(INFO) << "Finish to load file."
                      << ". txn_id: " << request.transaction_id << ", tablet=" << tablet.id()
                      << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
            write_bytes = handler.write_bytes();
            write_rows = handler.write_rows();
            StarRocksMetrics::instance()->push_requests_success_total.increment(1);
            StarRocksMetrics::instance()->push_request_duration_us.increment(duration_ns / 1000);
            StarRocksMetrics::instance()->push_request_write_bytes.increment(write_bytes);
            StarRocksMetrics::instance()->push_request_write_rows.increment(write_rows);
        }

    } else {
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
        if (tablet == nullptr) {
            LOG(WARNING) << "Not found tablet: " << request.tablet_id;
            StarRocksMetrics::instance()->push_requests_fail_total.increment(1);
            return Status::NotFound(fmt::format("Not found tablet: {}", request.tablet_id));
        }

        PushHandler push_handler;
        res = push_handler.process_streaming_ingestion(tablet, request, type, tablet_info_vec);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to load file. res=" << res << ", txn_id=" << request.transaction_id
                         << ", tablet=" << tablet->full_name()
                         << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
            StarRocksMetrics::instance()->push_requests_fail_total.increment(1);
        } else {
            LOG(INFO) << "Finish to load file."
                      << ". txn_id=" << request.transaction_id << ", tablet=" << tablet->full_name()
                      << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
            write_bytes = push_handler.write_bytes();
            write_rows = push_handler.write_rows();
            StarRocksMetrics::instance()->push_requests_success_total.increment(1);
            StarRocksMetrics::instance()->push_request_duration_us.increment(duration_ns / 1000);
            StarRocksMetrics::instance()->push_request_write_bytes.increment(write_bytes);
            StarRocksMetrics::instance()->push_request_write_rows.increment(write_rows);
        }
    }

    return res;
}

Status EngineBatchLoadTask::_delete_data(const TPushReq& request, std::vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to process delete data. request=" << ThriftDebugString(request);
    StarRocksMetrics::instance()->delete_requests_total.increment(1);

    if (tablet_info_vec == nullptr) {
        LOG(WARNING) << "The input tablet_info_vec is a null pointer";
        return Status::InternalError("The input tablet_info_vec is a null pointer");
    }

    // 1. Get all tablets with same tablet_id
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "Not found tablet: " << request.tablet_id;
        return Status::NotFound(fmt::format("Not found tablet: ", request.tablet_id));
    }

    // 2. Process delete data by push interface
    DCHECK(request.__isset.transaction_id);
    PushHandler push_handler;
    Status res = push_handler.process_streaming_ingestion(tablet, request, PUSH_FOR_DELETE, tablet_info_vec);
    if (!res.ok()) {
        LOG(WARNING) << "Fail to delete data. res: " << res << ", tablet: " << tablet->full_name();
        StarRocksMetrics::instance()->delete_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "Finish to delete data. tablet:" << tablet->full_name();
    return res;
}

} // namespace starrocks
