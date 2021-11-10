// This file is made available under Elastic License 2.0.
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

#include <pthread.h>

#include <cstdio>
#include <ctime>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "gen_cpp/AgentService_types.h"
#include "runtime/current_thread.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/push_handler.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/vectorized/push_handler.h"
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

OLAPStatus EngineBatchLoadTask::execute() {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

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
                    break; // not retry any more
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
        OLAPStatus delete_data_status = _delete_data(_push_req, _tablet_infos);
        if (delete_data_status != OLAPStatus::OLAP_SUCCESS) {
            LOG(WARNING) << "delete data failed. status: " << delete_data_status << ", signature: " << _signature;
            status = STARROCKS_ERROR;
        }
    } else {
        status = STARROCKS_TASK_REQUEST_ERROR;
    }
    *_res_status = status;
    return OLAP_SUCCESS;
}

AgentStatus EngineBatchLoadTask::_init() {
    AgentStatus status = STARROCKS_SUCCESS;

    if (_is_init) {
        VLOG(3) << "has been inited";
        return status;
    }

    // Check replica exist
    TabletSharedPtr tablet;
    tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_push_req.tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "get tables failed. "
                     << "tablet_id: " << _push_req.tablet_id << ", schema_hash: " << _push_req.schema_hash;
        return STARROCKS_PUSH_INVALID_TABLE;
    }

    // check disk capacity
    if (tablet->data_dir()->reach_capacity_limit(_push_req.__isset.http_file_size)) {
        return STARROCKS_DISK_REACH_CAPACITY_LIMIT;
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
        OLAPStatus push_status = _push(_push_req, _tablet_infos);
        time_t push_finish = time(nullptr);
        LOG(INFO) << "Push finish, cost time: " << (push_finish - push_begin);
        if (push_status == OLAPStatus::OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
            status = STARROCKS_PUSH_HAD_LOADED;
        } else if (push_status != OLAPStatus::OLAP_SUCCESS) {
            status = STARROCKS_ERROR;
        }
    }

    return status;
}

OLAPStatus EngineBatchLoadTask::_push(const TPushReq& request, std::vector<TTabletInfo>* tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "begin to process push. "
              << " transaction_id=" << request.transaction_id << " tablet_id=" << request.tablet_id
              << ", version=" << request.version;

    if (tablet_info_vec == nullptr) {
        LOG(WARNING) << "invalid output parameter which is nullptr pointer.";
        StarRocksMetrics::instance()->push_requests_fail_total.increment(1);
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "false to find tablet. tablet=" << request.tablet_id << ", schema_hash=" << request.schema_hash;
        StarRocksMetrics::instance()->push_requests_fail_total.increment(1);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    PushType type = PUSH_NORMAL_V2;

    int64_t duration_ns = 0;
    int64_t write_bytes = 0;
    int64_t write_rows = 0;
    bool use_vectorized = request.__isset.use_vectorized && request.use_vectorized;
    DCHECK(request.__isset.transaction_id);
    if (use_vectorized) {
        SCOPED_RAW_TIMER(&duration_ns);
        vectorized::PushHandler push_handler;
        Status st = push_handler.process_streaming_ingestion(tablet, request, type, tablet_info_vec);
        if (!st.ok()) {
            LOG(WARNING) << "fail to process streaming ingestion. res=" << st.to_string()
                         << ", transaction_id=" << request.transaction_id << ", tablet=" << tablet->full_name();
            res = OLAP_ERR_PUSH_INIT_ERROR;
        }
        write_bytes = push_handler.write_bytes();
        write_rows = push_handler.write_rows();
    } else {
        SCOPED_RAW_TIMER(&duration_ns);
        PushHandler push_handler;
        res = push_handler.process_streaming_ingestion(tablet, request, type, tablet_info_vec);
        write_bytes = push_handler.write_bytes();
        write_rows = push_handler.write_rows();
    }

    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to push delta. res=" << res << ", transaction_id=" << request.transaction_id
                     << ", tablet=" << tablet->full_name()
                     << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        StarRocksMetrics::instance()->push_requests_fail_total.increment(1);
    } else {
        LOG(INFO) << "success to push delta"
                  << ". transaction_id=" << request.transaction_id << ", tablet=" << tablet->full_name()
                  << ", cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        StarRocksMetrics::instance()->push_requests_success_total.increment(1);
        StarRocksMetrics::instance()->push_request_duration_us.increment(duration_ns / 1000);
        StarRocksMetrics::instance()->push_request_write_bytes.increment(write_bytes);
        StarRocksMetrics::instance()->push_request_write_rows.increment(write_rows);
    }
    return res;
}

OLAPStatus EngineBatchLoadTask::_delete_data(const TPushReq& request, std::vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to process delete data. request=" << ThriftDebugString(request);
    StarRocksMetrics::instance()->delete_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    if (tablet_info_vec == nullptr) {
        LOG(WARNING) << "invalid tablet info parameter which is nullptr pointer.";
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    // 1. Get all tablets with same tablet_id
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "can't find tablet. tablet=" << request.tablet_id << ", schema_hash=" << request.schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 2. Process delete data by push interface
    PushHandler push_handler;
    if (request.__isset.transaction_id) {
        res = push_handler.process_streaming_ingestion(tablet, request, PUSH_FOR_DELETE, tablet_info_vec);
    } else {
        res = OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED;
    }

    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to push empty version for delete data. res=" << res << " tablet=" << tablet->full_name();
        StarRocksMetrics::instance()->delete_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "finish to process delete data. res=" << res;
    return res;
}

} // namespace starrocks
