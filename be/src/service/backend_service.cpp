// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/backend_service.cpp

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

#include "service/backend_service.h"

#include <arrow/record_batch.h>
#include <gperftools/heap-profiler.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/processor/TMultiplexedProcessor.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/StarrocksExternalService_types.h"
#include "gen_cpp/TStarrocksExternalService.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/export_task_mgr.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/primitive_type.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "storage/storage_engine.h"
#include "util/arrow/row_batch.h"
#include "util/blocking_queue.hpp"
#include "util/thrift_server.h"
#include "util/uid_util.h"

namespace starrocks {

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::TMultiplexedProcessor;
using apache::thrift::transport::TTransportException;
using apache::thrift::concurrency::ThreadFactory;

BackendService::BackendService(ExecEnv* exec_env)
        : _exec_env(exec_env), _agent_server(new AgentServer(exec_env, *exec_env->master_info())) {
    char buf[64];
    DateTimeValue value = DateTimeValue::local_time();
    value.to_string(buf);
}

Status BackendService::create_service(ExecEnv* exec_env, int port, ThriftServer** server) {
    std::shared_ptr<BackendService> handler(new BackendService(exec_env));
    // TODO: do we want a BoostThreadFactory?
    // TODO: we want separate thread factories here, so that fe requests can't starve
    // be requests
    std::shared_ptr<ThreadFactory> thread_factory(new ThreadFactory());

    std::shared_ptr<TProcessor> be_processor(new BackendServiceProcessor(handler));

    *server = new ThriftServer("backend", be_processor, port, exec_env->metrics(), config::be_service_threads);

    LOG(INFO) << "StarRocksInternalService listening on " << port;

    return Status::OK();
}

void BackendService::exec_plan_fragment(TExecPlanFragmentResult& return_val, const TExecPlanFragmentParams& params) {
    LOG(INFO) << "exec_plan_fragment() instance_id=" << params.params.fragment_instance_id << " coord=" << params.coord
              << " backend#=" << params.backend_num;
    VLOG_ROW << "exec_plan_fragment params is " << apache::thrift::ThriftDebugString(params).c_str();
    start_plan_fragment_execution(params).set_t_status(&return_val);
}

Status BackendService::start_plan_fragment_execution(const TExecPlanFragmentParams& exec_params) {
    if (!exec_params.fragment.__isset.output_sink) {
        return Status::InternalError("missing sink in plan fragment");
    }
    return _exec_env->fragment_mgr()->exec_plan_fragment(exec_params);
}

void BackendService::cancel_plan_fragment(TCancelPlanFragmentResult& return_val,
                                          const TCancelPlanFragmentParams& params) {
    LOG(INFO) << "cancel_plan_fragment(): instance_id=" << params.fragment_instance_id;
    _exec_env->fragment_mgr()->cancel(params.fragment_instance_id).set_t_status(&return_val);
}

void BackendService::transmit_data(TTransmitDataResult& return_val, const TTransmitDataParams& params) {
    VLOG_ROW << "transmit_data(): instance_id=" << params.dest_fragment_instance_id
             << " node_id=" << params.dest_node_id << " #rows=" << params.row_batch.num_rows
             << " eos=" << (params.eos ? "true" : "false");
    // VLOG_ROW << "transmit_data params: " << apache::thrift::ThriftDebugString(params).c_str();

    if (params.__isset.packet_seq) {
        return_val.__set_packet_seq(params.packet_seq);
        return_val.__set_dest_fragment_instance_id(params.dest_fragment_instance_id);
        return_val.__set_dest_node_id(params.dest_node_id);
    }
}

void BackendService::fetch_data(TFetchDataResult& return_val, const TFetchDataParams& params) {
    // maybe hang in this function
    auto result = std::make_unique<TFetchDataResult>();
    Status status = _exec_env->result_mgr()->fetch_data(params.fragment_instance_id, &result);
    return_val = *(result.get());
    status.set_t_status(&return_val);
}

void BackendService::submit_export_task(TStatus& t_status, const TExportTaskRequest& request) {}

void BackendService::get_export_status(TExportStatusResult& result, const TUniqueId& task_id) {}

void BackendService::erase_export_task(TStatus& t_status, const TUniqueId& task_id) {}

void BackendService::get_tablet_stat(TTabletStatResult& result) {
    StorageEngine::instance()->tablet_manager()->get_tablet_stat(&result);
}

void BackendService::submit_routine_load_task(TStatus& t_status, const std::vector<TRoutineLoadTask>& tasks) {
    for (auto& task : tasks) {
        Status st = _exec_env->routine_load_task_executor()->submit_task(task);
        if (!st.ok()) {
            LOG(WARNING) << "failed to submit routine load task. job id: " << task.job_id << " task id: " << task.id;
            return st.to_thrift(&t_status);
        }
    }

    return Status::OK().to_thrift(&t_status);
}

/*
 * 1. validate user privilege (todo)
 * 2. FragmentMgr#exec_plan_fragment
 */
void BackendService::open_scanner(TScanOpenResult& result_, const TScanOpenParams& params) {
    TStatus t_status;
    TUniqueId fragment_instance_id = generate_uuid();
    std::shared_ptr<ScanContext> p_context;
    _exec_env->external_scan_context_mgr()->create_scan_context(&p_context);
    p_context->fragment_instance_id = fragment_instance_id;
    p_context->offset = 0;
    p_context->last_access_time = time(nullptr);
    if (params.__isset.keep_alive_min) {
        p_context->keep_alive_min = params.keep_alive_min;
    } else {
        p_context->keep_alive_min = 5;
    }
    std::vector<TScanColumnDesc> selected_columns;
    // start the scan procedure
    Status exec_st =
            _exec_env->fragment_mgr()->exec_external_plan_fragment(params, fragment_instance_id, &selected_columns);
    exec_st.to_thrift(&t_status);
    //return status
    // t_status.status_code = TStatusCode::OK;
    result_.status = t_status;
    result_.__set_context_id(p_context->context_id);
    result_.__set_selected_columns(selected_columns);
}

// fetch result from polling the queue, should always maintaince the context offset, otherwise inconsistent result
void BackendService::get_next(TScanBatchResult& result_, const TScanNextBatchParams& params) {
    std::string context_id = params.context_id;
    u_int64_t offset = params.offset;
    TStatus t_status;
    std::shared_ptr<ScanContext> context;
    Status st = _exec_env->external_scan_context_mgr()->get_scan_context(context_id, &context);
    if (!st.ok()) {
        st.to_thrift(&t_status);
        result_.status = t_status;
        return;
    }
    if (offset != context->offset) {
        LOG(ERROR) << "getNext error: context offset [" << context->offset << " ]"
                   << " ,client offset [ " << offset << " ]";
        // invalid offset
        t_status.status_code = TStatusCode::NOT_FOUND;
        t_status.error_msgs.push_back(strings::Substitute("context_id=$0, send_offset=$1, context_offset=$2",
                                                          context_id, offset, context->offset));
        result_.status = t_status;
    } else {
        // during accessing, should disabled last_access_time
        context->last_access_time = -1;
        TUniqueId fragment_instance_id = context->fragment_instance_id;
        std::shared_ptr<arrow::RecordBatch> record_batch;
        bool eos;

        st = _exec_env->result_queue_mgr()->fetch_result(fragment_instance_id, &record_batch, &eos);
        if (st.ok()) {
            result_.__set_eos(eos);
            if (!eos) {
                std::string record_batch_str;
                st = serialize_record_batch(*record_batch, &record_batch_str);
                st.to_thrift(&t_status);
                if (st.ok()) {
                    // avoid copy large string
                    result_.rows = std::move(record_batch_str);
                    // set __isset
                    result_.__isset.rows = true;
                    context->offset += record_batch->num_rows();
                }
            }
        } else {
            LOG(WARNING) << "fragment_instance_id [" << print_id(fragment_instance_id) << "] fetch result status ["
                         << st.to_string() + "]";
            st.to_thrift(&t_status);
            result_.status = t_status;
        }
    }
    context->last_access_time = time(nullptr);
}

void BackendService::close_scanner(TScanCloseResult& result_, const TScanCloseParams& params) {
    std::string context_id = params.context_id;
    TStatus t_status;
    Status st = _exec_env->external_scan_context_mgr()->clear_scan_context(context_id);
    st.to_thrift(&t_status);
    result_.status = t_status;
}

} // namespace starrocks
