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

#include "orchestration/external_scan_orchestrator.h"

#include <arrow/record_batch.h>

#include <cstdint>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

#include "base/uid_util.h"
#include "column/arrow/record_batch_converter.h"
#include "common/logging.h"
#include "common/status.h"
#include "compute_env/result/result_queue_mgr.h"
#include "exec/exec_env.h"
#include "gutil/strings/substitute.h"
#include "orchestration/external_scan_context_mgr.h"
#include "orchestration/query_orchestrator.h"

namespace starrocks::orchestration {

ExternalScanOrchestrator::ExternalScanOrchestrator(ExecEnv* exec_env, ExternalScanContextMgr* context_mgr)
        : _exec_env(exec_env), _context_mgr(context_mgr) {
    DCHECK(_exec_env != nullptr);
    DCHECK(_context_mgr != nullptr);
}

/*
 * 1. validate user privilege (todo)
 * 2. QueryOrchestrator#exec_external_plan_fragment
 */
void ExternalScanOrchestrator::open_scanner(TScanOpenResult& result, const TScanOpenParams& params) {
    TStatus t_status;
    TUniqueId fragment_instance_id = generate_uuid();
    std::shared_ptr<ScanContext> p_context;
    (void)_context_mgr->create_scan_context(&p_context);
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
    QueryOrchestrator query_orchestrator(_exec_env);
    Status exec_st = query_orchestrator.exec_external_plan_fragment(params, fragment_instance_id, &selected_columns,
                                                                    &(p_context->query_id));
    exec_st.to_thrift(&t_status);
    //return status
    // t_status.status_code = TStatusCode::OK;
    result.status = t_status;
    result.__set_context_id(p_context->context_id);
    result.__set_selected_columns(selected_columns);
}

// fetch result from polling the queue, should always maintaince the context offset, otherwise inconsistent result
void ExternalScanOrchestrator::get_next(TScanBatchResult& result, const TScanNextBatchParams& params) {
    std::string context_id = params.context_id;
    uint64_t offset = params.offset;
    TStatus t_status;
    std::shared_ptr<ScanContext> context;
    Status st = _context_mgr->get_scan_context(context_id, &context);
    if (!st.ok()) {
        st.to_thrift(&t_status);
        result.status = t_status;
        return;
    }
    if (offset != context->offset) {
        LOG(ERROR) << "getNext error: context offset [" << context->offset << " ]"
                   << " ,client offset [ " << offset << " ]";
        // invalid offset
        std::string error_msg = strings::Substitute("context_id=$0, send_offset=$1, context_offset=$2", context_id,
                                                    offset, context->offset);
        Status st = Status::NotFound(error_msg);
        st.to_thrift(&t_status);
        result.status = t_status;
    } else {
        // during accessing, should disabled last_access_time
        context->last_access_time = -1;
        TUniqueId fragment_instance_id = context->fragment_instance_id;
        std::shared_ptr<arrow::RecordBatch> record_batch;
        bool eos;

        st = _exec_env->result_queue_mgr()->fetch_result(fragment_instance_id, &record_batch, &eos);
        if (st.ok()) {
            result.__set_eos(eos);
            if (!eos) {
                std::string record_batch_str;
                st = serialize_record_batch(*record_batch, &record_batch_str);
                st.to_thrift(&t_status);
                if (st.ok()) {
                    // avoid copy large string
                    result.rows = std::move(record_batch_str);
                    // set __isset
                    result.__isset.rows = true;
                    context->offset += record_batch->num_rows();
                }
            }
        } else {
            LOG(WARNING) << "fragment_instance_id [" << print_id(fragment_instance_id) << "] fetch result status ["
                         << st.to_string() + "]";
            st.to_thrift(&t_status);
            result.status = t_status;
        }
    }
    context->last_access_time = time(nullptr);
}

void ExternalScanOrchestrator::close_scanner(TScanCloseResult& result, const TScanCloseParams& params) {
    std::string context_id = params.context_id;
    TStatus t_status;
    Status st = _context_mgr->clear_scan_context(context_id);
    st.to_thrift(&t_status);
    result.status = t_status;
}

} // namespace starrocks::orchestration
