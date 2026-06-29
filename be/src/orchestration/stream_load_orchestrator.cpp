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

#include "orchestration/stream_load_orchestrator.h"

#include <utility>

#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/uid_util.h"
#include "common/logging.h"
#include "common/process_exit.h"
#include "common/util/debug_util.h"
#include "compute_env/load/stream_load_context.h"
#include "compute_env/load/stream_load_metrics.h"
#include "orchestration/fragment_mgr.h"
#include "orchestration/plan_fragment_executor.h"
#include "runtime/exec_env.h"
#include "runtime/message_body_sink.h"
#include "runtime/runtime_state.h"

namespace starrocks::orchestration {

StreamLoadOrchestrator::StreamLoadOrchestrator(ExecEnv* exec_env, FragmentMgr* fragment_mgr)
        : _exec_env(exec_env), _fragment_mgr(fragment_mgr) {
    DCHECK(_exec_env != nullptr);
}

Status StreamLoadOrchestrator::execute_plan_fragment(StreamLoadContext* ctx) {
    if (process_exit_in_progress()) {
        return Status::ServiceUnavailable("Service is shutting down, please retry later!");
    }

    StreamLoadMetrics::instance()->txn_exec_plan_total.increment(1);
// submit this params
#ifndef BE_TEST
    ctx->ref();
    ctx->start_write_data_nanos = MonotonicNanos();
    LOG(INFO) << "begin to execute job. label=" << ctx->label << ", txn_id: " << ctx->txn_id
              << ", query_id=" << print_id(ctx->put_result.params.params.query_id);
    // Once this is added into FragmentMgr, the fragment will be counted during graceful exit.
    DCHECK(_fragment_mgr != nullptr);
    auto st = _fragment_mgr->exec_plan_fragment(
            ctx->put_result.params,
            [ctx](PlanFragmentExecutor* executor) {
                ctx->runtime_profile = executor->runtime_state()->runtime_profile_ptr();
                ctx->query_mem_tracker = executor->runtime_state()->query_mem_tracker_ptr();
                ctx->instance_mem_tracker = executor->runtime_state()->instance_mem_tracker_ptr();
            },
            [ctx](PlanFragmentExecutor* executor) {
                ctx->commit_infos = std::move(executor->runtime_state()->tablet_commit_infos());
                ctx->fail_infos = std::move(executor->runtime_state()->tablet_fail_infos());
                Status status = executor->status();
                if (status.ok()) {
                    ctx->number_total_rows = executor->runtime_state()->num_rows_load_sink() +
                                             executor->runtime_state()->num_rows_load_filtered() +
                                             executor->runtime_state()->num_rows_load_unselected();
                    ctx->number_loaded_rows = executor->runtime_state()->num_rows_load_sink();
                    ctx->number_filtered_rows = executor->runtime_state()->num_rows_load_filtered();
                    ctx->number_unselected_rows = executor->runtime_state()->num_rows_load_unselected();
                    ctx->loaded_bytes = executor->runtime_state()->num_bytes_load_sink();

                    int64_t num_selected_rows = ctx->number_total_rows - ctx->number_unselected_rows;
                    if ((double)ctx->number_filtered_rows / num_selected_rows > ctx->max_filter_ratio) {
                        // NOTE: Do not modify the error message here, for historical
                        // reasons,
                        // some users may rely on this error message.
                        status = Status::InternalError("too many filtered rows");
                    }

                    if (status.ok()) {
                        auto* metrics = StreamLoadMetrics::instance();
                        metrics->stream_receive_bytes_total.increment(ctx->total_receive_bytes);
                        metrics->stream_load_rows_total.increment(ctx->number_loaded_rows);
                    }
                } else {
                    LOG(WARNING) << "fragment execute failed"
                                 << ", query_id=" << UniqueId(ctx->put_result.params.params.query_id)
                                 << ", err_msg=" << status.message() << ", " << ctx->brief();
                    // cancel body_sink, make sender known it
                    if (ctx->body_sink != nullptr) {
                        ctx->body_sink->cancel(status);
                    }

                    switch (ctx->load_src_type) {
                    // reset the stream load ctx's kafka commit offset
                    case TLoadSourceType::KAFKA:
                        ctx->kafka_info->reset_offset();
                        break;
                    case TLoadSourceType::PULSAR:
                        ctx->pulsar_info->clear_backlog();
                        break;
                    default:
                        break;
                    }
                }
                ctx->write_data_cost_nanos = MonotonicNanos() - ctx->start_write_data_nanos;
                ctx->promise.set_value(status);

                if (!executor->runtime_state()->get_error_log_file_path().empty()) {
                    ctx->error_url = to_load_error_http_path(executor->runtime_state()->get_error_log_file_path());
                }

                // The legacy tab-delimited rejected-record file was removed;
                // ctx->rejected_record_path is never populated anymore. The
                // StreamLoadContext response JSON still carries the field
                // for backward compatibility but it will always be empty.
                // Clients should query `_statistics_.rejected_records` by
                // Label or txn_id to retrieve rejected rows.

                if (ctx->unref()) {
                    delete ctx;
                }
            });
    if (!st.ok()) {
        if (ctx->unref()) {
            delete ctx;
        }
        return st;
    }
#else
    Status status;
    TEST_SYNC_POINT_CALLBACK("StreamLoadExecutor::execute_plan_fragment:1", &status);
    ctx->promise.set_value(status);
#endif
    return Status::OK();
}

} // namespace starrocks::orchestration
