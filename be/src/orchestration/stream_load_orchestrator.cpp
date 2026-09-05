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

#include <atomic>
#include <memory>
#include <utility>

#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/uid_util.h"
#include "common/logging.h"
#include "common/process_exit.h"
#include "common/util/debug_util.h"
#include "compute_env/load/stream_load_context.h"
#include "compute_env/load/stream_load_metrics.h"
#include "exec/exec_env.h"
#include "exec/runtime/fragment_context.h"
#include "orchestration/fragment_executor.h"
#include "orchestration/fragment_mgr.h"
#include "orchestration/plan_fragment_executor.h"
#include "runtime/message_body_sink.h"
#include "runtime/runtime_state.h"

namespace starrocks::orchestration {

namespace {

// Bind the load's runtime profile + mem trackers so receive/exec memory is charged to the
// per-load tracker hierarchy. Called as soon as the fragment's RuntimeState exists (before
// the request body is received), so the upload buffers do not escape the tracker hierarchy.
void bind_load_mem_trackers(StreamLoadContext* ctx, RuntimeState* rs) {
    ctx->runtime_profile = rs->runtime_profile_ptr();
    ctx->query_mem_tracker = rs->query_mem_tracker_ptr();
    ctx->instance_mem_tracker = rs->instance_mem_tracker_ptr();
}

// Harvest a finished load fragment's results from its RuntimeState into the StreamLoadContext,
// apply the max-filter-ratio check, update metrics, run error finalization (cancel the body
// sink + reset the Kafka/Pulsar consumption offset), record error_url, and finally fulfill
// ctx->promise. This is the single source of truth shared by the legacy PlanFragmentExecutor
// finish callback and the pipeline FragmentContext finish callback.
void finish_stream_load(StreamLoadContext* ctx, RuntimeState* rs, Status status) {
    bind_load_mem_trackers(ctx, rs);
    ctx->commit_infos = std::move(rs->tablet_commit_infos());
    ctx->fail_infos = std::move(rs->tablet_fail_infos());
    if (status.ok()) {
        ctx->number_total_rows =
                rs->num_rows_load_sink() + rs->num_rows_load_filtered() + rs->num_rows_load_unselected();
        ctx->number_loaded_rows = rs->num_rows_load_sink();
        ctx->number_filtered_rows = rs->num_rows_load_filtered();
        ctx->number_unselected_rows = rs->num_rows_load_unselected();
        ctx->loaded_bytes = rs->num_bytes_load_sink();

        int64_t num_selected_rows = ctx->number_total_rows - ctx->number_unselected_rows;
        if (num_selected_rows > 0 && (double)ctx->number_filtered_rows / num_selected_rows > ctx->max_filter_ratio) {
            // NOTE: Do not modify the error message here, for historical reasons some users
            // may rely on it.
            status = Status::InternalError("too many filtered rows");
        }
        if (status.ok()) {
            auto* metrics = StreamLoadMetrics::instance();
            metrics->stream_receive_bytes_total.increment(ctx->total_receive_bytes);
            metrics->stream_load_rows_total.increment(ctx->number_loaded_rows);
        }
    }
    if (!status.ok()) {
        LOG(WARNING) << "stream load fragment execute failed"
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
    // Record error_url before fulfilling the promise, so the HTTP thread that wakes on the
    // future cannot race with this assignment.
    //
    // The legacy tab-delimited rejected-record file was removed; ctx->rejected_record_path is
    // never populated anymore. The response JSON still carries the field for backward
    // compatibility but it is always empty; query `_statistics_.rejected_records` by Label or
    // txn_id to retrieve rejected rows.
    if (!rs->get_error_log_file_path().empty()) {
        ctx->error_url = to_load_error_http_path(rs->get_error_log_file_path());
    }
    ctx->promise.set_value(status);
}

} // namespace

StreamLoadOrchestrator::StreamLoadOrchestrator(ExecEnv* exec_env, FragmentMgr* fragment_mgr)
        : _exec_env(exec_env), _fragment_mgr(fragment_mgr) {
    DCHECK(_exec_env != nullptr);
}

Status StreamLoadOrchestrator::execute_plan_fragment(StreamLoadContext* ctx) {
    if (process_exit_in_progress()) {
        return Status::ServiceUnavailable("Service is shutting down, please retry later!");
    }

    StreamLoadMetrics::instance()->txn_exec_plan_total.increment(1);
    // Route to the pipeline or the legacy engine; both stay BE-local on the receiving BE.
#ifndef BE_TEST
    if (ctx->put_result.params.__isset.is_pipeline && ctx->put_result.params.is_pipeline) {
        return _execute_plan_fragment_by_pipeline(ctx);
    }
    return _execute_plan_fragment_by_legacy(ctx);
#else
    Status status;
    TEST_SYNC_POINT_CALLBACK("StreamLoadExecutor::execute_plan_fragment:1", &status);
    ctx->promise.set_value(status);
    return Status::OK();
#endif
}

#ifndef BE_TEST
// Legacy (non-pipeline) BE-local execution: run the FE-returned fragment in-process on the
// receiving BE via FragmentMgr + PlanFragmentExecutor. BE-local by construction (it executes
// here), so no scan-BE pinning is needed. Results are harvested via the shared
// finish_stream_load() helper, the same one the pipeline path uses.
Status StreamLoadOrchestrator::_execute_plan_fragment_by_legacy(StreamLoadContext* ctx) {
    ctx->ref();
    ctx->start_write_data_nanos = MonotonicNanos();
    LOG(INFO) << "begin to execute job. label=" << ctx->label << ", txn_id: " << ctx->txn_id
              << ", query_id=" << print_id(ctx->put_result.params.params.query_id);
    // Once this is added into FragmentMgr, the fragment will be counted during graceful exit.
    DCHECK(_fragment_mgr != nullptr);
    auto st = _fragment_mgr->exec_plan_fragment(
            ctx->put_result.params,
            [ctx](PlanFragmentExecutor* executor) { bind_load_mem_trackers(ctx, executor->runtime_state()); },
            [ctx](PlanFragmentExecutor* executor) {
                finish_stream_load(ctx, executor->runtime_state(), executor->status());
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
    return Status::OK();
}
#endif

// BE-local synchronous stream load on the pipeline engine (feature-gated in FE by
// Config.enable_pipeline_stream_load, which stamps params.is_pipeline). Runs the
// FE-returned fragment in-process via orchestration::FragmentExecutor and harvests the
// result from the pipeline FragmentContext's runtime_state on completion via the same
// finish_stream_load() helper the legacy PlanFragmentExecutor path uses. Execution stays on
// the receiving BE because the StreamLoadPipe lives here (LoadStreamMgr keyed by load id).
Status StreamLoadOrchestrator::_execute_plan_fragment_by_pipeline(StreamLoadContext* ctx) {
    ctx->ref();
    ctx->start_write_data_nanos = MonotonicNanos();
    LOG(INFO) << "begin to execute job on pipeline engine. label=" << ctx->label << ", txn_id: " << ctx->txn_id
              << ", query_id=" << print_id(ctx->put_result.params.params.query_id);

    // Complete the load (fulfill promise + unref) exactly once, whether via the fragment
    // finish callback (normal completion) or the execute()-failure path below.
    auto completed = std::make_shared<std::atomic<bool>>(false);
    auto fail = [ctx, completed](const Status& status) {
        if (completed->exchange(true)) {
            return;
        }
        LOG(WARNING) << "pipeline stream load fragment failed"
                     << ", query_id=" << UniqueId(ctx->put_result.params.params.query_id)
                     << ", err_msg=" << status.message() << ", " << ctx->brief();
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(status);
        }
        ctx->write_data_cost_nanos = MonotonicNanos() - ctx->start_write_data_nanos;
        ctx->promise.set_value(status);
        if (ctx->unref()) {
            delete ctx;
        }
    };

    orchestration::FragmentExecutor fragment_executor;
    auto status = fragment_executor.prepare(_exec_env, ctx->put_result.params, ctx->put_result.params);
    if (!status.ok()) {
        fail(status);
        return Status::OK();
    }

    auto fragment_ctx = fragment_executor.fragment_ctx();
    if (fragment_ctx == nullptr) {
        fail(Status::InternalError("pipeline stream load: fragment context is null after prepare"));
        return Status::OK();
    }
    // Bind the load mem trackers now (before the body is received), mirroring the legacy
    // start callback; finish_stream_load() rebinds them harmlessly on completion.
    if (auto* rs = fragment_ctx->runtime_state(); rs != nullptr) {
        bind_load_mem_trackers(ctx, rs);
    }
    // Harvest on completion via the same helper as the legacy path. The callback fires exactly
    // once on the driver thread when all execution groups finish, after the exec-state report
    // is built (so std::move of commit_infos does not race with it).
    fragment_ctx->set_finish_cb([ctx, completed](pipeline::FragmentContext* fctx) {
        if (completed->exchange(true)) {
            return;
        }
        finish_stream_load(ctx, fctx->runtime_state(), fctx->final_status());
        if (ctx->unref()) {
            delete ctx;
        }
    });

    status = fragment_executor.execute(_exec_env);
    if (!status.ok()) {
        fail(status);
        return status;
    }
    return Status::OK();
}

} // namespace starrocks::orchestration
