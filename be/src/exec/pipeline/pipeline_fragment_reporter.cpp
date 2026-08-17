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

#include "exec/pipeline/pipeline_fragment_reporter.h"

#include <memory>
#include <vector>

#include "common/config_rpc_client_fwd.h"
#include "common/logging.h"
#include "common/system/master_info.h"
#include "common/util/thrift_client_cache.h"
#include "common/util/thrift_util.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_context_cancel.h"
#include "exec/pipeline/query_context.h"
#include "exec/runtime/fragment_context_manager.h"
#include "exec/runtime/query_context_manager.h"
#include "exec/runtime_compat/runtime_state_helper.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/Types_types.h"
#include "platform/thrift_rpc_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

void report_fragments_with_same_host(
        const std::vector<std::shared_ptr<pipeline::FragmentContext>>& need_report_fragment_context,
        std::vector<bool>& reported, const TNetworkAddress& last_coord_addr,
        std::vector<TReportExecStatusParams>& report_exec_status_params_vector,
        std::vector<int32_t>& cur_batch_report_indexes, std::vector<PipeLineReportTaskKey>& tasks_to_unregister) {
    for (int i = 0; i < need_report_fragment_context.size(); i++) {
        if (reported[i] == false) {
            pipeline::FragmentContext* fragment_ctx = need_report_fragment_context[i].get();

            if (fragment_ctx->all_execution_groups_finished()) {
                reported[i] = true;
                continue;
            }

            Status fragment_ctx_status = fragment_ctx->final_status();
            if (!fragment_ctx_status.ok()) {
                reported[i] = true;
                tasks_to_unregister.emplace_back(fragment_ctx->query_id(), fragment_ctx->fragment_instance_id());
                continue;
            }

            auto fe_addr = fragment_ctx->fe_addr();
            auto fragment_id = fragment_ctx->fragment_instance_id();
            auto* runtime_state = fragment_ctx->runtime_state();
            DCHECK(runtime_state != nullptr);

            if (fe_addr == last_coord_addr) {
                TReportExecStatusParams params;

                params.protocol_version = FrontendServiceVersion::V1;
                params.__set_query_id(fragment_ctx->query_id());
                params.__set_backend_num(runtime_state->be_number());
                params.__set_fragment_instance_id(fragment_id);
                fragment_ctx_status.set_t_status(&params);
                params.__set_done(false);

                if (runtime_state->query_options().query_type == TQueryType::LOAD) {
                    RuntimeStateHelper::update_report_load_status(runtime_state, &params);
                    params.__set_load_type(runtime_state->query_options().load_job_type);
                }

                auto backend_id = get_backend_id();
                if (backend_id.has_value()) {
                    params.__set_backend_id(backend_id.value());
                }

                report_exec_status_params_vector.emplace_back(std::move(params));
                cur_batch_report_indexes.push_back(i);
                reported[i] = true;
            }
        }
    }
}

} // namespace

std::vector<PipeLineReportTaskKey> report_pipeline_fragments(
        pipeline::QueryContextManager* query_context_mgr,
        const std::vector<PipeLineReportTaskKey>& pipeline_need_report_query_fragment_ids) {
    DCHECK(query_context_mgr != nullptr);
    std::vector<std::shared_ptr<pipeline::QueryContext>> need_report_query_ctx;
    std::vector<std::shared_ptr<pipeline::FragmentContext>> need_report_fragment_context;

    std::vector<PipeLineReportTaskKey> tasks_to_unregister;

    for (const auto& key : pipeline_need_report_query_fragment_ids) {
        TUniqueId query_id = key.query_id;
        TUniqueId fragment_instance_id = key.fragment_instance_id;
        auto query_ctx = query_context_mgr->get(query_id);
        if (!query_ctx) {
            tasks_to_unregister.push_back(key);
            continue;
        }
        need_report_query_ctx.push_back(query_ctx);
        auto fragment_ctx = query_ctx->fragment_mgr()->get(fragment_instance_id);
        if (!fragment_ctx) {
            tasks_to_unregister.push_back(key);
            continue;
        }
        need_report_fragment_context.push_back(fragment_ctx);
    }

    std::vector<bool> reported(need_report_fragment_context.size(), false);
    for (int i = 0; i < need_report_fragment_context.size(); i++) {
        if (reported[i] == false) {
            reported[i] = true;

            pipeline::FragmentContext* fragment_ctx = need_report_fragment_context[i].get();

            if (fragment_ctx->all_execution_groups_finished()) {
                continue;
            }

            Status fragment_ctx_status = fragment_ctx->final_status();
            if (!fragment_ctx_status.ok()) {
                tasks_to_unregister.emplace_back(fragment_ctx->query_id(), fragment_ctx->fragment_instance_id());
                continue;
            }

            auto fe_addr = fragment_ctx->fe_addr();
            auto fragment_id = fragment_ctx->fragment_instance_id();
            auto* runtime_state = fragment_ctx->runtime_state();
            DCHECK(runtime_state != nullptr);

            std::vector<TReportExecStatusParams> report_exec_status_params_vector;

            TReportExecStatusParams params;

            params.protocol_version = FrontendServiceVersion::V1;
            params.__set_query_id(fragment_ctx->query_id());
            params.__set_backend_num(runtime_state->be_number());
            params.__set_fragment_instance_id(fragment_id);
            fragment_ctx_status.set_t_status(&params);
            params.__set_done(false);

            if (runtime_state->query_options().query_type == TQueryType::LOAD) {
                RuntimeStateHelper::update_report_load_status(runtime_state, &params);
                params.__set_load_type(runtime_state->query_options().load_job_type);
            }

            auto backend_id = get_backend_id();
            if (backend_id.has_value()) {
                params.__set_backend_id(backend_id.value());
            }

            report_exec_status_params_vector.push_back(params);

            std::vector<int32_t> cur_batch_report_indexes;
            cur_batch_report_indexes.push_back(i);

            report_fragments_with_same_host(need_report_fragment_context, reported, fe_addr,
                                            report_exec_status_params_vector, cur_batch_report_indexes,
                                            tasks_to_unregister);

            TBatchReportExecStatusParams report_batch;
            report_batch.__set_params_list(report_exec_status_params_vector);

            TBatchReportExecStatusResult res;
            Status rpc_status;

            VLOG_ROW << "debug: reportExecStatus params is " << apache::thrift::ThriftDebugString(params).c_str();
            rpc_status = ThriftRpcHelper::rpc<FrontendServiceClient>(
                    fe_addr,
                    [&res, &report_batch](FrontendServiceConnection& client) {
                        client->batchReportExecStatus(res, report_batch);
                    },
                    config::thrift_rpc_timeout_ms);
            if (!rpc_status.ok()) {
                LOG(WARNING) << "thrift rpc error:" << rpc_status;
                continue;
            }

            const std::vector<TStatus>& status_list = res.status_list;
            for (int j = 0; j < status_list.size(); j++) {
                Status rpc_status = Status(status_list[j]);
                if (!rpc_status.ok()) {
                    int32_t index = cur_batch_report_indexes[j];
                    pipeline::FragmentContext* fragment_ctx = need_report_fragment_context[index].get();
                    pipeline::cancel_fragment_context(fragment_ctx, rpc_status);
                }
            }
        }
    }

    return tasks_to_unregister;
}

} // namespace starrocks
