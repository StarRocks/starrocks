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

#include "exec/stream/stream_pipeline_test.h"

#include <gtest/gtest.h>

#include <vector>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/stream_pipeline_driver.h"
#include "exec/stream/stream_operators_test.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "testutil/desc_tbl_helper.h"

namespace starrocks::stream {

using DriverPtr = pipeline::DriverPtr;

Status StreamPipelineTest::prepare() {
    VLOG_ROW << "PreparePipeline";
    _exec_env = ExecEnv::GetInstance();

    const auto& params = _request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_id = params.fragment_instance_id;

    _query_ctx = _exec_env->query_context_mgr()->get_or_register(query_id);
    _query_ctx->set_total_fragments(1);
    _query_ctx->set_delivery_expire_seconds(600);
    _query_ctx->set_query_expire_seconds(600);
    _query_ctx->extend_delivery_lifetime();
    _query_ctx->extend_query_lifetime();
    _query_ctx->init_mem_tracker(_exec_env->query_pool_mem_tracker()->limit(), _exec_env->query_pool_mem_tracker());

    _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_id);
    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(_request.params.query_id, _request.params.fragment_instance_id,
                                           _request.query_options, _request.query_globals, _exec_env));
    _fragment_ctx->set_is_stream_pipeline(true);

    _fragment_future = _fragment_ctx->finish_future();
    _runtime_state = _fragment_ctx->runtime_state();

    _runtime_state->set_chunk_size(config::vector_chunk_size);
    _runtime_state->init_mem_trackers(_query_ctx->mem_tracker());
    _runtime_state->set_be_number(_request.backend_num);
    _runtime_state->set_query_ctx(_query_ctx);
    _runtime_state->set_fragment_ctx(_fragment_ctx);

    _obj_pool = _runtime_state->obj_pool();

    DCHECK(_pipeline_builder != nullptr);
    _pipelines.clear();
    _pipeline_builder(_fragment_ctx->runtime_state());
    _fragment_ctx->set_pipelines(std::move(_pipelines));
    RETURN_IF_ERROR(_fragment_ctx->prepare_all_pipelines());

    const auto& pipelines = _fragment_ctx->pipelines();
    const size_t num_pipelines = pipelines.size();
    for (auto n = 0; n < num_pipelines; ++n) {
        const auto& pipeline = pipelines[n];
        pipeline->instantiate_drivers(_fragment_ctx->runtime_state());
        for (auto& driver : pipeline->drivers()) {
            for (auto& op : driver->operators()) {
                if (auto* stream_source_op = dynamic_cast<GeneratorStreamSourceOperator*>(op.get())) {
                    _tablet_ids.push_back(stream_source_op->tablet_id());
                }
            }
        }
    }
    return Status::OK();
}

Status StreamPipelineTest::execute() {
    VLOG_ROW << "ExecutePipeline";
    Status prepare_status = _fragment_ctx->iterate_drivers(
            [state = _fragment_ctx->runtime_state()](const DriverPtr& driver) { return driver->prepare(state); });
    DCHECK(prepare_status.ok());

    _fragment_ctx->iterate_drivers([exec_env = _exec_env](const DriverPtr& driver) {
        exec_env->driver_executor()->submit(driver.get());
        return Status::OK();
    });
    return Status::OK();
}

OpFactories StreamPipelineTest::maybe_interpolate_local_passthrough_exchange(OpFactories& pred_operators) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());
    auto* source_operator = down_cast<SourceOperatorFactory*>(pred_operators[0].get());
    if (source_operator->degree_of_parallelism() > 1) {
        auto pseudo_plan_node_id = -200;
        auto mem_mgr = std::make_shared<pipeline::LocalExchangeMemoryManager>(config::vector_chunk_size);
        auto local_exchange_source = std::make_shared<pipeline::LocalExchangeSourceOperatorFactory>(
                next_operator_id(), pseudo_plan_node_id, mem_mgr);

        // TODO: Test more cases: Shuffle/Broadcasts?
        auto local_exchange = std::make_shared<pipeline::PassthroughExchanger>(mem_mgr, local_exchange_source.get());

        auto local_exchange_sink = std::make_shared<pipeline::LocalExchangeSinkOperatorFactory>(
                next_operator_id(), pseudo_plan_node_id, local_exchange);

        // Add LocalExchangeSinkOperator to predecessor pipeline.
        pred_operators.emplace_back(std::move(local_exchange_sink));
        // predecessor pipeline comes to end.
        _pipelines.emplace_back(std::make_unique<pipeline::Pipeline>(next_pipeline_id(), pred_operators));

        OpFactories operators_source_with_local_exchange;
        // Multiple LocalChangeSinkOperators pipe into one LocalChangeSourceOperator.
        local_exchange_source->set_degree_of_parallelism(1);
        // A new pipeline is created, LocalExchangeSourceOperator is added as the head of the pipeline.
        operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));
        return operators_source_with_local_exchange;
    } else {
        return pred_operators;
    }
}

Status StreamPipelineTest::start_mv(InitiliazeFunc&& init_func) {
    RETURN_IF_ERROR(init_func());
    RETURN_IF_ERROR(prepare());
    RETURN_IF_ERROR(execute());
    return Status::OK();
}

void StreamPipelineTest::stop_mv() {
    VLOG_ROW << "StopMV";
    _query_ctx->stream_epoch_manager()->set_is_finished(true);
    auto num_activated_drivers =
            _exec_env->driver_executor()->activate_parked_driver([=](const pipeline::PipelineDriver* driver) {
                return driver->query_ctx()->query_id() == _fragment_ctx->query_id();
            });
    DCHECK_EQ(num_activated_drivers, _fragment_ctx->num_drivers());
    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
}

// TODO: Make it work!
void StreamPipelineTest::cancel_mv() {
    VLOG_ROW << "CancelMV";
    _fragment_ctx->cancel(Status::OK());
}

Status StreamPipelineTest::start_epoch(const std::vector<int64_t>& tablet_ids, const EpochInfo& epoch_info) {
    std::unordered_map<int64_t, BinlogOffset> binlog_offsets;
    for (auto tablet_id : tablet_ids) {
        binlog_offsets.insert({tablet_id, BinlogOffset{}});
    }
    std::unordered_map<int64_t, std::unordered_map<int64_t, BinlogOffset>> node_id_binlog_offsets;
    // TODO: WE assume scan node id is zero.
    node_id_binlog_offsets.emplace(0, binlog_offsets);

    // step1. update epoch info
    std::unordered_map<TUniqueId, NodeId2ScanRanges> fragment_id_to_node_id_scan_ranges;
    fragment_id_to_node_id_scan_ranges.emplace(_fragment_ctx->fragment_instance_id(), node_id_binlog_offsets);
    ScanRangeInfo scan_info;
    scan_info.instance_scan_range_map = std::move(fragment_id_to_node_id_scan_ranges);
    RETURN_IF_ERROR(_query_ctx->stream_epoch_manager()->update_epoch(epoch_info, scan_info));

    // step2. reset state
    RETURN_IF_ERROR(_fragment_ctx->reset_epoch());

    // step3. active driver
    auto num_activated_drivers =
            _exec_env->driver_executor()->activate_parked_driver([=](const pipeline::PipelineDriver* driver) {
                return driver->query_ctx()->query_id() == _fragment_ctx->query_id();
            });
    DCHECK_EQ(num_activated_drivers, _fragment_ctx->num_drivers());
    return Status::OK();
}

Status StreamPipelineTest::wait_until_epoch_finished(const EpochInfo& epoch_info) {
    VLOG_ROW << "WaitUntilEpochEnd: " << epoch_info.debug_string();

    auto is_epoch_finished = [=]() {
        for (auto& pipeline : _fragment_ctx->pipelines()) {
            for (auto& driver : pipeline->drivers()) {
                if (driver->driver_state() != pipeline::DriverState::EPOCH_FINISH) {
                    VLOG_ROW << "WaitUntilEpochEnd not epoch finished: " << epoch_info.debug_string();
                    return false;
                }
            }
        }
        return true;
    };
    while (!is_epoch_finished()) {
        sleep(0.1);
    }
    VLOG_ROW << "WaitUntilEpochEnd Done " << epoch_info.debug_string();
    return Status::OK();
}

} // namespace starrocks::stream
