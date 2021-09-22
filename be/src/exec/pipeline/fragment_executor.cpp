// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/fragment_executor.h"

#include <unordered_map>

#include "exec/exchange_node.h"
#include "exec/pipeline/exchange/exchange_sink_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/morsel.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/result_sink_operator.h"
#include "exec/pipeline/scan_operator.h"
#include "exec/scan_node.h"
#include "gen_cpp/starrocks_internal_service.pb.h"
#include "gutil/casts.h"
#include "gutil/map_util.h"
#include "runtime/data_stream_sender.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/result_sink.h"
#include "util/pretty_printer.h"
#include "util/uid_util.h"

namespace starrocks::pipeline {

Morsels convert_scan_range_to_morsel(const std::vector<TScanRangeParams>& scan_ranges, int node_id) {
    Morsels morsels;
    for (auto scan_range : scan_ranges) {
        morsels.emplace_back(std::make_unique<OlapMorsel>(node_id, scan_range));
    }
    return morsels;
}

Status FragmentExecutor::prepare(ExecEnv* exec_env, const TExecPlanFragmentParams& request) {
    const TPlanFragmentExecParams& params = request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_id = params.fragment_instance_id;

    // prevent an identical fragment instance from multiple execution caused by FE's
    // duplicate invocations of rpc exec_plan_fragment.
    auto&& existing_query_ctx = QueryContextManager::instance()->get(query_id);
    if (existing_query_ctx) {
        auto&& existing_fragment_ctx = existing_query_ctx->fragment_mgr()->get(fragment_id);
        if (existing_fragment_ctx) {
            return Status::OK();
        }
    }

    _query_ctx = QueryContextManager::instance()->get_or_register(query_id);
    if (params.__isset.instances_number) {
        _query_ctx->set_total_fragments(params.instances_number);
    }
    if (request.query_options.__isset.pipeline_query_expire_seconds) {
        _query_ctx->set_expire_seconds(std::max<int>(request.query_options.pipeline_query_expire_seconds, 1));
    } else {
        _query_ctx->set_expire_seconds(300);
    }
    _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_id);
    _fragment_ctx->set_fe_addr(request.coord);

    LOG(INFO) << "Prepare(): query_id=" << print_id(query_id)
              << " fragment_instance_id=" << print_id(params.fragment_instance_id)
              << " backend_num=" << request.backend_num;

    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(request, request.query_options, request.query_globals, exec_env));
    auto* runtime_state = _fragment_ctx->runtime_state();

    int64_t bytes_limit = request.query_options.mem_limit;
    // NOTE: this MemTracker only for olap
    _fragment_ctx->set_mem_tracker(
            std::make_unique<MemTracker>(bytes_limit, "fragment mem-limit", exec_env->query_pool_mem_tracker(), true));
    auto mem_tracker = _fragment_ctx->mem_tracker();

    runtime_state->set_batch_size(config::vector_chunk_size);
    RETURN_IF_ERROR(runtime_state->init_mem_trackers(query_id));
    runtime_state->set_be_number(request.backend_num);
    runtime_state->set_fragment_mem_tracker(mem_tracker);

    LOG(INFO) << "Using query memory limit: " << PrettyPrinter::print(bytes_limit, TUnit::BYTES);

    // Set up desc tbl
    auto* obj_pool = runtime_state->obj_pool();
    DescriptorTbl* desc_tbl = NULL;
    DCHECK(request.__isset.desc_tbl);
    RETURN_IF_ERROR(DescriptorTbl::create(obj_pool, request.desc_tbl, &desc_tbl));
    runtime_state->set_desc_tbl(desc_tbl);
    // Set up plan
    ExecNode* plan = nullptr;
    DCHECK(request.__isset.fragment);
    RETURN_IF_ERROR(ExecNode::create_tree(runtime_state, obj_pool, request.fragment.plan, *desc_tbl, &plan));
    runtime_state->set_fragment_root_id(plan->id());
    _fragment_ctx->set_plan(plan);

    // Set senders of exchange nodes before pipeline build
    std::vector<ExecNode*> exch_nodes;
    plan->collect_nodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
    for (auto* exch_node : exch_nodes) {
        int num_senders = FindWithDefault(params.per_exch_num_senders, exch_node->id(), 0);
        static_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
    }

    int32_t degree_of_parallelism = 1;
    if (request.query_options.__isset.query_threads) {
        degree_of_parallelism = std::max<int32_t>(request.query_options.query_threads, degree_of_parallelism);
    }

    // pipeline scan mode
    // 0: use sync io
    // 1: use async io and exec->thread_pool()
    int32_t pipeline_scan_mode = 1;
    if (request.query_options.__isset.pipeline_scan_mode) {
        pipeline_scan_mode = request.query_options.pipeline_scan_mode;
    }

    // set scan ranges
    std::vector<ExecNode*> scan_nodes;
    std::vector<TScanRangeParams> no_scan_ranges;
    plan->collect_scan_nodes(&scan_nodes);

    MorselQueueMap& morsel_queues = _fragment_ctx->morsel_queues();
    for (int i = 0; i < scan_nodes.size(); ++i) {
        ScanNode* scan_node = down_cast<ScanNode*>(scan_nodes[i]);
        const std::vector<TScanRangeParams>& scan_ranges =
                FindWithDefault(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
        Morsels morsels = convert_scan_range_to_morsel(scan_ranges, scan_node->id());
        morsel_queues.emplace(scan_node->id(), std::make_unique<MorselQueue>(std::move(morsels)));
    }

    PipelineBuilderContext context(_fragment_ctx, degree_of_parallelism);
    PipelineBuilder builder(context);
    _fragment_ctx->set_pipelines(builder.build(*_fragment_ctx, plan));
    // Set up sink, if required
    std::unique_ptr<DataSink> sink;
    if (request.fragment.__isset.output_sink) {
        RowDescriptor row_desc;
        RETURN_IF_ERROR(DataSink::create_data_sink(obj_pool, request.fragment.output_sink,
                                                   request.fragment.output_exprs, params, row_desc, &sink));
        RuntimeProfile* sink_profile = sink->profile();
        if (sink_profile != nullptr) {
            runtime_state->runtime_profile()->add_child(sink_profile, true, nullptr);
        }
        _convert_data_sink_to_operator(params, &context, sink.get());
    }

    Drivers drivers;
    const auto& pipelines = _fragment_ctx->pipelines();
    const size_t num_pipelines = pipelines.size();
    for (auto n = 0; n < num_pipelines; ++n) {
        const auto& pipeline = pipelines[n];
        // DOP(degree of parallelism) of Pipeline's SourceOperator determines the Pipeline's DOP.
        const auto degree_of_parallelism = pipeline->source_operator_factory()->degree_of_parallelism();
        const bool is_root = (n == num_pipelines - 1);
        // If pipeline's SourceOperator is with morsels, a MorselQueue is added to the SourceOperator.
        // at present, only ScanOperator need a MorselQueue attached.
        if (pipeline->source_operator_factory()->with_morsels()) {
            auto source_id = pipeline->get_op_factories()[0]->plan_node_id();
            DCHECK(morsel_queues.count(source_id));
            auto& morsel_queue = morsel_queues[source_id];
            if (morsel_queue->num_morsels() > 0) {
                DCHECK(degree_of_parallelism <= morsel_queue->num_morsels());
            }
            if (is_root) {
                _fragment_ctx->set_num_root_drivers(degree_of_parallelism);
            }
            for (auto i = 0; i < degree_of_parallelism; ++i) {
                Operators&& operators = pipeline->create_operators(degree_of_parallelism, i);
                DriverPtr driver =
                        std::make_shared<PipelineDriver>(std::move(operators), _query_ctx, _fragment_ctx, 0, is_root);
                driver->set_morsel_queue(morsel_queue.get());
                auto* scan_operator = down_cast<ScanOperator*>(driver->source_operator());
                if (pipeline_scan_mode == 1) {
                    scan_operator->set_io_threads(exec_env->pipeline_io_thread_pool());
                } else {
                    scan_operator->set_io_threads(nullptr);
                }
                drivers.emplace_back(std::move(driver));
            }
        } else {
            if (is_root) {
                _fragment_ctx->set_num_root_drivers(degree_of_parallelism);
            }
            for (auto i = 0; i < degree_of_parallelism; ++i) {
                auto&& operators = pipeline->create_operators(degree_of_parallelism, i);
                DriverPtr driver =
                        std::make_shared<PipelineDriver>(std::move(operators), _query_ctx, _fragment_ctx, i, is_root);
                drivers.emplace_back(driver);
            }
        }
    }
    _fragment_ctx->set_drivers(std::move(drivers));
    return Status::OK();
}

Status FragmentExecutor::execute(ExecEnv* exec_env) {
    for (auto driver : _fragment_ctx->drivers()) {
        RETURN_IF_ERROR(driver->prepare(_fragment_ctx->runtime_state()));
    }
    for (auto driver : _fragment_ctx->drivers()) {
        exec_env->driver_dispatcher()->dispatch(driver);
    }
    return Status::OK();
}

void FragmentExecutor::_convert_data_sink_to_operator(const TPlanFragmentExecParams& params,
                                                      PipelineBuilderContext* context, DataSink* datasink) {
    if (typeid(*datasink) == typeid(starrocks::ResultSink)) {
        starrocks::ResultSink* result_sink = down_cast<starrocks::ResultSink*>(datasink);
        // Result sink doesn't have plan node id;
        OpFactoryPtr op = std::make_shared<ResultSinkOperatorFactory>(
                context->next_operator_id(), -1, result_sink->get_sink_type(), result_sink->get_output_exprs());
        // Add result sink operator to last pipeline
        _fragment_ctx->pipelines().back()->add_op_factory(op);
    } else if (typeid(*datasink) == typeid(starrocks::DataStreamSender)) {
        starrocks::DataStreamSender* sender = down_cast<starrocks::DataStreamSender*>(datasink);
        std::shared_ptr<SinkBuffer> sink_buffer = std::make_shared<SinkBuffer>(sender->get_destinations_size());

        OpFactoryPtr exchange_sink = std::make_shared<ExchangeSinkOperatorFactory>(
                context->next_operator_id(), -1, sink_buffer, sender->get_partition_type(), params.destinations,
                params.sender_id, sender->get_dest_node_id(), sender->get_partition_exprs());
        _fragment_ctx->pipelines().back()->add_op_factory(exchange_sink);
    }
}

} // namespace starrocks::pipeline
