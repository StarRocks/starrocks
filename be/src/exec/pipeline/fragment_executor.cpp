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
#include "gen_cpp/doris_internal_service.pb.h"
#include "gutil/casts.h"
#include "gutil/map_util.h"
#include "runtime/data_stream_sender.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/result_sink.h"
#include "util/pretty_printer.h"
#include "util/uid_util.h"

namespace starrocks::pipeline {

static void setup_profile_hierarchy(RuntimeState* runtime_state, const DriverPtr& driver) {
    runtime_state->runtime_profile()->add_child(driver->runtime_profile(), true, nullptr);
    auto& operators = driver->operators();
    for (int32_t j = operators.size() - 1; j >= 0; --j) {
        auto& curr_op = operators[j];
        if (j == operators.size() - 1) {
            driver->runtime_profile()->add_child(curr_op->get_runtime_profile(), true, nullptr);
        } else {
            auto& prev_op = operators[j + 1];
            prev_op->get_runtime_profile()->add_child(curr_op->get_runtime_profile(), true, nullptr);
        }
    }
}

Morsels convert_scan_range_to_morsel(const std::vector<TScanRangeParams>& scan_ranges, int node_id) {
    Morsels morsels;
    for (const auto& scan_range : scan_ranges) {
        morsels.emplace_back(std::make_unique<OlapMorsel>(node_id, scan_range));
    }
    return morsels;
}

Status FragmentExecutor::prepare(ExecEnv* exec_env, const TExecPlanFragmentParams& request) {
    DCHECK(request.__isset.desc_tbl);
    DCHECK(request.__isset.fragment);
    const auto& params = request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_instance_id = params.fragment_instance_id;
    const auto& coord = request.coord;
    const auto& query_options = request.query_options;
    const auto& query_globals = request.query_globals;
    const auto& backend_num = request.backend_num;
    const auto& t_desc_tbl = request.desc_tbl;
    const auto& fragment = request.fragment;

    // prevent an identical fragment instance from multiple execution caused by FE's
    // duplicate invocations of rpc exec_plan_fragment.
    auto&& existing_query_ctx = QueryContextManager::instance()->get(query_id);
    if (existing_query_ctx) {
        auto&& existing_fragment_ctx = existing_query_ctx->fragment_mgr()->get(fragment_instance_id);
        if (existing_fragment_ctx) {
            return Status::DuplicateRpcInvocation("Duplicate invocations of exec_plan_fragment");
        }
    }

    _query_ctx = QueryContextManager::instance()->get_or_register(query_id);
    if (params.__isset.instances_number) {
        _query_ctx->set_total_fragments(params.instances_number);
    }
    if (query_options.__isset.pipeline_query_expire_seconds) {
        _query_ctx->set_expire_seconds(std::max<int>(query_options.pipeline_query_expire_seconds, 1));
    } else {
        _query_ctx->set_expire_seconds(300);
    }
    // initialize query's deadline
    _query_ctx->extend_lifetime();

    _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_instance_id);
    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_instance_id);
    _fragment_ctx->set_fe_addr(coord);

    LOG(INFO) << "Prepare(): query_id=" << print_id(query_id)
              << " fragment_instance_id=" << print_id(params.fragment_instance_id) << " backend_num=" << backend_num;

    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(query_id, fragment_instance_id, query_options, query_globals, exec_env));
    auto* runtime_state = _fragment_ctx->runtime_state();

    runtime_state->set_batch_size(config::vector_chunk_size);
    runtime_state->init_mem_trackers(query_id);
    runtime_state->set_be_number(backend_num);

    // Set up desc tbl
    auto* obj_pool = runtime_state->obj_pool();
    DescriptorTbl* desc_tbl = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(obj_pool, t_desc_tbl, &desc_tbl));
    runtime_state->set_desc_tbl(desc_tbl);
    // Set up plan
    ExecNode* plan = nullptr;
    RETURN_IF_ERROR(ExecNode::create_tree(runtime_state, obj_pool, fragment.plan, *desc_tbl, &plan));
    runtime_state->set_fragment_root_id(plan->id());
    _fragment_ctx->set_plan(plan);

    // Set up global dict
    if (request.fragment.__isset.global_dicts) {
        RETURN_IF_ERROR(runtime_state->init_global_dict(request.fragment.global_dicts));
    }

    // Set senders of exchange nodes before pipeline build
    std::vector<ExecNode*> exch_nodes;
    plan->collect_nodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
    for (auto* exch_node : exch_nodes) {
        int num_senders = FindWithDefault(params.per_exch_num_senders, exch_node->id(), 0);
        static_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
    }

    int32_t degree_of_parallelism = 1;
    if (query_options.__isset.pipeline_dop && query_options.pipeline_dop > 0) {
        degree_of_parallelism = query_options.pipeline_dop;
    } else {
        // default dop is a half of the number of hardware threads.
        degree_of_parallelism = std::max<int32_t>(1, std::thread::hardware_concurrency() / 2);
    }

    // set scan ranges
    std::vector<ExecNode*> scan_nodes;
    std::vector<TScanRangeParams> no_scan_ranges;
    plan->collect_scan_nodes(&scan_nodes);

    MorselQueueMap& morsel_queues = _fragment_ctx->morsel_queues();
    for (auto& i : scan_nodes) {
        ScanNode* scan_node = down_cast<ScanNode*>(i);
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
    if (fragment.__isset.output_sink) {
        RowDescriptor row_desc;
        RETURN_IF_ERROR(DataSink::create_data_sink(obj_pool, fragment.output_sink, fragment.output_exprs, params,
                                                   row_desc, &sink));
        RuntimeProfile* sink_profile = sink->profile();
        if (sink_profile != nullptr) {
            runtime_state->runtime_profile()->add_child(sink_profile, true, nullptr);
        }
        _convert_data_sink_to_operator(params, &context, sink.get());
    }

    RETURN_IF_ERROR(_fragment_ctx->prepare_all_pipelines());
    Drivers drivers;
    const auto& pipelines = _fragment_ctx->pipelines();
    const size_t num_pipelines = pipelines.size();
    size_t driver_id = 0;
    for (auto n = 0; n < num_pipelines; ++n) {
        const auto& pipeline = pipelines[n];
        // DOP(degree of parallelism) of Pipeline's SourceOperator determines the Pipeline's DOP.
        const auto degree_of_parallelism = pipeline->source_operator_factory()->degree_of_parallelism();
        LOG(INFO) << "Pipeline " << pipeline->to_readable_string() << " parallel=" << degree_of_parallelism
                  << " fragment_instance_id=" << print_id(params.fragment_instance_id);
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
            for (size_t i = 0; i < degree_of_parallelism; ++i) {
                auto&& operators = pipeline->create_operators(degree_of_parallelism, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), _query_ctx, _fragment_ctx,
                                                                    driver_id++, is_root);
                driver->set_morsel_queue(morsel_queue.get());
                auto* scan_operator = down_cast<ScanOperator*>(driver->source_operator());
                scan_operator->set_io_threads(exec_env->pipeline_io_thread_pool());
                setup_profile_hierarchy(runtime_state, driver);
                drivers.emplace_back(std::move(driver));
            }
        } else {
            if (is_root) {
                _fragment_ctx->set_num_root_drivers(degree_of_parallelism);
            }
            for (size_t i = 0; i < degree_of_parallelism; ++i) {
                auto&& operators = pipeline->create_operators(degree_of_parallelism, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), _query_ctx, _fragment_ctx,
                                                                    driver_id++, is_root);
                setup_profile_hierarchy(runtime_state, driver);
                drivers.emplace_back(std::move(driver));
            }
        }
    }
    _fragment_ctx->set_drivers(std::move(drivers));
    return Status::OK();
}

Status FragmentExecutor::execute(ExecEnv* exec_env) {
    for (const auto& driver : _fragment_ctx->drivers()) {
        RETURN_IF_ERROR(driver->prepare(_fragment_ctx->runtime_state()));
    }
    for (const auto& driver : _fragment_ctx->drivers()) {
        exec_env->driver_dispatcher()->dispatch(driver.get());
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
        auto dop = _fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();
        std::shared_ptr<SinkBuffer> sink_buffer = std::make_shared<SinkBuffer>(
                _fragment_ctx->runtime_state()->instance_mem_tracker(), sender->get_destinations_size(), dop);

        OpFactoryPtr exchange_sink = std::make_shared<ExchangeSinkOperatorFactory>(
                context->next_operator_id(), -1, sink_buffer, sender->get_partition_type(), params.destinations,
                params.sender_id, sender->get_dest_node_id(), sender->get_partition_exprs());
        _fragment_ctx->pipelines().back()->add_op_factory(exchange_sink);
    }
}

} // namespace starrocks::pipeline
