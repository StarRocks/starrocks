// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/fragment_executor.h"

#include <unordered_map>

#include "exec/exchange_node.h"
#include "exec/pipeline/exchange/exchange_sink_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/morsel.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_dispatcher.h"
#include "exec/pipeline/result_sink_operator.h"
#include "exec/pipeline/scan_operator.h"
#include "exec/scan_node.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gutil/casts.h"
#include "gutil/map_util.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_sender.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/multi_cast_data_stream_sink.h"
#include "runtime/result_sink.h"
#include "util/pretty_printer.h"
#include "util/uid_util.h"

namespace starrocks::pipeline {

static void setup_profile_hierarchy(RuntimeState* runtime_state, const PipelinePtr& pipeline) {
    runtime_state->runtime_profile()->add_child(pipeline->runtime_profile(), true, nullptr);
}

static void setup_profile_hierarchy(const PipelinePtr& pipeline, const DriverPtr& driver) {
    pipeline->runtime_profile()->add_child(driver->runtime_profile(), true, nullptr);
    pipeline->runtime_profile()->add_info_string(
            "DegreeOfParallelism",
            strings::Substitute("$0", pipeline->source_operator_factory()->degree_of_parallelism()));
    auto& operators = driver->operators();
    for (int32_t j = operators.size() - 1; j >= 0; --j) {
        auto& curr_op = operators[j];
        driver->runtime_profile()->add_child(curr_op->get_runtime_profile(), true, nullptr);
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

    RETURN_IF_ERROR(exec_env->query_pool_mem_tracker()->check_mem_limit("Start execute plan fragment."));

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
    _query_ctx->set_exec_env(exec_env);
    if (params.__isset.instances_number) {
        _query_ctx->set_total_fragments(params.instances_number);
    }
    if (query_options.__isset.query_timeout) {
        _query_ctx->set_expire_seconds(std::max<int>(query_options.query_timeout, 1));
    } else {
        _query_ctx->set_expire_seconds(300);
    }

    // initialize query's deadline
    _query_ctx->extend_lifetime();

    auto fragment_ctx = std::make_unique<FragmentContext>();
    _fragment_ctx = fragment_ctx.get();

    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_instance_id);
    _fragment_ctx->set_fe_addr(coord);

    if (query_options.__isset.pipeline_profile_mode) {
        _fragment_ctx->set_profile_mode(query_options.pipeline_profile_mode);
    }

    LOG(INFO) << "Prepare(): query_id=" << print_id(query_id)
              << " fragment_instance_id=" << print_id(params.fragment_instance_id) << " backend_num=" << backend_num;

    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(query_id, fragment_instance_id, query_options, query_globals, exec_env));
    auto* runtime_state = _fragment_ctx->runtime_state();
    runtime_state->init_mem_trackers(query_id);
    runtime_state->set_be_number(backend_num);

    // RuntimeFilterWorker::open_query is idempotent
    if (params.__isset.runtime_filter_params && params.runtime_filter_params.id_to_prober_params.size() != 0) {
        _query_ctx->set_is_runtime_filter_coordinator(true);
        exec_env->runtime_filter_worker()->open_query(query_id, request.query_options, params.runtime_filter_params,
                                                      true);
    }
    _fragment_ctx->prepare_pass_through_chunk_buffer();

    // Set up desc tbl
    auto* obj_pool = runtime_state->obj_pool();
    DescriptorTbl* desc_tbl = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(obj_pool, t_desc_tbl, &desc_tbl, runtime_state->chunk_size()));
    runtime_state->set_desc_tbl(desc_tbl);
    // Set up plan
    ExecNode* plan = nullptr;
    RETURN_IF_ERROR(ExecNode::create_tree(runtime_state, obj_pool, fragment.plan, *desc_tbl, &plan));
    plan->push_down_join_runtime_filter_recursively(runtime_state);
    std::vector<TupleSlotMapping> empty_mappings;
    plan->push_down_tuple_slot_mappings(runtime_state, empty_mappings);
    runtime_state->set_fragment_root_id(plan->id());
    _fragment_ctx->set_plan(plan);

    // Set up global dict
    if (request.fragment.__isset.query_global_dicts) {
        RETURN_IF_ERROR(runtime_state->init_query_global_dict(request.fragment.query_global_dicts));
    }

    // Set senders of exchange nodes before pipeline build
    std::vector<ExecNode*> exch_nodes;
    plan->collect_nodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
    for (auto* exch_node : exch_nodes) {
        int num_senders = FindWithDefault(params.per_exch_num_senders, exch_node->id(), 0);
        static_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
    }

    int32_t degree_of_parallelism = 1;
    if (request.__isset.pipeline_dop && request.pipeline_dop > 0) {
        degree_of_parallelism = request.pipeline_dop;
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

    std::remove_cv_t<typeof(request.per_scan_node_dop)> per_scan_node_dop;
    if (request.__isset.per_scan_node_dop) {
        per_scan_node_dop = request.per_scan_node_dop;
    }
    PipelineBuilderContext context(_fragment_ctx, degree_of_parallelism, std::move(per_scan_node_dop));
    PipelineBuilder builder(context);
    _fragment_ctx->set_pipelines(builder.build(*_fragment_ctx, plan));
    // Set up sink, if required
    std::unique_ptr<DataSink> sink;
    if (fragment.__isset.output_sink) {
        RowDescriptor row_desc;
        RETURN_IF_ERROR(DataSink::create_data_sink(runtime_state, fragment.output_sink, fragment.output_exprs, params,
                                                   row_desc, &sink));
        RuntimeProfile* sink_profile = sink->profile();
        if (sink_profile != nullptr) {
            runtime_state->runtime_profile()->add_child(sink_profile, true, nullptr);
        }
        _decompose_data_sink_to_operator(runtime_state, &context, fragment.output_sink, sink.get());
    }

    RETURN_IF_ERROR(_fragment_ctx->prepare_all_pipelines());
    Drivers drivers;
    const auto& pipelines = _fragment_ctx->pipelines();
    const size_t num_pipelines = pipelines.size();
    size_t driver_id = 0;
    size_t num_root_drivers = 0;
    for (auto n = 0; n < num_pipelines; ++n) {
        const auto& pipeline = pipelines[n];
        // DOP(degree of parallelism) of Pipeline's SourceOperator determines the Pipeline's DOP.
        const auto degree_of_parallelism = pipeline->source_operator_factory()->degree_of_parallelism();
        LOG(INFO) << "Pipeline " << pipeline->to_readable_string() << " parallel=" << degree_of_parallelism
                  << " fragment_instance_id=" << print_id(params.fragment_instance_id);
        const bool is_root = pipeline->is_root();
        // If pipeline's SourceOperator is with morsels, a MorselQueue is added to the SourceOperator.
        // at present, only ScanOperator need a MorselQueue attached.
        setup_profile_hierarchy(runtime_state, pipeline);
        if (pipeline->source_operator_factory()->with_morsels()) {
            auto source_id = pipeline->get_op_factories()[0]->plan_node_id();
            DCHECK(morsel_queues.count(source_id));
            auto& morsel_queue = morsel_queues[source_id];
            if (morsel_queue->num_morsels() > 0) {
                DCHECK(degree_of_parallelism <= morsel_queue->num_morsels());
            }
            if (is_root) {
                num_root_drivers += degree_of_parallelism;
            }
            for (size_t i = 0; i < degree_of_parallelism; ++i) {
                auto&& operators = pipeline->create_operators(degree_of_parallelism, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), _query_ctx, _fragment_ctx,
                                                                    driver_id++, is_root);
                driver->set_morsel_queue(morsel_queue.get());
                auto* scan_operator = down_cast<ScanOperator*>(driver->source_operator());
                scan_operator->set_io_threads(exec_env->pipeline_scan_io_thread_pool());
                setup_profile_hierarchy(pipeline, driver);
                drivers.emplace_back(std::move(driver));
            }
        } else {
            if (is_root) {
                num_root_drivers += degree_of_parallelism;
            }

            for (size_t i = 0; i < degree_of_parallelism; ++i) {
                auto&& operators = pipeline->create_operators(degree_of_parallelism, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), _query_ctx, _fragment_ctx,
                                                                    driver_id++, is_root);
                setup_profile_hierarchy(pipeline, driver);
                drivers.emplace_back(std::move(driver));
            }
        }
        // The pipeline created later should be placed in the front
        runtime_state->runtime_profile()->reverse_childs();
    }
    _fragment_ctx->set_num_root_drivers(num_root_drivers);
    _fragment_ctx->set_drivers(std::move(drivers));

    _query_ctx->fragment_mgr()->register_ctx(fragment_instance_id, std::move(fragment_ctx));

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

void FragmentExecutor::_decompose_data_sink_to_operator(RuntimeState* runtime_state, PipelineBuilderContext* context,
                                                        const TDataSink& t_datasink, DataSink* datasink) {
    if (typeid(*datasink) == typeid(starrocks::ResultSink)) {
        starrocks::ResultSink* result_sink = down_cast<starrocks::ResultSink*>(datasink);
        // Result sink doesn't have plan node id;
        OpFactoryPtr op =
                std::make_shared<ResultSinkOperatorFactory>(context->next_operator_id(), result_sink->get_sink_type(),
                                                            result_sink->get_output_exprs(), _fragment_ctx);
        // Add result sink operator to last pipeline
        _fragment_ctx->pipelines().back()->add_op_factory(op);
    } else if (typeid(*datasink) == typeid(starrocks::DataStreamSender)) {
        starrocks::DataStreamSender* sender = down_cast<starrocks::DataStreamSender*>(datasink);
        auto dop = _fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();
        auto& t_stream_sink = t_datasink.stream_sink;
        bool is_dest_merge = false;
        if (t_stream_sink.__isset.is_merge && t_stream_sink.is_merge) {
            is_dest_merge = true;
        }
        std::shared_ptr<SinkBuffer> sink_buffer = std::make_shared<SinkBuffer>(
                _fragment_ctx->runtime_state(), sender->destinations(), is_dest_merge, dop);

        OpFactoryPtr exchange_sink = std::make_shared<ExchangeSinkOperatorFactory>(
                context->next_operator_id(), t_stream_sink.dest_node_id, sink_buffer, sender->get_partition_type(),
                sender->destinations(), sender->sender_id(), sender->get_dest_node_id(), sender->get_partition_exprs(),
                sender->get_enable_exchange_pass_through(), _fragment_ctx);
        _fragment_ctx->pipelines().back()->add_op_factory(exchange_sink);

    } else if (typeid(*datasink) == typeid(starrocks::MultiCastDataStreamSink)) {
        // note(yan): steps are:
        // 1. create exchange[EX]
        // 2. create sink[A] at the end of current pipeline
        // 3. create source[B]/sink[C] pipelines.
        // A -> EX -> B0/C0
        //       | -> B1/C1
        //       | -> B2/C2
        // sink[A] will push chunk to exchanger
        // and source[B] will pull chunk from exchanger
        // so basically you can think exchanger is a chunk repository.
        // Further workflow explanation is in mcast_local_exchange.h file.
        starrocks::MultiCastDataStreamSink* mcast_sink = down_cast<starrocks::MultiCastDataStreamSink*>(datasink);
        const auto& sinks = mcast_sink->get_sinks();
        auto& t_multi_case_stream_sink = t_datasink.multi_cast_stream_sink;

        // === create exchange ===
        auto mcast_local_exchanger = std::make_shared<MultiCastLocalExchanger>(runtime_state, sinks.size());

        // === create sink op ====
        auto pseudo_plan_node_id = context->next_pseudo_plan_node_id();
        {
            OpFactoryPtr sink_op = std::make_shared<MultiCastLocalExchangeSinkOperatorFactory>(
                    context->next_operator_id(), pseudo_plan_node_id, mcast_local_exchanger);
            _fragment_ctx->pipelines().back()->add_op_factory(sink_op);
            _fragment_ctx->pipelines().back()->unset_root();
        }

        // ==== create source/sink pipelines ====
        for (size_t i = 0; i < sinks.size(); i++) {
            const auto& sender = sinks[i];
            OpFactories ops;
            // it's okary to set arbitrary dop.
            const size_t dop = 1;
            bool is_dest_merge = false;
            auto& t_stream_sink = t_multi_case_stream_sink.sinks[i];
            if (t_stream_sink.__isset.is_merge && t_stream_sink.is_merge) {
                is_dest_merge = true;
            }

            // source op
            auto source_op = std::make_shared<MultiCastLocalExchangeSourceOperatorFactory>(
                    context->next_operator_id(), pseudo_plan_node_id, i, mcast_local_exchanger);
            source_op->set_degree_of_parallelism(dop);

            // sink op
            auto sink_buffer = std::make_shared<SinkBuffer>(_fragment_ctx->runtime_state(), sender->destinations(),
                                                            is_dest_merge, dop);
            auto sink_op = std::make_shared<ExchangeSinkOperatorFactory>(
                    context->next_operator_id(), -1, sink_buffer, sender->get_partition_type(), sender->destinations(),
                    sender->sender_id(), sender->get_dest_node_id(), sender->get_partition_exprs(),
                    sender->get_enable_exchange_pass_through(), _fragment_ctx);

            ops.emplace_back(source_op);
            ops.emplace_back(sink_op);
            auto pp = std::make_shared<Pipeline>(context->next_pipe_id(), ops);
            pp->set_root();
            _fragment_ctx->pipelines().emplace_back(pp);
        }
    }
}

} // namespace starrocks::pipeline
