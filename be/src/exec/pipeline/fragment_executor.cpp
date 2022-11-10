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
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/result_sink_operator.h"
#include "exec/pipeline/scan_operator.h"
#include "exec/scan_node.h"
#include "exec/workgroup/work_group.h"
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

using WorkGroupManager = workgroup::WorkGroupManager;
using WorkGroup = workgroup::WorkGroup;
using WorkGroupPtr = workgroup::WorkGroupPtr;

static void setup_profile_hierarchy(RuntimeState* runtime_state, const PipelinePtr& pipeline) {
    runtime_state->runtime_profile()->add_child(pipeline->runtime_profile(), true, nullptr);
}

static void setup_profile_hierarchy(const PipelinePtr& pipeline, const DriverPtr& driver) {
    pipeline->runtime_profile()->add_child(driver->runtime_profile(), true, nullptr);
    auto* dop_counter = ADD_COUNTER(pipeline->runtime_profile(), "DegreeOfParallelism", TUnit::UNIT);
    COUNTER_SET(dop_counter, static_cast<int64_t>(pipeline->source_operator_factory()->degree_of_parallelism()));
    auto* total_dop_counter = ADD_COUNTER(pipeline->runtime_profile(), "TotalDegreeOfParallelism", TUnit::UNIT);
    COUNTER_SET(total_dop_counter, dop_counter->value());
    auto& operators = driver->operators();
    for (int32_t i = operators.size() - 1; i >= 0; --i) {
        auto& curr_op = operators[i];
        driver->runtime_profile()->add_child(curr_op->get_runtime_profile(), true, nullptr);
    }
}

Morsels convert_scan_range_to_morsel(const std::vector<TScanRangeParams>& scan_ranges, int node_id) {
    Morsels morsels;
    for (const auto& scan_range : scan_ranges) {
        morsels.emplace_back(std::make_unique<ScanMorsel>(node_id, scan_range));
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
    auto&& existing_query_ctx = exec_env->query_context_mgr()->get(query_id);
    if (existing_query_ctx) {
        auto&& existingfragment_ctx = existing_query_ctx->fragment_mgr()->get(fragment_instance_id);
        if (existingfragment_ctx) {
            return Status::DuplicateRpcInvocation("Duplicate invocations of exec_plan_fragment");
        }
    }

    bool prepare_success = false;
    int64_t prepare_time = 0;
    DeferOp defer([this, &request, &prepare_success, &prepare_time]() {
        if (prepare_success) {
            auto fragment_ctx = _query_ctx->fragment_mgr()->get(request.params.fragment_instance_id);
            auto* prepare_timer = fragment_ctx->runtime_state()->runtime_profile()->add_counter(
                    "FragmentInstancePrepareTime", TUnit::TIME_NS);
            COUNTER_SET(prepare_timer, prepare_time);
        } else {
            _fail_cleanup();
        }
    });

    SCOPED_RAW_TIMER(&prepare_time);
    _query_ctx = exec_env->query_context_mgr()->get_or_register(query_id);
    _query_ctx->set_exec_env(exec_env);
    if (params.__isset.instances_number) {
        _query_ctx->set_total_fragments(params.instances_number);
    }

    _query_ctx->set_delivery_expire_seconds(_calc_delivery_expired_seconds(request));
    _query_ctx->set_query_expire_seconds(_calc_query_expired_seconds(request));
    // initialize query's deadline
    _query_ctx->extend_delivery_lifetime();
    _query_ctx->extend_query_lifetime();

    auto fragment_ctx = std::make_unique<FragmentContext>();

    fragment_ctx->set_query_id(query_id);
    fragment_ctx->set_fragment_instance_id(fragment_instance_id);
    fragment_ctx->set_fe_addr(coord);

    if (query_options.__isset.is_report_success && query_options.is_report_success) {
        fragment_ctx->set_report_profile();
    }
    if (query_options.__isset.pipeline_profile_level) {
        fragment_ctx->set_profile_level(query_options.pipeline_profile_level);
    }

    LOG(INFO) << "Prepare(): query_id=" << print_id(query_id)
              << " fragment_instance_id=" << print_id(params.fragment_instance_id) << " backend_num=" << backend_num;

    WorkGroupPtr wg = nullptr;
    // wg is always non-nullable, when request.enable_resource_group is true.
    if (request.__isset.enable_resource_group && request.enable_resource_group) {
        fragment_ctx->set_enable_resource_group();
        if (request.__isset.workgroup && request.workgroup.id != WorkGroup::DEFAULT_WG_ID) {
            wg = std::make_shared<WorkGroup>(request.workgroup);
            wg = WorkGroupManager::instance()->add_workgroup(wg);
        } else {
            wg = WorkGroupManager::instance()->get_default_workgroup();
        }
        DCHECK(wg != nullptr);
    }

    int32_t degree_of_parallelism = 1;
    if (request.__isset.pipeline_dop && request.pipeline_dop > 0) {
        degree_of_parallelism = request.pipeline_dop;
    } else {
        // default dop is a half of the number of hardware threads.
        degree_of_parallelism = std::max<int32_t>(1, std::thread::hardware_concurrency() / 2);
    }

    fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(query_id, fragment_instance_id, query_options, query_globals, exec_env));

    auto* parent_mem_tracker = wg != nullptr ? wg->mem_tracker() : exec_env->query_pool_mem_tracker();
    auto per_instance_mem_limit = query_options.__isset.mem_limit ? query_options.mem_limit : -1;
    int64_t query_mem_limit = _query_ctx->compute_query_mem_limit(parent_mem_tracker->limit(), per_instance_mem_limit,
                                                                  degree_of_parallelism);
    _query_ctx->init_mem_tracker(query_mem_limit, parent_mem_tracker);

    auto query_mem_tracker = _query_ctx->mem_tracker();
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(query_mem_tracker.get());

    auto* runtime_state = fragment_ctx->runtime_state();
    int func_version = request.__isset.func_version ? request.func_version : 2;
    runtime_state->set_func_version(func_version);
    runtime_state->init_mem_trackers(query_mem_tracker);
    runtime_state->set_be_number(request.backend_num);
    runtime_state->set_query_ctx(_query_ctx);

    // RuntimeFilterWorker::open_query is idempotent
    if (params.__isset.runtime_filter_params && params.runtime_filter_params.id_to_prober_params.size() != 0) {
        _query_ctx->set_is_runtime_filter_coordinator(true);
        exec_env->runtime_filter_worker()->open_query(query_id, request.query_options, params.runtime_filter_params,
                                                      true);
    }
    fragment_ctx->prepare_pass_through_chunk_buffer();

    auto* obj_pool = runtime_state->obj_pool();
    // Set up desc tbl
    DescriptorTbl* desc_tbl = nullptr;
    if (t_desc_tbl.__isset.is_cached) {
        if (t_desc_tbl.is_cached) {
            desc_tbl = _query_ctx->desc_tbl();
            if (desc_tbl == nullptr) {
                return Status::Cancelled("Query terminates prematurely");
            }
        } else {
            RETURN_IF_ERROR(DescriptorTbl::create(_query_ctx->object_pool(), t_desc_tbl, &desc_tbl,
                                                  runtime_state->chunk_size()));
            _query_ctx->set_desc_tbl(desc_tbl);
        }
    } else {
        RETURN_IF_ERROR(DescriptorTbl::create(obj_pool, t_desc_tbl, &desc_tbl, runtime_state->chunk_size()));
    }
    runtime_state->set_desc_tbl(desc_tbl);
    // Set up plan
    RETURN_IF_ERROR(ExecNode::create_tree(runtime_state, obj_pool, fragment.plan, *desc_tbl, &fragment_ctx->plan()));
    ExecNode* plan = fragment_ctx->plan();
    plan->push_down_join_runtime_filter_recursively(runtime_state);
    std::vector<TupleSlotMapping> empty_mappings;
    plan->push_down_tuple_slot_mappings(runtime_state, empty_mappings);
    runtime_state->set_fragment_root_id(plan->id());

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

    // set scan ranges
    std::vector<ExecNode*> scan_nodes;
    std::vector<TScanRangeParams> no_scan_ranges;
    plan->collect_scan_nodes(&scan_nodes);

    MorselQueueMap& morsel_queues = fragment_ctx->morsel_queues();
    for (auto& i : scan_nodes) {
        ScanNode* scan_node = down_cast<ScanNode*>(i);
        const std::vector<TScanRangeParams>& scan_ranges =
                FindWithDefault(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
        Morsels morsels = convert_scan_range_to_morsel(scan_ranges, scan_node->id());
        morsel_queues.emplace(scan_node->id(), std::make_unique<MorselQueue>(std::move(morsels)));
    }

    PipelineBuilderContext context(fragment_ctx.get(), degree_of_parallelism);
    PipelineBuilder builder(context);
    fragment_ctx->set_pipelines(builder.build(*fragment_ctx, plan));
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

    RETURN_IF_ERROR(fragment_ctx->prepare_all_pipelines());

    Drivers drivers;
    const auto& pipelines = fragment_ctx->pipelines();
    const size_t num_pipelines = pipelines.size();
    size_t driver_id = 0;
    for (auto n = 0; n < num_pipelines; ++n) {
        const auto& pipeline = pipelines[n];
        // DOP(degree of parallelism) of Pipeline's SourceOperator determines the Pipeline's DOP.
        const auto cur_pipeline_dop = pipeline->source_operator_factory()->degree_of_parallelism();
        VLOG_ROW << "Pipeline " << pipeline->to_readable_string() << " parallel=" << cur_pipeline_dop
                 << " fragment_instance_id=" << print_id(params.fragment_instance_id);

        // If pipeline's SourceOperator is with morsels, a MorselQueue is added to the SourceOperator.
        // at present, only OlapScanOperator need a MorselQueue attached.
        setup_profile_hierarchy(runtime_state, pipeline);
        if (pipeline->source_operator_factory()->with_morsels()) {
            auto source_id = pipeline->get_op_factories()[0]->plan_node_id();
            DCHECK(morsel_queues.count(source_id));
            auto& morsel_queue = morsel_queues[source_id];
            DCHECK(morsel_queue->num_morsels() == 0 || cur_pipeline_dop <= morsel_queue->num_morsels());
            std::vector<MorselQueuePtr> morsel_queue_per_driver = morsel_queue->split_by_size(cur_pipeline_dop);
            DCHECK(morsel_queue_per_driver.size() == cur_pipeline_dop);

            for (size_t i = 0; i < cur_pipeline_dop; ++i) {
                auto&& operators = pipeline->create_operators(cur_pipeline_dop, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), _query_ctx,
                                                                    fragment_ctx.get(), driver_id++);
                driver->set_morsel_queue(std::move(morsel_queue_per_driver[i]));
                auto* scan_operator = down_cast<ScanOperator*>(driver->source_operator());
                if (wg != nullptr) {
                    // Workgroup uses scan_executor instead of pipeline_scan_io_thread_pool.
                    scan_operator->set_workgroup(wg);
                } else {
                    scan_operator->set_io_threads(exec_env->pipeline_scan_io_thread_pool());
                }
                setup_profile_hierarchy(pipeline, driver);
                drivers.emplace_back(std::move(driver));
            }
        } else {
            for (size_t i = 0; i < cur_pipeline_dop; ++i) {
                auto&& operators = pipeline->create_operators(cur_pipeline_dop, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), _query_ctx,
                                                                    fragment_ctx.get(), driver_id++);
                setup_profile_hierarchy(pipeline, driver);
                drivers.emplace_back(std::move(driver));
            }
        }
    }
    // The pipeline created later should be placed in the front
    runtime_state->runtime_profile()->reverse_childs();
    fragment_ctx->set_drivers(std::move(drivers));

    auto maybe_driver_token = exec_env->driver_limiter()->try_acquire(fragment_ctx->drivers().size());
    if (maybe_driver_token.ok()) {
        fragment_ctx->set_driver_token(std::move(maybe_driver_token.value()));
    } else {
        return maybe_driver_token.status();
    }

    if (wg != nullptr) {
        for (auto& driver : fragment_ctx->drivers()) {
            driver->set_workgroup(wg);
        }
    }

    _fragment_ctx = fragment_ctx.get();
    _query_ctx->fragment_mgr()->register_ctx(fragment_instance_id, std::move(fragment_ctx));
    prepare_success = true;
    return Status::OK();
}

Status FragmentExecutor::execute(ExecEnv* exec_env) {
    bool prepare_success = false;
    DeferOp defer([this, &prepare_success]() {
        if (!prepare_success) {
            _fail_cleanup();
        }
    });

    for (const auto& driver : _fragment_ctx->drivers()) {
        RETURN_IF_ERROR(driver->prepare(_fragment_ctx->runtime_state()));
    }
    prepare_success = true;

    if (_fragment_ctx->enable_resource_group()) {
        for (const auto& driver : _fragment_ctx->drivers()) {
            exec_env->wg_driver_executor()->submit(driver.get());
        }
    } else {
        for (const auto& driver : _fragment_ctx->drivers()) {
            exec_env->driver_executor()->submit(driver.get());
        }
    }

    return Status::OK();
}

void FragmentExecutor::_fail_cleanup() {
    if (_query_ctx) {
        if (_fragment_ctx != nullptr) {
            _fragment_ctx->destroy_pass_through_chunk_buffer();
            _query_ctx->fragment_mgr()->unregister(_fragment_ctx->fragment_instance_id());
        }
        if (_query_ctx->count_down_fragments()) {
            auto query_id = _query_ctx->query_id();
            ExecEnv::GetInstance()->query_context_mgr()->remove(query_id);
        }
    }
}

void FragmentExecutor::_decompose_data_sink_to_operator(RuntimeState* runtime_state, PipelineBuilderContext* context,
                                                        const TDataSink& t_datasink, DataSink* datasink) {
    auto fragment_ctx = context->fragment_context();
    if (typeid(*datasink) == typeid(starrocks::ResultSink)) {
        starrocks::ResultSink* result_sink = down_cast<starrocks::ResultSink*>(datasink);
        // Result sink doesn't have plan node id;
        OpFactoryPtr op =
                std::make_shared<ResultSinkOperatorFactory>(context->next_operator_id(), result_sink->get_sink_type(),
                                                            result_sink->get_output_exprs(), fragment_ctx);
        // Add result sink operator to last pipeline
        fragment_ctx->pipelines().back()->add_op_factory(op);
    } else if (typeid(*datasink) == typeid(starrocks::DataStreamSender)) {
        starrocks::DataStreamSender* sender = down_cast<starrocks::DataStreamSender*>(datasink);
        auto dop = fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();
        auto& t_stream_sink = t_datasink.stream_sink;
        bool is_dest_merge = false;
        if (t_stream_sink.__isset.is_merge && t_stream_sink.is_merge) {
            is_dest_merge = true;
        }
        bool is_pipeline_level_shuffle = false;
        int32_t dest_dop = -1;
        if (sender->get_partition_type() == TPartitionType::HASH_PARTITIONED ||
            sender->get_partition_type() == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
            dest_dop = t_stream_sink.dest_dop;

            // UNPARTITIONED mode will be performed if both num of destination and dest dop is 1
            // So we only enable pipeline level shuffle when num of destination or dest dop is greater than 1
            if (sender->destinations().size() > 1 || dest_dop > 1) {
                is_pipeline_level_shuffle = true;
            }
            DCHECK_GT(dest_dop, 0);
        }

        std::shared_ptr<SinkBuffer> sink_buffer =
                std::make_shared<SinkBuffer>(fragment_ctx, sender->destinations(), is_dest_merge, dop);

        OpFactoryPtr exchange_sink = std::make_shared<ExchangeSinkOperatorFactory>(
                context->next_operator_id(), t_stream_sink.dest_node_id, sink_buffer, sender->get_partition_type(),
                sender->destinations(), is_pipeline_level_shuffle, dest_dop, sender->sender_id(),
                sender->get_dest_node_id(), sender->get_partition_exprs(), sender->get_enable_exchange_pass_through(),
                fragment_ctx);
        fragment_ctx->pipelines().back()->add_op_factory(exchange_sink);

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
            fragment_ctx->pipelines().back()->add_op_factory(sink_op);
        }

        // ==== create source/sink pipelines ====
        for (size_t i = 0; i < sinks.size(); i++) {
            const auto& sender = sinks[i];
            OpFactories ops;
            // it's okary to set arbitrary dop.
            const size_t dop = 1;
            // TODO(hcf) set dest dop properly
            bool is_pipeline_level_shuffle = false;
            auto dest_dop = context->degree_of_parallelism();
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
            auto sink_buffer = std::make_shared<SinkBuffer>(fragment_ctx, sender->destinations(), is_dest_merge, dop);
            auto sink_op = std::make_shared<ExchangeSinkOperatorFactory>(
                    context->next_operator_id(), t_stream_sink.dest_node_id, sink_buffer, sender->get_partition_type(),
                    sender->destinations(), is_pipeline_level_shuffle, dest_dop, sender->sender_id(),
                    sender->get_dest_node_id(), sender->get_partition_exprs(),
                    sender->get_enable_exchange_pass_through(), fragment_ctx);

            ops.emplace_back(source_op);
            ops.emplace_back(sink_op);
            auto pp = std::make_shared<Pipeline>(context->next_pipe_id(), ops);
            fragment_ctx->pipelines().emplace_back(pp);
        }
    }
}

int FragmentExecutor::_calc_delivery_expired_seconds(const TExecPlanFragmentParams& request) const {
    const auto& query_options = request.query_options;

    int expired_seconds = QueryContext::DEFAULT_EXPIRE_SECONDS;
    if (query_options.__isset.query_delivery_timeout) {
        if (query_options.__isset.query_timeout) {
            expired_seconds = std::min(query_options.query_timeout, query_options.query_delivery_timeout);
        } else {
            expired_seconds = query_options.query_delivery_timeout;
        }
    } else if (query_options.__isset.query_timeout) {
        expired_seconds = query_options.query_timeout;
    }

    return std::max<int>(1, expired_seconds);
}

int FragmentExecutor::_calc_query_expired_seconds(const TExecPlanFragmentParams& request) const {
    const auto& query_options = request.query_options;

    if (query_options.__isset.query_timeout) {
        return std::max<int>(1, query_options.query_timeout);
    }

    return QueryContext::DEFAULT_EXPIRE_SECONDS;
}

} // namespace starrocks::pipeline
