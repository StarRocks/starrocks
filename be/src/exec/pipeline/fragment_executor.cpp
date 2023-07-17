// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/fragment_executor.h"

#include <unordered_map>

#include "common/config.h"
#include "exec/exchange_node.h"
#include "exec/pipeline/exchange/exchange_sink_operator.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/olap_table_sink_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/result_sink_operator.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/scan_node.h"
#include "exec/tablet_sink.h"
#include "exec/vectorized/cross_join_node.h"
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
#include "util/runtime_profile.h"
#include "util/time.h"
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
    auto* dop_counter = ADD_COUNTER_SKIP_MERGE(pipeline->runtime_profile(), "DegreeOfParallelism", TUnit::UNIT);
    COUNTER_SET(dop_counter, static_cast<int64_t>(pipeline->source_operator_factory()->degree_of_parallelism()));
    auto* total_dop_counter = ADD_COUNTER(pipeline->runtime_profile(), "TotalDegreeOfParallelism", TUnit::UNIT);
    COUNTER_SET(total_dop_counter, dop_counter->value());
    auto& operators = driver->operators();
    for (int32_t i = operators.size() - 1; i >= 0; --i) {
        auto& curr_op = operators[i];
        driver->runtime_profile()->add_child(curr_op->get_runtime_profile(), true, nullptr);
    }
}

Status FragmentExecutor::_prepare_query_ctx(ExecEnv* exec_env, const TExecPlanFragmentParams& request) {
    // prevent an identical fragment instance from multiple execution caused by FE's
    // duplicate invocations of rpc exec_plan_fragment.
    const auto& params = request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_instance_id = params.fragment_instance_id;
    const auto& query_options = request.query_options;

    auto&& existing_query_ctx = exec_env->query_context_mgr()->get(query_id);
    if (existing_query_ctx) {
        auto&& existingfragment_ctx = existing_query_ctx->fragment_mgr()->get(fragment_instance_id);
        if (existingfragment_ctx) {
            return Status::DuplicateRpcInvocation("Duplicate invocations of exec_plan_fragment");
        }
    }

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

    if (query_options.__isset.is_report_success && query_options.is_report_success) {
        _query_ctx->set_report_profile();
    }
    if (query_options.__isset.pipeline_profile_level) {
        _query_ctx->set_profile_level(query_options.pipeline_profile_level);
    }

    return Status::OK();
}

Status FragmentExecutor::_prepare_fragment_ctx(const TExecPlanFragmentParams& request) {
    const auto& coord = request.coord;
    const auto& params = request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_instance_id = params.fragment_instance_id;

    _fragment_ctx = std::make_shared<FragmentContext>();

    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_instance_id);
    _fragment_ctx->set_fe_addr(coord);

    LOG(INFO) << "Prepare(): query_id=" << print_id(query_id)
              << " fragment_instance_id=" << print_id(params.fragment_instance_id)
              << " backend_num=" << request.backend_num;

    return Status::OK();
}

Status FragmentExecutor::_prepare_workgroup(const TExecPlanFragmentParams& request) {
    // wg is always non-nullable, when request.enable_resource_group is true.
    WorkGroupPtr wg = nullptr;
    if (request.__isset.enable_resource_group && request.enable_resource_group) {
        _fragment_ctx->set_enable_resource_group();
        if (request.__isset.workgroup && request.workgroup.id != WorkGroup::DEFAULT_WG_ID) {
            wg = std::make_shared<WorkGroup>(request.workgroup);
            wg = WorkGroupManager::instance()->add_workgroup(wg);
        } else {
            wg = WorkGroupManager::instance()->get_default_workgroup();
        }
        DCHECK(wg != nullptr);
        RETURN_IF_ERROR(_query_ctx->init_query_once(wg.get()));
        _wg = wg;
    }
    DCHECK(!_fragment_ctx->enable_resource_group() || _wg != nullptr);

    return Status::OK();
}

Status FragmentExecutor::_prepare_runtime_state(ExecEnv* exec_env, const TExecPlanFragmentParams& request) {
    const auto& params = request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_instance_id = params.fragment_instance_id;
    const auto& query_globals = request.query_globals;
    const auto& query_options = request.query_options;
    const auto& t_desc_tbl = request.desc_tbl;
    const int32_t degree_of_parallelism = _calc_dop(exec_env, request);
    auto& wg = _wg;

    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(query_id, fragment_instance_id, query_options, query_globals, exec_env));
    auto* runtime_state = _fragment_ctx->runtime_state();
    runtime_state->set_enable_pipeline_engine(true);

    auto* parent_mem_tracker = wg != nullptr ? wg->mem_tracker() : exec_env->query_pool_mem_tracker();
    auto per_instance_mem_limit = query_options.__isset.mem_limit ? query_options.mem_limit : -1;
    auto option_query_mem_limit = query_options.__isset.query_mem_limit ? query_options.query_mem_limit : -1;
    int64_t query_mem_limit = _query_ctx->compute_query_mem_limit(parent_mem_tracker->limit(), per_instance_mem_limit,
                                                                  degree_of_parallelism, option_query_mem_limit);
    int64_t big_query_mem_limit = wg != nullptr && wg->use_big_query_mem_limit() ? wg->big_query_mem_limit() : -1;
    _query_ctx->init_mem_tracker(query_mem_limit, parent_mem_tracker, big_query_mem_limit, wg.get());

    auto query_mem_tracker = _query_ctx->mem_tracker();
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(query_mem_tracker.get());

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
    _fragment_ctx->prepare_pass_through_chunk_buffer();

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

    return Status::OK();
}

int32_t FragmentExecutor::_calc_dop(ExecEnv* exec_env, const TExecPlanFragmentParams& request) const {
    int32_t degree_of_parallelism = request.__isset.pipeline_dop ? request.pipeline_dop : 0;
    return exec_env->calc_pipeline_dop(degree_of_parallelism);
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

Status FragmentExecutor::_prepare_exec_plan(ExecEnv* exec_env, const TExecPlanFragmentParams& request) {
    auto* runtime_state = _fragment_ctx->runtime_state();
    auto* obj_pool = runtime_state->obj_pool();
    const DescriptorTbl& desc_tbl = runtime_state->desc_tbl();
    const auto& params = request.params;
    const auto& fragment = request.fragment;
    const int chunk_size = runtime_state->chunk_size();

    // Set up plan
    RETURN_IF_ERROR(ExecNode::create_tree(runtime_state, obj_pool, fragment.plan, desc_tbl, &_fragment_ctx->plan()));
    ExecNode* plan = _fragment_ctx->plan();
    plan->push_down_join_runtime_filter_recursively(runtime_state);
    std::vector<TupleSlotMapping> empty_mappings;
    plan->push_down_tuple_slot_mappings(runtime_state, empty_mappings);
    runtime_state->set_fragment_root_id(plan->id());

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

    MorselQueueMap& morsel_queues = _fragment_ctx->morsel_queues();
    for (auto& i : scan_nodes) {
        ScanNode* scan_node = down_cast<ScanNode*>(i);
        const std::vector<TScanRangeParams>& scan_ranges =
                FindWithDefault(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
        ASSIGN_OR_RETURN(MorselQueuePtr morsel_queue,
                         scan_node->convert_scan_range_to_morsel_queue(scan_ranges, scan_node->id(), request));
        morsel_queues.emplace(scan_node->id(), std::move(morsel_queue));
    }

    int dop = exec_env->calc_pipeline_dop(request.pipeline_dop);
    int64_t logical_scan_limit = 0;
    int64_t physical_scan_limit = 0;
    for (auto& i : scan_nodes) {
        auto* scan_node = down_cast<ScanNode*>(i);
        if (scan_node->limit() > 0) {
            // The upper bound of records we actually will scan is `limit * dop * io_parallelism`.
            // For SQL like: select * from xxx limit 5, the underlying scan_limit should be 5 * parallelism
            // Otherwise this SQL would exceed the bigquery_rows_limit due to underlying IO parallelization.
            // Some chunk sources scan `chunk_size` rows at a time, so normalize `limit` to be rounded up to `chunk_size`.
            logical_scan_limit += scan_node->limit();
            int64_t normalized_limit = (scan_node->limit() + chunk_size - 1) / chunk_size * chunk_size;
            physical_scan_limit += normalized_limit * dop * scan_node->io_tasks_per_scan_operator();
        } else {
            // Not sure how many rows will be scan.
            logical_scan_limit = -1;
            break;
        }
    }

    if (_wg && _wg->big_query_scan_rows_limit() > 0) {
        if (logical_scan_limit >= 0 && logical_scan_limit <= _wg->big_query_scan_rows_limit() &&
            physical_scan_limit > _wg->big_query_scan_rows_limit()) {
            _query_ctx->set_scan_limit(physical_scan_limit);
        } else {
            _query_ctx->set_scan_limit(_wg->big_query_scan_rows_limit());
        }
    }

    return Status::OK();
}

Status FragmentExecutor::_prepare_pipeline_driver(ExecEnv* exec_env, const TExecPlanFragmentParams& request) {
    const auto fragment_instance_id = request.params.fragment_instance_id;
    const auto degree_of_parallelism = _calc_dop(exec_env, request);
    const auto& fragment = request.fragment;
    ExecNode* plan = _fragment_ctx->plan();

    Drivers drivers;
    MorselQueueMap& morsel_queues = _fragment_ctx->morsel_queues();
    auto* runtime_state = _fragment_ctx->runtime_state();
    const auto& pipelines = _fragment_ctx->pipelines();

    // Build pipelines
    PipelineBuilderContext context(_fragment_ctx.get(), degree_of_parallelism);
    PipelineBuilder builder(context);
    _fragment_ctx->set_pipelines(builder.build(*_fragment_ctx, plan));

    // Set up sink if required
    std::unique_ptr<DataSink> datasink;
    if (fragment.__isset.output_sink) {
        if (fragment.output_sink.type == TDataSinkType::RESULT_SINK) {
            _query_ctx->set_result_sink(true);
        }
        RowDescriptor row_desc;
        RETURN_IF_ERROR(DataSink::create_data_sink(runtime_state, request.fragment.output_sink,
                                                   request.fragment.output_exprs, request.params, row_desc, &datasink));
        RuntimeProfile* sink_profile = datasink->profile();
        if (sink_profile != nullptr) {
            runtime_state->runtime_profile()->add_child(sink_profile, true, nullptr);
        }
        RETURN_IF_ERROR(_decompose_data_sink_to_operator(runtime_state, &context, request, datasink));
    }
    RETURN_IF_ERROR(_fragment_ctx->prepare_all_pipelines());

    size_t driver_id = 0;
    for (auto n = 0; n < pipelines.size(); ++n) {
        const auto& pipeline = pipelines[n];
        // DOP(degree of parallelism) of Pipeline's SourceOperator determines the Pipeline's DOP.
        const auto cur_pipeline_dop = pipeline->source_operator_factory()->degree_of_parallelism();
        VLOG_ROW << "Pipeline " << pipeline->to_readable_string() << " parallel=" << cur_pipeline_dop
                 << " fragment_instance_id=" << print_id(fragment_instance_id);

        // If pipeline's SourceOperator is with morsels, a MorselQueue is added to the SourceOperator.
        // at present, only OlapScanOperator need a MorselQueue attached.
        setup_profile_hierarchy(runtime_state, pipeline);
        if (pipeline->source_operator_factory()->with_morsels()) {
            auto source_id = pipeline->get_op_factories()[0]->plan_node_id();
            DCHECK(morsel_queues.count(source_id));
            auto& morsel_queue = morsel_queues[source_id];
            DCHECK(morsel_queue->max_degree_of_parallelism() == 0 ||
                   cur_pipeline_dop <= morsel_queue->max_degree_of_parallelism());

            for (size_t i = 0; i < cur_pipeline_dop; ++i) {
                auto&& operators = pipeline->create_operators(cur_pipeline_dop, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), _query_ctx,
                                                                    _fragment_ctx.get(), driver_id++);
                driver->set_morsel_queue(morsel_queue.get());
                if (auto* scan_operator = driver->source_scan_operator()) {
                    scan_operator->set_query_ctx(_query_ctx->get_shared_ptr());
                    if (_wg != nullptr) {
                        scan_operator->set_workgroup(_wg);

                        if (dynamic_cast<ConnectorScanOperator*>(scan_operator) != nullptr) {
                            scan_operator->set_scan_executor(exec_env->hdfs_scan_executor_with_workgroup());
                        } else {
                            scan_operator->set_scan_executor(exec_env->scan_executor_with_workgroup());
                        }
                    } else {
                        if (dynamic_cast<ConnectorScanOperator*>(scan_operator) != nullptr) {
                            scan_operator->set_scan_executor(exec_env->hdfs_scan_executor_without_workgroup());
                        } else {
                            scan_operator->set_scan_executor(exec_env->scan_executor_without_workgroup());
                        }
                    }
                }

                setup_profile_hierarchy(pipeline, driver);
                drivers.emplace_back(std::move(driver));
            }
        } else {
            for (size_t i = 0; i < cur_pipeline_dop; ++i) {
                auto&& operators = pipeline->create_operators(cur_pipeline_dop, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), _query_ctx,
                                                                    _fragment_ctx.get(), driver_id++);
                setup_profile_hierarchy(pipeline, driver);
                drivers.emplace_back(std::move(driver));
            }
        }
    }
    // The pipeline created later should be placed in the front
    runtime_state->runtime_profile()->reverse_childs();
    _fragment_ctx->set_drivers(std::move(drivers));

    // Acquire driver token to avoid overload
    auto maybe_driver_token = exec_env->driver_limiter()->try_acquire(_fragment_ctx->drivers().size());
    if (maybe_driver_token.ok()) {
        _fragment_ctx->set_driver_token(std::move(maybe_driver_token.value()));
    } else {
        return maybe_driver_token.status();
    }

    if (_wg != nullptr) {
        for (auto& driver : _fragment_ctx->drivers()) {
            driver->set_workgroup(_wg);
        }
    }

    return Status::OK();
}

Status FragmentExecutor::_prepare_global_dict(const TExecPlanFragmentParams& request) {
    // Set up global dict
    auto* runtime_state = _fragment_ctx->runtime_state();
    if (request.fragment.__isset.query_global_dicts) {
        RETURN_IF_ERROR(runtime_state->init_query_global_dict(request.fragment.query_global_dicts));
    }

    if (request.fragment.__isset.load_global_dicts) {
        RETURN_IF_ERROR(runtime_state->init_load_global_dict(request.fragment.load_global_dicts));
    }
    return Status::OK();
}

Status FragmentExecutor::prepare(ExecEnv* exec_env, const TExecPlanFragmentParams& request) {
    DCHECK(request.__isset.desc_tbl);
    DCHECK(request.__isset.fragment);

    bool prepare_success = false;
    struct {
        int64_t prepare_time = 0;
        int64_t prepare_query_ctx_time = 0;
        int64_t prepare_fragment_ctx_time = 0;
        int64_t prepare_runtime_state_time = 0;
        int64_t prepare_pipeline_driver_time = 0;
    } profiler;

    DeferOp defer([this, &request, &prepare_success, &profiler]() {
        if (prepare_success) {
            auto fragment_ctx = _query_ctx->fragment_mgr()->get(request.params.fragment_instance_id);
            auto* prepare_timer =
                    ADD_TIMER(fragment_ctx->runtime_state()->runtime_profile(), "FragmentInstancePrepareTime");
            COUNTER_SET(prepare_timer, profiler.prepare_time);
            auto* prepare_query_ctx_timer =
                    ADD_CHILD_TIMER_THESHOLD(fragment_ctx->runtime_state()->runtime_profile(), "prepare-query-ctx",
                                             "FragmentInstancePrepareTime", 10_ms);
            COUNTER_SET(prepare_query_ctx_timer, profiler.prepare_query_ctx_time);

            auto* prepare_fragment_ctx_timer =
                    ADD_CHILD_TIMER_THESHOLD(fragment_ctx->runtime_state()->runtime_profile(), "prepare-fragment-ctx",
                                             "FragmentInstancePrepareTime", 10_ms);
            COUNTER_SET(prepare_fragment_ctx_timer, profiler.prepare_fragment_ctx_time);

            auto* prepare_runtime_state_timer =
                    ADD_CHILD_TIMER_THESHOLD(fragment_ctx->runtime_state()->runtime_profile(), "prepare-runtime-state",
                                             "FragmentInstancePrepareTime", 10_ms);
            COUNTER_SET(prepare_runtime_state_timer, profiler.prepare_runtime_state_time);

            auto* prepare_pipeline_driver_timer =
                    ADD_CHILD_TIMER_THESHOLD(fragment_ctx->runtime_state()->runtime_profile(),
                                             "prepare-pipeline-driver", "FragmentInstancePrepareTime", 10_ms);
            COUNTER_SET(prepare_pipeline_driver_timer, profiler.prepare_runtime_state_time);
        } else {
            _fail_cleanup();
        }
    });

    SCOPED_RAW_TIMER(&profiler.prepare_time);
    RETURN_IF_ERROR(exec_env->query_pool_mem_tracker()->check_mem_limit("Start execute plan fragment."));
    {
        SCOPED_RAW_TIMER(&profiler.prepare_query_ctx_time);
        RETURN_IF_ERROR(_prepare_query_ctx(exec_env, request));
    }
    {
        SCOPED_RAW_TIMER(&profiler.prepare_fragment_ctx_time);
        RETURN_IF_ERROR(_prepare_fragment_ctx(request));
    }
    {
        SCOPED_RAW_TIMER(&profiler.prepare_runtime_state_time);
        RETURN_IF_ERROR(_prepare_workgroup(request));
        RETURN_IF_ERROR(_prepare_runtime_state(exec_env, request));
        RETURN_IF_ERROR(_prepare_exec_plan(exec_env, request));
        RETURN_IF_ERROR(_prepare_global_dict(request));
    }
    {
        SCOPED_RAW_TIMER(&profiler.prepare_pipeline_driver_time);
        RETURN_IF_ERROR(_prepare_pipeline_driver(exec_env, request));
    }

    _query_ctx->fragment_mgr()->register_ctx(request.params.fragment_instance_id, _fragment_ctx);
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

    auto* executor =
            _fragment_ctx->enable_resource_group() ? exec_env->wg_driver_executor() : exec_env->driver_executor();
    for (const auto& driver : _fragment_ctx->drivers()) {
        DCHECK(!_fragment_ctx->enable_resource_group() || driver->workgroup() != nullptr);
        executor->submit(driver.get());
    }

    return Status::OK();
}

void FragmentExecutor::_fail_cleanup() {
    if (_query_ctx) {
        if (_fragment_ctx) {
            _query_ctx->fragment_mgr()->unregister(_fragment_ctx->fragment_instance_id());
            _fragment_ctx->destroy_pass_through_chunk_buffer();
            _fragment_ctx.reset();
        }
        if (_query_ctx->count_down_fragments()) {
            auto query_id = _query_ctx->query_id();
            ExecEnv::GetInstance()->query_context_mgr()->remove(query_id);
        }
    }
}

std::shared_ptr<ExchangeSinkOperatorFactory> _create_exchange_sink_operator(PipelineBuilderContext* context,
                                                                            const TDataStreamSink& stream_sink,
                                                                            const DataStreamSender* sender,
                                                                            size_t dop) {
    auto fragment_ctx = context->fragment_context();

    bool is_dest_merge = stream_sink.__isset.is_merge && stream_sink.is_merge;

    bool is_pipeline_level_shuffle = false;
    int32_t dest_dop = -1;
    if (sender->get_partition_type() == TPartitionType::HASH_PARTITIONED ||
        sender->get_partition_type() == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
        dest_dop = stream_sink.dest_dop;

        // UNPARTITIONED mode will be performed if both num of destination and dest dop is 1
        // So we only enable pipeline level shuffle when num of destination or dest dop is greater than 1
        if (sender->destinations().size() > 1 || dest_dop > 1) {
            is_pipeline_level_shuffle = true;
        }
        DCHECK_GT(dest_dop, 0);
    }

    std::shared_ptr<SinkBuffer> sink_buffer =
            std::make_shared<SinkBuffer>(fragment_ctx, sender->destinations(), is_dest_merge, dop);

    auto exchange_sink = std::make_shared<ExchangeSinkOperatorFactory>(
            context->next_operator_id(), stream_sink.dest_node_id, sink_buffer, sender->get_partition_type(),
            sender->destinations(), is_pipeline_level_shuffle, dest_dop, sender->sender_id(),
            sender->get_dest_node_id(), sender->get_partition_exprs(), sender->get_enable_exchange_pass_through(),
            fragment_ctx, sender->output_columns());
    return exchange_sink;
}

Status FragmentExecutor::_decompose_data_sink_to_operator(RuntimeState* runtime_state, PipelineBuilderContext* context,
                                                          const TExecPlanFragmentParams& params,
                                                          std::unique_ptr<starrocks::DataSink>& datasink) {
    auto fragment_ctx = context->fragment_context();
    if (typeid(*datasink) == typeid(starrocks::ResultSink)) {
        ResultSink* result_sink = down_cast<starrocks::ResultSink*>(datasink.get());
        // Result sink doesn't have plan node id;
        OpFactoryPtr op =
                std::make_shared<ResultSinkOperatorFactory>(context->next_operator_id(), result_sink->get_sink_type(),
                                                            result_sink->get_output_exprs(), fragment_ctx);
        // Add result sink operator to last pipeline
        fragment_ctx->pipelines().back()->add_op_factory(op);
    } else if (typeid(*datasink) == typeid(starrocks::DataStreamSender)) {
        DataStreamSender* sender = down_cast<starrocks::DataStreamSender*>(datasink.get());
        auto dop = fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();
        auto& t_stream_sink = params.fragment.output_sink.stream_sink;
        auto exchange_sink = _create_exchange_sink_operator(context, t_stream_sink, sender, dop);
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
        MultiCastDataStreamSink* mcast_sink = down_cast<starrocks::MultiCastDataStreamSink*>(datasink.get());
        const auto& sinks = mcast_sink->get_sinks();
        auto& t_multi_case_stream_sink = params.fragment.output_sink.multi_cast_stream_sink;

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
            auto& t_stream_sink = t_multi_case_stream_sink.sinks[i];

            // source op
            auto source_op = std::make_shared<MultiCastLocalExchangeSourceOperatorFactory>(
                    context->next_operator_id(), pseudo_plan_node_id, i, mcast_local_exchanger);
            source_op->set_degree_of_parallelism(dop);

            // sink op
            auto sink_op = _create_exchange_sink_operator(context, t_stream_sink, sender.get(), dop);

            ops.emplace_back(source_op);
            ops.emplace_back(sink_op);
            auto pp = std::make_shared<Pipeline>(context->next_pipe_id(), ops);
            fragment_ctx->pipelines().emplace_back(pp);
        }
    } else if (typeid(*datasink) == typeid(starrocks::stream_load::OlapTableSink)) {
        runtime_state->set_per_fragment_instance_idx(params.params.sender_id);
        runtime_state->set_num_per_fragment_instances(params.params.num_senders);
        OpFactoryPtr op =
                std::make_shared<OlapTableSinkOperatorFactory>(context->next_operator_id(), datasink, fragment_ctx);
        fragment_ctx->pipelines().back()->add_op_factory(op);
    }

    return Status::OK();
}

} // namespace starrocks::pipeline
