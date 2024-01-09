// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
#include "exec/pipeline/sink/export_sink_operator.h"
#include "exec/pipeline/sink/file_sink_operator.h"
#include "exec/pipeline/sink/memory_scratch_sink_operator.h"
#include "exec/pipeline/sink/mysql_table_sink_operator.h"
#include "exec/scan_node.h"
#include "exec/tablet_sink.h"
#include "exec/vectorized/cross_join_node.h"
#include "exec/vectorized/olap_scan_node.h"
#include "exec/workgroup/work_group.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gutil/casts.h"
#include "gutil/map_util.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_sender.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/export_sink.h"
#include "runtime/memory_scratch_sink.h"
#include "runtime/multi_cast_data_stream_sink.h"
#include "runtime/mysql_table_sink.h"
#include "runtime/result_sink.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "util/debug/query_trace.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace starrocks::pipeline {

using WorkGroupManager = workgroup::WorkGroupManager;
using WorkGroup = workgroup::WorkGroup;
using WorkGroupPtr = workgroup::WorkGroupPtr;

/// UnifiedExecPlanFragmentParams.
const std::vector<TScanRangeParams> UnifiedExecPlanFragmentParams::_no_scan_ranges;
const PerDriverScanRangesMap UnifiedExecPlanFragmentParams::_no_scan_ranges_per_driver_seq;

const std::vector<TScanRangeParams>& UnifiedExecPlanFragmentParams::scan_ranges_of_node(TPlanNodeId node_id) const {
    return FindWithDefault(_unique_request.params.per_node_scan_ranges, node_id, _no_scan_ranges);
}

const PerDriverScanRangesMap& UnifiedExecPlanFragmentParams::per_driver_seq_scan_ranges_of_node(
        TPlanNodeId node_id) const {
    if (!_unique_request.params.__isset.node_to_per_driver_seq_scan_ranges) {
        return _no_scan_ranges_per_driver_seq;
    }

    return FindWithDefault(_unique_request.params.node_to_per_driver_seq_scan_ranges, node_id,
                           _no_scan_ranges_per_driver_seq);
}

const TDataSink& UnifiedExecPlanFragmentParams::output_sink() const {
    if (_unique_request.fragment.__isset.output_sink) {
        return _unique_request.fragment.output_sink;
    }
    return _common_request.fragment.output_sink;
}

/// Profile methods.
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
        driver->runtime_profile()->add_child(curr_op->runtime_profile(), true, nullptr);
    }
}

/// FragmentExecutor.
FragmentExecutor::FragmentExecutor() {
    _fragment_start_time = MonotonicNanos();
}

Status FragmentExecutor::_prepare_query_ctx(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request) {
    // prevent an identical fragment instance from multiple execution caused by FE's
    // duplicate invocations of rpc exec_plan_fragment.
    const auto& params = request.common().params;
    const auto& query_id = params.query_id;
    const auto& fragment_instance_id = request.fragment_instance_id();
    const auto& query_options = request.common().query_options;

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

    if (query_options.__isset.enable_profile && query_options.enable_profile) {
        _query_ctx->set_report_profile();
    }
    if (query_options.__isset.big_query_profile_second_threshold) {
        _query_ctx->set_big_query_profile_threshold(query_options.big_query_profile_second_threshold);
    }
    if (query_options.__isset.pipeline_profile_level) {
        _query_ctx->set_profile_level(query_options.pipeline_profile_level);
    }

    bool enable_query_trace = false;
    if (query_options.__isset.enable_query_debug_trace && query_options.enable_query_debug_trace) {
        enable_query_trace = true;
    }
    _query_ctx->set_query_trace(std::make_shared<starrocks::debug::QueryTrace>(query_id, enable_query_trace));

    return Status::OK();
}

Status FragmentExecutor::_prepare_fragment_ctx(const UnifiedExecPlanFragmentParams& request) {
    const auto& coord = request.common().coord;
    const auto& query_id = request.common().params.query_id;
    const auto& fragment_instance_id = request.fragment_instance_id();

    _fragment_ctx = std::make_shared<FragmentContext>();

    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_instance_id);
    _fragment_ctx->set_fe_addr(coord);

    LOG(INFO) << "Prepare(): query_id=" << print_id(query_id)
              << " fragment_instance_id=" << print_id(fragment_instance_id) << " backend_num=" << request.backend_num();

    return Status::OK();
}

Status FragmentExecutor::_prepare_workgroup(const UnifiedExecPlanFragmentParams& request) {
    // wg is always non-nullable, when request.enable_resource_group is true.
    WorkGroupPtr wg = nullptr;
    if (request.common().__isset.enable_resource_group && request.common().enable_resource_group) {
        _fragment_ctx->set_enable_resource_group();
        if (request.common().__isset.workgroup && request.common().workgroup.id != WorkGroup::DEFAULT_WG_ID) {
            wg = std::make_shared<WorkGroup>(request.common().workgroup);
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

Status FragmentExecutor::_prepare_runtime_state(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request) {
    const auto& params = request.common().params;
    const auto& query_id = params.query_id;
    const auto& fragment_instance_id = request.fragment_instance_id();
    const auto& query_globals = request.common().query_globals;
    const auto& query_options = request.common().query_options;
    const auto& t_desc_tbl = request.common().desc_tbl;
    const int32_t degree_of_parallelism = _calc_dop(exec_env, request);
    auto& wg = _wg;

    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(query_id, fragment_instance_id, query_options, query_globals, exec_env));
    auto* runtime_state = _fragment_ctx->runtime_state();
    runtime_state->set_enable_pipeline_engine(true);
    runtime_state->set_fragment_ctx(_fragment_ctx.get());

    auto* parent_mem_tracker = wg != nullptr ? wg->mem_tracker() : exec_env->query_pool_mem_tracker();
    auto per_instance_mem_limit = query_options.__isset.mem_limit ? query_options.mem_limit : -1;
    auto option_query_mem_limit = query_options.__isset.query_mem_limit ? query_options.query_mem_limit : -1;
    int64_t query_mem_limit = _query_ctx->compute_query_mem_limit(parent_mem_tracker->limit(), per_instance_mem_limit,
                                                                  degree_of_parallelism, option_query_mem_limit);
    int64_t big_query_mem_limit = wg != nullptr && wg->use_big_query_mem_limit() ? wg->big_query_mem_limit() : -1;
    _query_ctx->init_mem_tracker(query_mem_limit, parent_mem_tracker, big_query_mem_limit, wg.get());

    auto query_mem_tracker = _query_ctx->mem_tracker();
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(query_mem_tracker.get());

    int func_version = request.common().__isset.func_version ? request.common().func_version : 2;
    runtime_state->set_func_version(func_version);
    runtime_state->init_mem_trackers(query_mem_tracker);
    runtime_state->set_be_number(request.backend_num());
    runtime_state->set_query_ctx(_query_ctx);

    // RuntimeFilterWorker::open_query is idempotent
    const TRuntimeFilterParams* runtime_filter_params = nullptr;
    if (request.unique().params.__isset.runtime_filter_params &&
        !request.unique().params.runtime_filter_params.id_to_prober_params.empty()) {
        runtime_filter_params = &request.unique().params.runtime_filter_params;
    } else if (request.common().params.__isset.runtime_filter_params &&
               !request.common().params.runtime_filter_params.id_to_prober_params.empty()) {
        runtime_filter_params = &request.common().params.runtime_filter_params;
    }
    if (runtime_filter_params != nullptr) {
        _query_ctx->set_is_runtime_filter_coordinator(true);
        exec_env->runtime_filter_worker()->open_query(query_id, query_options, *runtime_filter_params, true);
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
            RETURN_IF_ERROR(DescriptorTbl::create(runtime_state, _query_ctx->object_pool(), t_desc_tbl, &desc_tbl,
                                                  runtime_state->chunk_size()));
            _query_ctx->set_desc_tbl(desc_tbl);
        }
    } else {
        RETURN_IF_ERROR(
                DescriptorTbl::create(runtime_state, obj_pool, t_desc_tbl, &desc_tbl, runtime_state->chunk_size()));
    }
    runtime_state->set_desc_tbl(desc_tbl);

    return Status::OK();
}

int32_t FragmentExecutor::_calc_dop(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request) const {
    int32_t degree_of_parallelism = request.pipeline_dop();
    return exec_env->calc_pipeline_dop(degree_of_parallelism);
}

int FragmentExecutor::_calc_delivery_expired_seconds(const UnifiedExecPlanFragmentParams& request) const {
    const auto& query_options = request.common().query_options;

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

int FragmentExecutor::_calc_query_expired_seconds(const UnifiedExecPlanFragmentParams& request) const {
    const auto& query_options = request.common().query_options;

    if (query_options.__isset.query_timeout) {
        return std::max<int>(1, query_options.query_timeout);
    }

    return QueryContext::DEFAULT_EXPIRE_SECONDS;
}

Status FragmentExecutor::_prepare_exec_plan(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request) {
    auto* runtime_state = _fragment_ctx->runtime_state();
    auto* obj_pool = runtime_state->obj_pool();
    const DescriptorTbl& desc_tbl = runtime_state->desc_tbl();
    const auto& params = request.common().params;
    const auto& fragment = request.common().fragment;
    const auto dop = _calc_dop(exec_env, request);
    const auto& query_options = request.common().query_options;
    const int chunk_size = runtime_state->chunk_size();

    bool enable_shared_scan = request.common().__isset.enable_shared_scan && request.common().enable_shared_scan;
    bool enable_tablet_internal_parallel =
            query_options.__isset.enable_tablet_internal_parallel && query_options.enable_tablet_internal_parallel;
    TTabletInternalParallelMode::type tablet_internal_parallel_mode =
            query_options.__isset.tablet_internal_parallel_mode ? query_options.tablet_internal_parallel_mode
                                                                : TTabletInternalParallelMode::type::AUTO;

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
        down_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
    }

    // set scan ranges
    std::vector<ExecNode*> scan_nodes;
    plan->collect_scan_nodes(&scan_nodes);

    MorselQueueFactoryMap& morsel_queue_factories = _fragment_ctx->morsel_queue_factories();

    if (fragment.__isset.cache_param) {
        auto const& tcache_param = fragment.cache_param;
        auto& cache_param = _fragment_ctx->cache_param();
        cache_param.plan_node_id = tcache_param.id;
        cache_param.digest = tcache_param.digest;
        cache_param.force_populate = tcache_param.force_populate;
        cache_param.entry_max_bytes = tcache_param.entry_max_bytes;
        cache_param.entry_max_rows = tcache_param.entry_max_rows;
        for (auto& [slot, remapped_slot] : tcache_param.slot_remapping) {
            cache_param.slot_remapping[slot] = remapped_slot;
            cache_param.reverse_slot_remapping[remapped_slot] = slot;
        }
        cache_param.can_use_multiversion = tcache_param.can_use_multiversion;
        cache_param.keys_type = tcache_param.keys_type;
        _fragment_ctx->set_enable_cache(true);
    }

    for (auto& i : scan_nodes) {
        auto* scan_node = down_cast<ScanNode*>(i);
        const std::vector<TScanRangeParams>& scan_ranges = request.scan_ranges_of_node(scan_node->id());

        PerDriverScanRangesMap& scan_ranges_per_driver = _fragment_ctx->scan_ranges_per_driver();
        if (scan_ranges.size() >= dop && _fragment_ctx->enable_cache()) {
            for (auto k = 0; k < scan_ranges.size(); ++k) {
                scan_ranges_per_driver[k % dop].push_back(scan_ranges[k]);
            }
        }

        const auto& scan_ranges_per_driver_seq = !scan_ranges_per_driver.empty()
                                                         ? scan_ranges_per_driver
                                                         : request.per_driver_seq_scan_ranges_of_node(scan_node->id());

        // num_lanes ranges in [1,16] in default 4.
        _fragment_ctx->cache_param().num_lanes = std::min(16, std::max(1, config::query_cache_num_lanes_per_driver));

        for (auto& [driver_seq, scan_ranges] : scan_ranges_per_driver_seq) {
            for (auto& scan_range : scan_ranges) {
                if (scan_range.scan_range.__isset.internal_scan_range && fragment.__isset.cache_param) {
                    const auto& tcache_param = fragment.cache_param;
                    auto& internal_scan_range = scan_range.scan_range.internal_scan_range;
                    auto tablet_id = internal_scan_range.tablet_id;
                    auto partition_id = internal_scan_range.partition_id;
                    if (!tcache_param.region_map.count(partition_id)) {
                        continue;
                    }
                    const auto& region = tcache_param.region_map.at(partition_id);
                    std::string cache_prefix_key;
                    cache_prefix_key.reserve(sizeof(partition_id) + region.size() + sizeof(tablet_id));
                    cache_prefix_key.insert(cache_prefix_key.end(), (uint8_t*)&partition_id,
                                            ((uint8_t*)&partition_id) + sizeof(partition_id));
                    cache_prefix_key.insert(cache_prefix_key.end(), region.begin(), region.end());
                    cache_prefix_key.insert(cache_prefix_key.end(), (uint8_t*)&tablet_id,
                                            ((uint8_t*)&tablet_id) + sizeof(tablet_id));
                    _fragment_ctx->cache_param().cache_key_prefixes[tablet_id] = std::move(cache_prefix_key);
                }
            }
        }

        if (scan_ranges_per_driver_seq.empty()) {
            _fragment_ctx->set_enable_cache(false);
        }
        // TODO (by satanson): shared_scan mechanism conflicts with per-tablet computation that is required for query
        //  cache, so it is turned off at present, it would be solved in the future.
        if (_fragment_ctx->enable_cache()) {
            enable_shared_scan = false;
        }

        ASSIGN_OR_RETURN(auto morsel_queue_factory,
                         scan_node->convert_scan_range_to_morsel_queue_factory(
                                 scan_ranges, scan_ranges_per_driver_seq, scan_node->id(), dop,
                                 enable_tablet_internal_parallel, tablet_internal_parallel_mode));
        scan_node->enable_shared_scan(enable_shared_scan && morsel_queue_factory->is_shared());
        morsel_queue_factories.emplace(scan_node->id(), std::move(morsel_queue_factory));
    }

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
        if (logical_scan_limit >= 0 && logical_scan_limit <= _wg->big_query_scan_rows_limit()) {
            _query_ctx->set_scan_limit(std::max(_wg->big_query_scan_rows_limit(), physical_scan_limit));
        } else {
            _query_ctx->set_scan_limit(_wg->big_query_scan_rows_limit());
        }
    }

    return Status::OK();
}

Status FragmentExecutor::_prepare_stream_load_pipe(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request) {
    const TExecPlanFragmentParams& unique_request = request.unique();
    if (!unique_request.params.__isset.node_to_per_driver_seq_scan_ranges) {
        return Status::OK();
    }
    const auto& scan_range_map = unique_request.params.node_to_per_driver_seq_scan_ranges;
    if (scan_range_map.size() == 0) {
        return Status::OK();
    }
    auto iter = scan_range_map.begin();
    if (iter->second.size() == 0) {
        return Status::OK();
    }
    auto iter2 = iter->second.begin();
    if (iter2->second.size() == 0) {
        return Status::OK();
    }
    if (!iter2->second[0].scan_range.__isset.broker_scan_range) {
        return Status::OK();
    }
    if (!iter2->second[0].scan_range.broker_scan_range.__isset.channel_id) {
        return Status::OK();
    }
    std::vector<StreamLoadContext*> stream_load_contexts;
    for (; iter != scan_range_map.end(); iter++) {
        for (; iter2 != iter->second.end(); iter2++) {
            for (const auto& scan_range : iter2->second) {
                const TBrokerScanRange& broker_scan_range = scan_range.scan_range.broker_scan_range;
                int channel_id = broker_scan_range.channel_id;
                const string& label = broker_scan_range.params.label;
                const string& db_name = broker_scan_range.params.db_name;
                const string& table_name = broker_scan_range.params.table_name;
                TFileFormatType::type format = broker_scan_range.ranges[0].format_type;
                TUniqueId load_id = broker_scan_range.ranges[0].load_id;
                long txn_id = broker_scan_range.params.txn_id;
                StreamLoadContext* ctx = nullptr;
                RETURN_IF_ERROR(exec_env->stream_context_mgr()->create_channel_context(
                        exec_env, label, channel_id, db_name, table_name, format, ctx, load_id, txn_id));
                DeferOp op([&] {
                    if (ctx->unref()) {
                        delete ctx;
                    }
                });
                RETURN_IF_ERROR(exec_env->stream_context_mgr()->put_channel_context(label, channel_id, ctx));
                stream_load_contexts.push_back(ctx);
            }
        }
    }
    _fragment_ctx->set_stream_load_contexts(stream_load_contexts);
    return Status::OK();
}

Status FragmentExecutor::_prepare_pipeline_driver(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request) {
    const auto& fragment_instance_id = request.fragment_instance_id();
    const auto degree_of_parallelism = _calc_dop(exec_env, request);
    const auto& fragment = request.common().fragment;
    const auto& params = request.common().params;
    ExecNode* plan = _fragment_ctx->plan();

    Drivers drivers;
    MorselQueueFactoryMap& morsel_queue_factories = _fragment_ctx->morsel_queue_factories();
    auto* runtime_state = _fragment_ctx->runtime_state();
    const auto& pipelines = _fragment_ctx->pipelines();

    // Build pipelines
    PipelineBuilderContext context(_fragment_ctx.get(), degree_of_parallelism);
    PipelineBuilder builder(context);
    _fragment_ctx->set_pipelines(builder.build(*_fragment_ctx, plan));

    // Set up sink if required
    std::unique_ptr<DataSink> datasink;
    if (request.isset_output_sink()) {
        const auto& tsink = request.output_sink();
        if (tsink.type == TDataSinkType::RESULT_SINK || tsink.type == TDataSinkType::OLAP_TABLE_SINK ||
            tsink.type == TDataSinkType::MEMORY_SCRATCH_SINK || tsink.type == TDataSinkType::EXPORT_SINK) {
            _query_ctx->set_final_sink();
        }
        RETURN_IF_ERROR(DataSink::create_data_sink(runtime_state, tsink, fragment.output_exprs, params,
                                                   request.sender_id(), plan->row_desc(), &datasink));
        RETURN_IF_ERROR(_decompose_data_sink_to_operator(runtime_state, &context, request, datasink, tsink,
                                                         fragment.output_exprs));
    }
    RETURN_IF_ERROR(_fragment_ctx->prepare_all_pipelines());

    size_t driver_id = 0;
    for (const auto& pipeline : pipelines) {
        // DOP(degree of parallelism) of Pipeline's SourceOperator determines the Pipeline's DOP.
        const auto cur_pipeline_dop = pipeline->source_operator_factory()->degree_of_parallelism();
        VLOG_ROW << "Pipeline " << pipeline->to_readable_string() << " parallel=" << cur_pipeline_dop
                 << " fragment_instance_id=" << print_id(fragment_instance_id);

        // If pipeline's SourceOperator is with morsels, a MorselQueue is added to the SourceOperator.
        // at present, only OlapScanOperator need a MorselQueue attached.
        setup_profile_hierarchy(runtime_state, pipeline);
        if (pipeline->source_operator_factory()->with_morsels()) {
            auto source_id = pipeline->get_op_factories()[0]->plan_node_id();
            DCHECK(morsel_queue_factories.count(source_id));
            auto& morsel_queue_factory = morsel_queue_factories[source_id];
            // DOP of OlapScanPrepareOperator is 1 (shared scan)
            // or equal to DOP of OlapScanOperator (morsel_queue_factory->size()).
            DCHECK(cur_pipeline_dop == 1 || cur_pipeline_dop == morsel_queue_factory->size());

            pipeline->source_operator_factory()->set_morsel_queue_factory(morsel_queue_factory.get());
            for (size_t i = 0; i < cur_pipeline_dop; ++i) {
                auto&& operators = pipeline->create_operators(cur_pipeline_dop, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), _query_ctx,
                                                                    _fragment_ctx.get(), driver_id++);
                driver->set_morsel_queue(morsel_queue_factory->create(i));
                if (auto* scan_operator = driver->source_scan_operator()) {
                    scan_operator->set_workgroup(_wg);
                    scan_operator->set_query_ctx(_query_ctx->get_shared_ptr());
                    if (dynamic_cast<ConnectorScanOperator*>(scan_operator) != nullptr) {
                        if (_wg != nullptr) {
                            scan_operator->set_scan_executor(exec_env->connector_scan_executor_with_workgroup());
                        } else {
                            scan_operator->set_scan_executor(exec_env->connector_scan_executor_without_workgroup());
                        }
                    } else {
                        if (_wg != nullptr) {
                            scan_operator->set_scan_executor(exec_env->scan_executor_with_workgroup());
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
    _query_ctx->query_trace()->register_drivers(fragment_instance_id, _fragment_ctx->drivers());

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

Status FragmentExecutor::_prepare_global_dict(const UnifiedExecPlanFragmentParams& request) {
    const auto& fragment = request.common().fragment;
    // Set up global dict
    auto* runtime_state = _fragment_ctx->runtime_state();
    if (fragment.__isset.query_global_dicts) {
        RETURN_IF_ERROR(runtime_state->init_query_global_dict(fragment.query_global_dicts));
    }

    if (fragment.__isset.load_global_dicts) {
        RETURN_IF_ERROR(runtime_state->init_load_global_dict(fragment.load_global_dicts));
    }
    return Status::OK();
}

Status FragmentExecutor::prepare(ExecEnv* exec_env, const TExecPlanFragmentParams& common_request,
                                 const TExecPlanFragmentParams& unique_request) {
    DCHECK(common_request.__isset.desc_tbl);
    DCHECK(common_request.__isset.fragment);

    UnifiedExecPlanFragmentParams request(common_request, unique_request);

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
            auto fragment_ctx = _query_ctx->fragment_mgr()->get(request.fragment_instance_id());
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
            _fail_cleanup(prepare_success);
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
        RETURN_IF_ERROR(_prepare_stream_load_pipe(exec_env, request));
    }

    RETURN_IF_ERROR(_query_ctx->fragment_mgr()->register_ctx(request.fragment_instance_id(), _fragment_ctx));
    prepare_success = true;

    return Status::OK();
}

Status FragmentExecutor::execute(ExecEnv* exec_env) {
    bool prepare_success = false;
    DeferOp defer([this, &prepare_success]() {
        if (!prepare_success) {
            _fail_cleanup(true);
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

void FragmentExecutor::_fail_cleanup(bool fragment_has_registed) {
    if (_query_ctx) {
        if (_fragment_ctx) {
            if (fragment_has_registed) {
                _query_ctx->fragment_mgr()->unregister(_fragment_ctx->fragment_instance_id());
            }
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
        is_pipeline_level_shuffle = true;
        DCHECK_GT(dest_dop, 0);
    }

    std::shared_ptr<SinkBuffer> sink_buffer =
            std::make_shared<SinkBuffer>(fragment_ctx, sender->destinations(), is_dest_merge, dop);

    auto exchange_sink = std::make_shared<ExchangeSinkOperatorFactory>(
            context->next_operator_id(), stream_sink.dest_node_id, sink_buffer, sender->get_partition_type(),
            sender->destinations(), is_pipeline_level_shuffle, dest_dop, sender->sender_id(),
            sender->get_dest_node_id(), sender->get_partition_exprs(),
            !is_dest_merge && sender->get_enable_exchange_pass_through(),
            sender->get_enable_exchange_perf() && !context->has_aggregation, fragment_ctx, sender->output_columns());
    return exchange_sink;
}

DIAGNOSTIC_PUSH
#if defined(__clang__)
DIAGNOSTIC_IGNORE("-Wpotentially-evaluated-expression")
#endif
Status FragmentExecutor::_decompose_data_sink_to_operator(RuntimeState* runtime_state, PipelineBuilderContext* context,
                                                          const UnifiedExecPlanFragmentParams& request,
                                                          std::unique_ptr<starrocks::DataSink>& datasink,
                                                          const TDataSink& thrift_sink,
                                                          const std::vector<TExpr>& output_exprs) {
    auto fragment_ctx = context->fragment_context();
    if (typeid(*datasink) == typeid(starrocks::ResultSink)) {
        auto* result_sink = down_cast<starrocks::ResultSink*>(datasink.get());
        // Result sink doesn't have plan node id;
        OpFactoryPtr op = nullptr;
        if (result_sink->get_sink_type() == TResultSinkType::FILE) {
            auto dop = fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();
            op = std::make_shared<FileSinkOperatorFactory>(context->next_operator_id(), result_sink->get_output_exprs(),
                                                           result_sink->get_file_opts(), dop, fragment_ctx);
        } else {
            op = std::make_shared<ResultSinkOperatorFactory>(context->next_operator_id(), result_sink->get_sink_type(),
                                                             result_sink->get_output_exprs(), fragment_ctx);
        }
        // Add result sink operator to last pipeline
        fragment_ctx->pipelines().back()->add_op_factory(op);
    } else if (typeid(*datasink) == typeid(starrocks::DataStreamSender)) {
        auto* sender = down_cast<starrocks::DataStreamSender*>(datasink.get());
        auto dop = fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();
        auto& t_stream_sink = request.output_sink().stream_sink;

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
        auto* mcast_sink = down_cast<starrocks::MultiCastDataStreamSink*>(datasink.get());
        const auto& sinks = mcast_sink->get_sinks();
        auto& t_multi_case_stream_sink = request.output_sink().multi_cast_stream_sink;

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
        size_t desired_tablet_sink_dop = request.pipeline_sink_dop();
        DCHECK(desired_tablet_sink_dop > 0);
        size_t source_operator_dop =
                fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();

        runtime_state->set_num_per_fragment_instances(request.common().params.num_senders);
        std::vector<std::unique_ptr<starrocks::stream_load::OlapTableSink>> tablet_sinks;
        for (int i = 1; i < desired_tablet_sink_dop; i++) {
            Status st;
            std::unique_ptr<starrocks::stream_load::OlapTableSink> sink =
                    std::make_unique<starrocks::stream_load::OlapTableSink>(runtime_state->obj_pool(), output_exprs,
                                                                            &st);
            RETURN_IF_ERROR(st);
            if (sink != nullptr) {
                RETURN_IF_ERROR(sink->init(thrift_sink));
            }
            tablet_sinks.emplace_back(std::move(sink));
        }
        OpFactoryPtr tablet_sink_op = std::make_shared<OlapTableSinkOperatorFactory>(
                context->next_operator_id(), datasink, fragment_ctx, request.sender_id(), desired_tablet_sink_dop,
                tablet_sinks);
        // FE will pre-set the parallelism for all fragment instance which contains the tablet sink,
        // For stream load, routine load or broker load, the desired_tablet_sink_dop set
        // by FE is same as the source_operator_dop.
        // For insert into select, in the simplest case like insert into table select * from table2;
        // the desired_tablet_sink_dop set by FE is same as the source_operator_dop.
        // However, if the select statement is complex, like insert into table select * from table2 limit 1,
        // the desired_tablet_sink_dop set by FE is not same as the source_operator_dop, and it needs to
        // add a local passthrough exchange here
        if (desired_tablet_sink_dop != source_operator_dop) {
            std::vector<OpFactories> pred_operators_list;
            pred_operators_list.push_back(fragment_ctx->pipelines().back()->get_op_factories());

            size_t max_row_count = 0;
            auto* source_operator =
                    down_cast<SourceOperatorFactory*>(fragment_ctx->pipelines().back()->get_op_factories()[0].get());
            max_row_count += source_operator->degree_of_parallelism() * runtime_state->chunk_size();

            auto pseudo_plan_node_id = context->next_pseudo_plan_node_id();
            auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(
                    max_row_count * PipelineBuilderContext::localExchangeBufferChunks());
            auto local_exchange_source = std::make_shared<LocalExchangeSourceOperatorFactory>(
                    context->next_operator_id(), pseudo_plan_node_id, mem_mgr);
            local_exchange_source->set_runtime_state(runtime_state);
            auto exchanger = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());

            auto local_exchange_sink = std::make_shared<LocalExchangeSinkOperatorFactory>(
                    context->next_operator_id(), pseudo_plan_node_id, exchanger);
            fragment_ctx->pipelines().back()->add_op_factory(local_exchange_sink);

            OpFactories operators_source_with_local_exchange;
            local_exchange_source->set_degree_of_parallelism(desired_tablet_sink_dop);
            operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));
            operators_source_with_local_exchange.emplace_back(std::move(tablet_sink_op));

            auto pipeline_with_local_exchange_source =
                    std::make_shared<Pipeline>(context->next_pipe_id(), operators_source_with_local_exchange);
            fragment_ctx->pipelines().emplace_back(std::move(pipeline_with_local_exchange_source));
        } else {
            fragment_ctx->pipelines().back()->add_op_factory(tablet_sink_op);
        }
    } else if (typeid(*datasink) == typeid(starrocks::ExportSink)) {
        auto* export_sink = down_cast<starrocks::ExportSink*>(datasink.get());
        auto dop = fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();
        auto output_expr = export_sink->get_output_expr();
        OpFactoryPtr op = std::make_shared<ExportSinkOperatorFactory>(
                context->next_operator_id(), request.output_sink().export_sink, export_sink->get_output_expr(), dop,
                fragment_ctx);
        fragment_ctx->pipelines().back()->add_op_factory(op);
    } else if (typeid(*datasink) == typeid(starrocks::MysqlTableSink)) {
        auto* mysql_table_sink = down_cast<starrocks::MysqlTableSink*>(datasink.get());
        auto dop = fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();
        auto output_expr = mysql_table_sink->get_output_expr();
        OpFactoryPtr op = std::make_shared<MysqlTableSinkOperatorFactory>(
                context->next_operator_id(), request.output_sink().mysql_table_sink,
                mysql_table_sink->get_output_expr(), dop, fragment_ctx);
        fragment_ctx->pipelines().back()->add_op_factory(op);
    } else if (typeid(*datasink) == typeid(starrocks::MemoryScratchSink)) {
        auto* memory_scratch_sink = down_cast<starrocks::MemoryScratchSink*>(datasink.get());
        auto output_expr = memory_scratch_sink->get_output_expr();
        auto row_desc = memory_scratch_sink->get_row_desc();
        auto dop = fragment_ctx->pipelines().back()->source_operator_factory()->degree_of_parallelism();
        DCHECK_EQ(dop, 1);
        OpFactoryPtr op = std::make_shared<MemoryScratchSinkOperatorFactory>(context->next_operator_id(), row_desc,
                                                                             output_expr, fragment_ctx);
        fragment_ctx->pipelines().back()->add_op_factory(op);
    }

    return Status::OK();
}
DIAGNOSTIC_POP

} // namespace starrocks::pipeline
