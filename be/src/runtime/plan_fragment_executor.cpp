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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/plan_fragment_executor.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/plan_fragment_executor.h"

#include <memory>
#include <utility>

#include "common/logging.h"
#include "common/object_pool.h"
#include "exec/data_sink.h"
#include "exec/exchange_node.h"
#include "exec/exec_node.h"
#include "exec/scan_node.h"
#include "gutil/map_util.h"
#include "runtime/current_thread.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/profile_report_worker.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "util/parse_util.h"
#include "util/uid_util.h"

namespace starrocks {

PlanFragmentExecutor::PlanFragmentExecutor(ExecEnv* exec_env, report_status_callback report_status_cb)
        : _exec_env(exec_env),
          _report_status_cb(std::move(report_status_cb)),
          _done(false),
          _prepared(false),
          _closed(false),
          enable_profile(true),
          _is_report_on_cancel(true),
          _collect_query_statistics_with_every_batch(false),
          _is_runtime_filter_merge_node(false) {}

PlanFragmentExecutor::~PlanFragmentExecutor() {
    close();
}

Status PlanFragmentExecutor::prepare(const TExecPlanFragmentParams& request) {
    const TPlanFragmentExecParams& params = request.params;
    _query_id = params.query_id;

    LOG(INFO) << "Prepare(): query_id=" << print_id(_query_id)
              << " fragment_instance_id=" << print_id(params.fragment_instance_id)
              << " backend_num=" << request.backend_num;

    DCHECK(_runtime_state->chunk_size() > 0);

    _runtime_state->set_be_number(request.backend_num);
    if (request.query_options.__isset.enable_profile) {
        enable_profile = request.query_options.enable_profile;
    }
    if (request.query_options.__isset.load_profile_collect_second) {
        load_profile_collect_second = request.query_options.load_profile_collect_second;
    }

    // set up desc tbl
    DescriptorTbl* desc_tbl = nullptr;
    DCHECK(request.__isset.desc_tbl);
    RETURN_IF_ERROR(DescriptorTbl::create(_runtime_state, obj_pool(), request.desc_tbl, &desc_tbl,
                                          _runtime_state->chunk_size()));
    _runtime_state->set_desc_tbl(desc_tbl);

    LOG(INFO) << "Prepare(): set up desc tbl " << desc_tbl.debug_string();

    // set up plan
    DCHECK(request.__isset.fragment);
    RETURN_IF_ERROR(ExecNode::create_tree(_runtime_state, obj_pool(), request.fragment.plan, *desc_tbl, &_plan));
    _runtime_state->set_fragment_root_id(_plan->id());

    if (request.fragment.__isset.query_global_dicts) {
        RETURN_IF_ERROR(_runtime_state->init_query_global_dict(request.fragment.query_global_dicts));
    }

    if (request.fragment.__isset.load_global_dicts) {
        RETURN_IF_ERROR(_runtime_state->init_load_global_dict(request.fragment.load_global_dicts));
    }

    if (params.__isset.runtime_filter_params && params.runtime_filter_params.id_to_prober_params.size() != 0) {
        _is_runtime_filter_merge_node = true;
        _exec_env->runtime_filter_worker()->open_query(_query_id, request.query_options, params.runtime_filter_params,
                                                       false);
    }
    _exec_env->stream_mgr()->prepare_pass_through_chunk_buffer(_query_id);

    // set #senders of exchange nodes before calling Prepare()
    std::vector<ExecNode*> exch_nodes;
    _plan->collect_nodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
    for (auto* exch_node : exch_nodes) {
        DCHECK_EQ(exch_node->type(), TPlanNodeType::EXCHANGE_NODE);
        int num_senders = FindWithDefault(params.per_exch_num_senders, exch_node->id(), 0);
        DCHECK_GT(num_senders, 0);
        static_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
    }

    RETURN_IF_ERROR(_plan->prepare(_runtime_state));
    // set scan ranges
    std::vector<ExecNode*> scan_nodes;
    std::vector<TScanRangeParams> no_scan_ranges;
    _plan->collect_scan_nodes(&scan_nodes);

    for (auto& i : scan_nodes) {
        auto* scan_node = down_cast<ScanNode*>(i);
        const std::vector<TScanRangeParams>& scan_ranges =
                FindWithDefault(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
        scan_node->set_scan_ranges(scan_ranges);
        VLOG(1) << "scan_node_Id=" << scan_node->id() << " size=" << scan_ranges.size();
    }

    _runtime_state->set_per_fragment_instance_idx(params.sender_id);
    _runtime_state->set_num_per_fragment_instances(params.num_senders);
    _query_statistics.reset(new QueryStatistics());

    // set up sink, if required
    if (request.fragment.__isset.output_sink) {
        RETURN_IF_ERROR(DataSink::create_data_sink(_runtime_state, request.fragment.output_sink,
                                                   request.fragment.output_exprs, params, params.sender_id, row_desc(),
                                                   &_sink));
        DCHECK(_sink != nullptr);
        RETURN_IF_ERROR(_sink->prepare(runtime_state()));
        _sink->set_query_statistics(_query_statistics);

        RuntimeProfile* sink_profile = _sink->profile();

        if (sink_profile != nullptr) {
            profile()->add_child(sink_profile, true, nullptr);
        }

        _collect_query_statistics_with_every_batch = params.__isset.send_query_statistics_with_every_batch
                                                             ? params.send_query_statistics_with_every_batch
                                                             : false;
    } else {
        // _sink is set to NULL
        _sink.reset(nullptr);
    }

    // set up profile counters
    profile()->add_child(_plan->runtime_profile(), true, nullptr);
    _rows_produced_counter = ADD_COUNTER(profile(), "RowsProduced", TUnit::UNIT);

    VLOG(3) << "plan_root=\n" << _plan->debug_string();
    _chunk = std::make_shared<Chunk>();
    _prepared = true;
    RETURN_IF_ERROR(_prepare_stream_load_pipe(request));

    return Status::OK();
}

Status PlanFragmentExecutor::open() {
    LOG(INFO) << "Open(): fragment_instance_id=" << print_id(_runtime_state->fragment_instance_id());
    tls_thread_status.set_query_id(_runtime_state->query_id());

    // Only register profile report worker for broker load and insert into here,
    // for stream load and routine load, currently we don't need BE to report their progress regularly.
    const TQueryOptions& query_options = _runtime_state->query_options();
    if (query_options.query_type == TQueryType::LOAD && (query_options.load_job_type == TLoadJobType::BROKER ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_QUERY ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_VALUES)) {
        RETURN_IF_ERROR(starrocks::ExecEnv::GetInstance()->profile_report_worker()->register_non_pipeline_load(
                _runtime_state->fragment_instance_id()));
    }

    Status status = _open_internal_vectorized();
    if (!status.ok() && !status.is_cancelled() && _runtime_state->log_has_space()) {
        LOG(WARNING) << "Fail to open fragment, instance_id=" << print_id(_runtime_state->fragment_instance_id())
                     << ", status=" << status;
        // Log error message in addition to returning in Status. Queries that do not
        // fetch results (e.g. insert) may not receive the message directly and can
        // only retrieve the log.
        _runtime_state->log_error(status.get_error_msg());
    }

    update_status(status);
    return status;
}

Status PlanFragmentExecutor::_open_internal_vectorized() {
    {
        SCOPED_TIMER(profile()->total_time_counter());
        RETURN_IF_ERROR(_plan->open(_runtime_state));
    }

    if (_sink == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_sink->open(runtime_state()));

    // If there is a sink, do all the work of driving it here, so that
    // when this returns the query has actually finished
    ChunkPtr chunk;
    while (true) {
        RETURN_IF_ERROR(runtime_state()->check_mem_limit("QUERY"));
        RETURN_IF_ERROR(_get_next_internal_vectorized(&chunk));

        if (chunk == nullptr) {
            break;
        }

        if (VLOG_ROW_IS_ON) {
            VLOG_ROW << "_open_internal_vectorized: #rows=" << chunk->num_rows()
                     << " desc=" << row_desc().debug_string() << " columns=" << chunk->debug_columns();
            // TODO(kks): support chunk debug log
        }

        SCOPED_TIMER(profile()->total_time_counter());
        // Collect this plan and sub plan statistics, and send to parent plan.
        if (_collect_query_statistics_with_every_batch) {
            collect_query_statistics();
        }
        RETURN_IF_ERROR(_sink->send_chunk(runtime_state(), chunk.get()));
    }

    // Close the sink *before* stopping the report thread. Close may
    // need to add some important information to the last report that
    // gets sent. (e.g. table sinks record the files they have written
    // to in this method)
    // The coordinator report channel waits until all backends are
    // either in error or have returned a status report with done =
    // true, so tearing down any data stream state (a separate
    // channel) in Close is safe.

    // TODO: If this returns an error, the d'tor will call Close again. We should
    // audit the sinks to check that this is ok, or change that behaviour.
    Status close_status;
    {
        SCOPED_TIMER(profile()->total_time_counter());
        collect_query_statistics();
        Status status;
        {
            std::lock_guard<std::mutex> l(_status_lock);
            status = _status;
        }
        close_status = _sink->close(runtime_state(), status);
    }

    update_status(close_status);
    // Setting to NULL ensures that the d'tor won't double-close the sink.
    _sink.reset(nullptr);
    _done = true;

    send_report(true);

    return close_status;
}

void PlanFragmentExecutor::collect_query_statistics() {
    _query_statistics->clear();
    _plan->collect_query_statistics(_query_statistics.get());
}

void PlanFragmentExecutor::send_report(bool done) {
    if (!_report_status_cb) {
        return;
    }

    Status status;
    {
        std::lock_guard<std::mutex> l(_status_lock);
        status = _status;
    }

    // If plan is done successfully, but enable_profile is false,
    // no need to send report.
    if (!enable_profile && done && status.ok()) {
        return;
    }

    // If both enable_profile and _is_report_on_cancel are false,
    // which means no matter query is success or failed, no report is needed.
    // This may happen when the query limit reached and
    // a internal cancellation being processed
    if (!enable_profile && !_is_report_on_cancel) {
        return;
    }

    auto start_timestamp = _runtime_state->timestamp_ms() / 1000;
    auto now = std::time(nullptr);
    if (load_profile_collect_second != -1 && now - start_timestamp < load_profile_collect_second) {
        return;
    }

    // This will send a report even if we are cancelled.  If the query completed correctly
    // but fragments still need to be cancelled (e.g. limit reached), the coordinator will
    // be waiting for a final report and profile.
    _report_status_cb(status, profile(), done || !status.ok());
}

Status PlanFragmentExecutor::_get_next_internal_vectorized(ChunkPtr* chunk) {
    // If there is a empty chunk, we continue to read next chunk
    // If we set chunk to nullptr, means this fragment read done
    while (!_done) {
        SCOPED_TIMER(profile()->total_time_counter());
        RETURN_IF_ERROR(_plan->get_next(_runtime_state, &_chunk, &_done));
        if (_done) {
            *chunk = nullptr;
            return Status::OK();
        } else if (_chunk->num_rows() > 0) {
            COUNTER_UPDATE(_rows_produced_counter, _chunk->num_rows());
            *chunk = _chunk;
            return Status::OK();
        }
    }

    return Status::OK();
}

void PlanFragmentExecutor::update_status(const Status& new_status) {
    if (new_status.ok()) {
        return;
    }

    {
        std::lock_guard<std::mutex> l(_status_lock);
        // if current `_status` is ok, set it to `new_status` to record the error.
        if (_status.ok()) {
            if (new_status.is_mem_limit_exceeded()) {
                (void)_runtime_state->set_mem_limit_exceeded(new_status.get_error_msg());
            }
            _status = new_status;
            if (_runtime_state->query_options().query_type == TQueryType::EXTERNAL) {
                TUniqueId fragment_instance_id = _runtime_state->fragment_instance_id();
                _exec_env->result_queue_mgr()->update_queue_status(fragment_instance_id, new_status);
            }
        }
    }

    send_report(true);
}

void PlanFragmentExecutor::cancel() {
    LOG(INFO) << "cancel(): fragment_instance_id=" << print_id(_runtime_state->fragment_instance_id());
    DCHECK(_prepared);
    {
        std::lock_guard<std::mutex> l(_status_lock);
        if (_runtime_state->is_cancelled()) {
            return;
        }
        _runtime_state->set_is_cancelled(true);
    }

    const TQueryOptions& query_options = _runtime_state->query_options();
    if (query_options.query_type == TQueryType::LOAD && (query_options.load_job_type == TLoadJobType::BROKER ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_QUERY ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_VALUES)) {
        starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_non_pipeline_load(
                _runtime_state->fragment_instance_id());
    }
    if (_stream_load_contexts.size() > 0) {
        for (const auto& stream_load_context : _stream_load_contexts) {
            if (stream_load_context->body_sink) {
                Status st;
                stream_load_context->body_sink->cancel(st);
            }
            if (_channel_stream_load) {
                _exec_env->stream_context_mgr()->remove_channel_context(stream_load_context);
            }
        }
        _stream_load_contexts.resize(0);
    }
    _runtime_state->exec_env()->stream_mgr()->cancel(_runtime_state->fragment_instance_id());
    auto st = _runtime_state->exec_env()->result_mgr()->cancel(_runtime_state->fragment_instance_id());
    st.permit_unchecked_error();

    if (_is_runtime_filter_merge_node) {
        _runtime_state->exec_env()->runtime_filter_worker()->close_query(_query_id);
    }
}

const RowDescriptor& PlanFragmentExecutor::row_desc() {
    return _plan->row_desc();
}

RuntimeProfile* PlanFragmentExecutor::profile() {
    return _runtime_state->runtime_profile();
}

void PlanFragmentExecutor::report_profile_once() {
    if (_done) return;

    if (VLOG_FILE_IS_ON) {
        VLOG_FILE << "Reporting profile for instance " << _runtime_state->fragment_instance_id();
        std::stringstream ss;
        profile()->compute_time_in_profile();
        profile()->pretty_print(&ss);
        VLOG_FILE << ss.str();
    }

    send_report(false);
}

void PlanFragmentExecutor::close() {
    if (_closed) {
        return;
    }

    _chunk.reset();

    if (_is_runtime_filter_merge_node) {
        _exec_env->runtime_filter_worker()->close_query(_query_id);
    }
    _exec_env->stream_mgr()->destroy_pass_through_chunk_buffer(_query_id);

    // Prepare may not have been called, which sets _runtime_state
    if (_runtime_state != nullptr) {
        const TQueryOptions& query_options = _runtime_state->query_options();
        if (query_options.query_type == TQueryType::LOAD &&
            (query_options.load_job_type == TLoadJobType::BROKER ||
             query_options.load_job_type == TLoadJobType::INSERT_QUERY ||
             query_options.load_job_type == TLoadJobType::INSERT_VALUES) &&
            !_runtime_state->is_cancelled()) {
            starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_non_pipeline_load(
                    _runtime_state->fragment_instance_id());
        }

        if (_stream_load_contexts.size() > 0) {
            for (const auto& stream_load_context : _stream_load_contexts) {
                if (stream_load_context->body_sink) {
                    Status st;
                    stream_load_context->body_sink->cancel(st);
                }
                if (_channel_stream_load) {
                    _exec_env->stream_context_mgr()->remove_channel_context(stream_load_context);
                }
            }
            _stream_load_contexts.resize(0);
        }
        // _runtime_state init failed
        if (_plan != nullptr) {
            _plan->close(_runtime_state);
        }

        if (_sink != nullptr) {
            if (_prepared) {
                Status status;
                {
                    std::lock_guard<std::mutex> l(_status_lock);
                    status = _status;
                }
                _sink->close(runtime_state(), status);
            } else {
                _sink->close(runtime_state(), Status::InternalError("prepare failed"));
            }
        }

        if (FLAGS_minloglevel == 0 /*INFO*/) {
            std::stringstream ss;
            // Compute the _local_time_percent before pretty_print the runtime_profile
            // Before add this operation, the print out like that:
            // UNION_NODE (id=0):(Active: 56.720us, non-child: 00.00%)
            // After add the operation, the print out like that:
            // UNION_NODE (id=0):(Active: 56.720us, non-child: 82.53%)
            // We can easily know the exec node execute time without child time consumed.
            _runtime_state->runtime_profile()->compute_time_in_profile();
            _runtime_state->runtime_profile()->pretty_print(&ss);
            LOG(INFO) << ss.str();
        }
    }

    _closed = true;
}

Status PlanFragmentExecutor::_prepare_stream_load_pipe(const TExecPlanFragmentParams& request) {
    const auto& scan_range_map = request.params.per_node_scan_ranges;
    if (scan_range_map.size() == 0) {
        return Status::OK();
    }
    auto iter = scan_range_map.begin();
    if (iter->second.size() == 0) {
        return Status::OK();
    }
    if (!iter->second[0].scan_range.__isset.broker_scan_range) {
        return Status::OK();
    }
    if (!iter->second[0].scan_range.broker_scan_range.__isset.channel_id) {
        return Status::OK();
    }
    _channel_stream_load = true;
    for (; iter != scan_range_map.end(); iter++) {
        for (const auto& scan_range : iter->second) {
            const TBrokerScanRange& broker_scan_range = scan_range.scan_range.broker_scan_range;
            int channel_id = broker_scan_range.channel_id;
            const string& label = broker_scan_range.params.label;
            const string& db_name = broker_scan_range.params.db_name;
            const string& table_name = broker_scan_range.params.table_name;
            TFileFormatType::type format = broker_scan_range.ranges[0].format_type;
            TUniqueId load_id = broker_scan_range.ranges[0].load_id;
            long txn_id = broker_scan_range.params.txn_id;
            StreamLoadContext* ctx = nullptr;
            RETURN_IF_ERROR(_exec_env->stream_context_mgr()->create_channel_context(
                    _exec_env, label, channel_id, db_name, table_name, format, ctx, load_id, txn_id));
            DeferOp op([&] {
                if (ctx->unref()) {
                    delete ctx;
                }
            });
            RETURN_IF_ERROR(_exec_env->stream_context_mgr()->put_channel_context(label, channel_id, ctx));
            _stream_load_contexts.push_back(ctx);
        }
    }
    return Status::OK();
}

} // namespace starrocks
