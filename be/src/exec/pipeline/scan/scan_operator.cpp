// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/chunk_buffer_limiter.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "exec/vectorized/olap_scan_node.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

// ========== ScanOperator ==========

ScanOperator::ScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                           ScanNode* scan_node, ChunkBufferLimiter* buffer_limiter)
        : SourceOperator(factory, id, scan_node->name(), scan_node->id(), driver_sequence),
          _scan_node(scan_node),
          _chunk_source_profiles(MAX_IO_TASKS_PER_OP),
          _buffer_limiter(buffer_limiter),
          _dop(dop),
          _is_io_task_running(MAX_IO_TASKS_PER_OP),
          _chunk_sources(MAX_IO_TASKS_PER_OP) {
    for (auto i = 0; i < MAX_IO_TASKS_PER_OP; i++) {
        _chunk_source_profiles[i] = std::make_shared<RuntimeProfile>(strings::Substitute("ChunkSource$0", i));
    }
}

ScanOperator::~ScanOperator() {
    auto* state = runtime_state();
    if (state == nullptr) {
        return;
    }

    for (size_t i = 0; i < _chunk_sources.size(); i++) {
        if (_chunk_sources[i] != nullptr) {
            _chunk_sources[i]->close(state);
            _chunk_sources[i] = nullptr;
        }
    }
}

Status ScanOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));

    _unique_metrics->add_info_string("MorselQueueType", _morsel_queue->name());
    _peak_buffer_size_counter = _unique_metrics->AddHighWaterMarkCounter("PeakChunkBufferSize", TUnit::UNIT);
    _morsels_counter = ADD_COUNTER(_unique_metrics, "MorselsCount", TUnit::UNIT);
    _submit_task_counter = ADD_COUNTER(_unique_metrics, "SubmitIOTaskCount", TUnit::UNIT);

    if (_workgroup == nullptr) {
        DCHECK(_io_threads != nullptr);
        auto num_scan_operators = 1 + state->exec_env()->increment_num_scan_operators(1);
        if (num_scan_operators > _io_threads->get_queue_capacity()) {
            state->exec_env()->decrement_num_scan_operators(1);
            return Status::TooManyTasks(
                    strings::Substitute("num_scan_operators exceeds queue capacity($0) of pipeline_pool_thread",
                                        _io_threads->get_queue_capacity()));
        }
    }

    RETURN_IF_ERROR(do_prepare(state));

    return Status::OK();
}

void ScanOperator::close(RuntimeState* state) {
    if (_workgroup == nullptr) {
        state->exec_env()->decrement_num_scan_operators(1);
    }
    // For the running io task, we close its chunk sources in ~ScanOperator not in ScanOperator::close.
    for (size_t i = 0; i < _chunk_sources.size(); i++) {
        if (_chunk_sources[i] != nullptr) {
            _chunk_sources[i]->set_finished(state);
            if (!_is_io_task_running[i]) {
                _chunk_sources[i]->close(state);
                _chunk_sources[i] = nullptr;
            }
        }
    }

    _default_buffer_capacity_counter = ADD_COUNTER(_unique_metrics, "DefaultChunkBufferCapacity", TUnit::UNIT);
    COUNTER_SET(_default_buffer_capacity_counter, static_cast<int64_t>(_buffer_limiter->default_capacity()));
    _buffer_capacity_counter = ADD_COUNTER(_unique_metrics, "ChunkBufferCapacity", TUnit::UNIT);
    COUNTER_SET(_buffer_capacity_counter, static_cast<int64_t>(_buffer_limiter->capacity()));

    _tablets_counter = ADD_COUNTER(_unique_metrics, "TabletCount", TUnit::UNIT);
    COUNTER_SET(_tablets_counter, static_cast<int64_t>(_morsel_queue->num_original_morsels()));

    _merge_chunk_source_profiles();

    do_close(state);
    Operator::close(state);
}

size_t ScanOperator::_buffer_unplug_threshold() const {
    size_t threshold = _buffer_limiter->capacity() / _dop / 2;
    threshold = std::max<size_t>(1, std::min<size_t>(_buffer_size, threshold));
    return threshold;
}

size_t ScanOperator::_num_buffered_chunks() const {
    size_t buffered_chunks = 0;
    for (const auto& chunk_source : _chunk_sources) {
        if (chunk_source != nullptr) {
            buffered_chunks += chunk_source->get_buffer_size();
        }
    }
    return buffered_chunks;
}

bool ScanOperator::has_output() const {
    if (_is_finished) {
        return false;
    }
    // if storage layer returns an error, we should make sure `pull_chunk` has a chance to get it
    if (!_get_scan_status().ok()) {
        return true;
    }

    size_t buffered_chunks = _num_buffered_chunks();
    if (_unpluging) {
        if (buffered_chunks > 0) {
            return true;
        }
        _unpluging = false;
    }
    if (buffered_chunks >= _buffer_unplug_threshold()) {
        _unpluging = true;
        return true;
    }
    if (_buffer_limiter->is_full() && buffered_chunks > 0) {
        return true;
    }

    if (_num_running_io_tasks >= MAX_IO_TASKS_PER_OP || _buffer_limiter->is_full()) {
        return false;
    }

    // Because committing i/o task is trigger ONLY in pull_chunk,
    // return true if more i/o tasks can be committed.

    // Can pick up more morsels.
    if (!_morsel_queue->empty()) {
        return true;
    }

    // Can trigger_next_scan for the picked-up morsel.
    for (int i = 0; i < MAX_IO_TASKS_PER_OP; ++i) {
        if (_chunk_sources[i] != nullptr && !_is_io_task_running[i] && _chunk_sources[i]->has_next_chunk()) {
            return true;
        }
    }

    return buffered_chunks > 0;
}

bool ScanOperator::pending_finish() const {
    DCHECK(is_finished());
    return false;
}

bool ScanOperator::is_finished() const {
    if (_is_finished) {
        return true;
    }
    // if storage layer returns an error, we should make sure `pull_chunk` has a chance to get it
    if (!_get_scan_status().ok()) {
        return false;
    }

    // Any io task is running or needs to run.
    if (_num_running_io_tasks > 0 || !_morsel_queue->empty()) {
        return false;
    }

    for (const auto& chunk_source : _chunk_sources) {
        if (chunk_source != nullptr && (chunk_source->has_output() || chunk_source->has_next_chunk())) {
            return false;
        }
    }

    // This scan operator is finished, if no more io tasks are running
    // or need to run, and all the read chunks are consumed.
    return true;
}

Status ScanOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> ScanOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_get_scan_status());

    _peak_buffer_size_counter->set(_buffer_limiter->size());

    RETURN_IF_ERROR(_try_to_trigger_next_scan(state));
    if (_workgroup != nullptr) {
        _workgroup->incr_period_ask_chunk_num(1);
    }

    for (auto& chunk_source : _chunk_sources) {
        if (chunk_source != nullptr && chunk_source->has_output()) {
            auto&& chunk = chunk_source->get_next_chunk_from_buffer();
            eval_runtime_bloom_filters(chunk.value().get());

            return std::move(chunk);
        }
    }

    return nullptr;
}

int64_t ScanOperator::global_rf_wait_timeout_ns() const {
    const auto* global_rf_collector = runtime_bloom_filters();
    if (global_rf_collector == nullptr) {
        return 0;
    }

    return 1000'000L * global_rf_collector->scan_wait_timeout_ms();
}

Status ScanOperator::_try_to_trigger_next_scan(RuntimeState* state) {
    if (_num_running_io_tasks >= MAX_IO_TASKS_PER_OP) {
        return Status::OK();
    }
    if (_unpluging && _num_buffered_chunks() >= _buffer_unplug_threshold()) {
        return Status::OK();
    }

    for (int i = 0; i < MAX_IO_TASKS_PER_OP; ++i) {
        if (_is_io_task_running[i]) {
            continue;
        }

        if (_chunk_sources[i] == nullptr) {
            RETURN_IF_ERROR(_pickup_morsel(state, i));
        } else if (_chunk_sources[i]->has_next_chunk()) {
            RETURN_IF_ERROR(_trigger_next_scan(state, i));
        } else if (!_chunk_sources[i]->has_output()) {
            RETURN_IF_ERROR(_pickup_morsel(state, i));
        }
    }

    return Status::OK();
}

// this is a more efficient way to check if a weak_ptr has been initialized
// ref: https://stackoverflow.com/a/45507610
// after compiler optimization, it generates far fewer instructions than std::weak_ptr::expired() and std::weak_ptr::lock()
// see: https://godbolt.org/z/16bWqqM5n
inline bool is_uninitialized(const std::weak_ptr<QueryContext>& ptr) {
    using wp = std::weak_ptr<QueryContext>;
    return !ptr.owner_before(wp{}) && !wp{}.owner_before(ptr);
}

void ScanOperator::_finish_chunk_source_task(RuntimeState* state, int chunk_source_index, int64_t cpu_time_ns,
                                             int64_t scan_rows, int64_t scan_bytes) {
    _last_growth_cpu_time_ns += cpu_time_ns;
    _last_scan_rows_num += scan_rows;
    _last_scan_bytes += scan_bytes;
    _num_running_io_tasks--;
    _is_io_task_running[chunk_source_index] = false;
}

Status ScanOperator::_trigger_next_scan(RuntimeState* state, int chunk_source_index) {
    ChunkBufferTokenPtr buffer_token;
    if (buffer_token = _buffer_limiter->pin(1); buffer_token == nullptr) {
        return Status::OK();
    }

    COUNTER_UPDATE(_submit_task_counter, 1);
    _chunk_sources[chunk_source_index]->pin_chunk_token(std::move(buffer_token));
    _num_running_io_tasks++;
    _is_io_task_running[chunk_source_index] = true;

    bool offer_task_success = false;
    // to avoid holding mutex in bthread, we choose to initialize lazily here instead of in prepare
    if (is_uninitialized(_query_ctx)) {
        _query_ctx = state->exec_env()->query_context_mgr()->get(state->query_id());
    }
    int32_t driver_id = CurrentThread::current().get_driver_id();
    if (_workgroup != nullptr) {
        workgroup::ScanTask task = workgroup::ScanTask(
                _workgroup, [wp = _query_ctx, this, state, chunk_source_index, driver_id](int worker_id) {
                    if (auto sp = wp.lock()) {
                        // Set driver_id here to share some driver-local contents.
                        // Current it's used by ExprContext's driver-local state
                        CurrentThread::current().set_pipeline_driver_id(driver_id);
                        DeferOp defer([]() { CurrentThread::current().set_pipeline_driver_id(0); });
                        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());

                        auto& chunk_source = _chunk_sources[chunk_source_index];
                        size_t num_read_chunks = 0;
                        int64_t prev_cpu_time = chunk_source->get_cpu_time_spent();
                        int64_t prev_scan_rows = chunk_source->get_scan_rows();
                        int64_t prev_scan_bytes = chunk_source->get_scan_bytes();

                        // Read chunk
                        Status status = chunk_source->buffer_next_batch_chunks_blocking_for_workgroup(
                                _buffer_size, state, &num_read_chunks, worker_id, _workgroup);
                        if (!status.ok() && !status.is_end_of_file()) {
                            _set_scan_status(status);
                        }

                        int64_t delta_cpu_time = chunk_source->get_cpu_time_spent() - prev_cpu_time;
                        _workgroup->increment_real_runtime_ns(delta_cpu_time);
                        _workgroup->incr_period_scaned_chunk_num(num_read_chunks);

                        _finish_chunk_source_task(state, chunk_source_index, delta_cpu_time,
                                                  chunk_source->get_scan_rows() - prev_scan_rows,
                                                  chunk_source->get_scan_bytes() - prev_scan_bytes);
                    }
                });
        if (dynamic_cast<ConnectorScanOperator*>(this) != nullptr) {
            offer_task_success = ExecEnv::GetInstance()->hdfs_scan_executor()->submit(std::move(task));
        } else {
            offer_task_success = ExecEnv::GetInstance()->scan_executor()->submit(std::move(task));
        }
    } else {
        PriorityThreadPool::Task task;
        task.work_function = [wp = _query_ctx, this, state, chunk_source_index, driver_id]() {
            if (auto sp = wp.lock()) {
                // Set driver_id here to share some driver-local contents.
                // Current it's used by ExprContext's driver-local state
                CurrentThread::current().set_pipeline_driver_id(driver_id);
                DeferOp defer([]() { CurrentThread::current().set_pipeline_driver_id(0); });

                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());

                auto& chunk_source = _chunk_sources[chunk_source_index];
                int64_t prev_cpu_time = chunk_source->get_cpu_time_spent();
                int64_t prev_scan_rows = chunk_source->get_scan_rows();
                int64_t prev_scan_bytes = chunk_source->get_scan_bytes();

                Status status =
                        _chunk_sources[chunk_source_index]->buffer_next_batch_chunks_blocking(_buffer_size, state);
                if (!status.ok() && !status.is_end_of_file()) {
                    _set_scan_status(status);
                }
                int64_t delta_cpu_time = chunk_source->get_cpu_time_spent() - prev_cpu_time;

                _finish_chunk_source_task(state, chunk_source_index, delta_cpu_time,
                                          chunk_source->get_scan_rows() - prev_scan_rows,
                                          chunk_source->get_scan_bytes() - prev_scan_bytes);
            }
        };
        // TODO: consider more factors, such as scan bytes and i/o time.
        task.priority = vectorized::OlapScanNode::compute_priority(_submit_task_counter->value());

        offer_task_success = _io_threads->try_offer(task);
    }

    if (offer_task_success) {
        _io_task_retry_cnt = 0;
    } else {
        _chunk_sources[chunk_source_index]->unpin_chunk_token();
        _num_running_io_tasks--;
        _is_io_task_running[chunk_source_index] = false;
        // TODO(hcf) set a proper retry times
        LOG(WARNING) << "ScanOperator failed to offer io task due to thread pool overload, retryCnt="
                     << _io_task_retry_cnt;
        if (++_io_task_retry_cnt > 100) {
            return Status::RuntimeError("ScanOperator failed to offer io task due to thread pool overload");
        }
    }

    return Status::OK();
}

Status ScanOperator::_pickup_morsel(RuntimeState* state, int chunk_source_index) {
    DCHECK(_morsel_queue != nullptr);
    if (_chunk_sources[chunk_source_index] != nullptr) {
        _chunk_sources[chunk_source_index]->close(state);
        _chunk_sources[chunk_source_index] = nullptr;
    }

    ASSIGN_OR_RETURN(auto morsel, _morsel_queue->try_get(_driver_sequence));
    if (morsel != nullptr) {
        COUNTER_UPDATE(_morsels_counter, 1);

        _chunk_sources[chunk_source_index] = create_chunk_source(std::move(morsel), chunk_source_index);
        auto status = _chunk_sources[chunk_source_index]->prepare(state);
        if (!status.ok()) {
            _chunk_sources[chunk_source_index] = nullptr;
            _is_finished = true;
            return status;
        }
        RETURN_IF_ERROR(_trigger_next_scan(state, chunk_source_index));
    }

    return Status::OK();
}

void ScanOperator::_merge_chunk_source_profiles() {
    std::vector<RuntimeProfile*> profiles(_chunk_source_profiles.size());
    for (auto i = 0; i < _chunk_source_profiles.size(); i++) {
        profiles[i] = _chunk_source_profiles[i].get();
    }
    RuntimeProfile::merge_isomorphic_profiles(profiles);

    RuntimeProfile* merged_profile = profiles[0];

    _unique_metrics->copy_all_info_strings_from(merged_profile);
    _unique_metrics->copy_all_counters_from(merged_profile);
}

// ========== ScanOperatorFactory ==========

ScanOperatorFactory::ScanOperatorFactory(int32_t id, ScanNode* scan_node, ChunkBufferLimiterPtr buffer_limiter)
        : SourceOperatorFactory(id, scan_node->name(), scan_node->id()),
          _scan_node(scan_node),
          _buffer_limiter(std::move(buffer_limiter)) {}

Status ScanOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(do_prepare(state));

    return Status::OK();
}

OperatorPtr ScanOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return do_create(degree_of_parallelism, driver_sequence);
}

void ScanOperatorFactory::close(RuntimeState* state) {
    do_close(state);

    OperatorFactory::close(state);
}

// ====================

pipeline::OpFactories decompose_scan_node_to_pipeline(std::shared_ptr<ScanOperatorFactory> scan_operator,
                                                      ScanNode* scan_node, pipeline::PipelineBuilderContext* context) {
    OpFactories ops;

    size_t scan_dop = context->degree_of_parallelism_of_source_operator(scan_operator.get());
    scan_operator->set_degree_of_parallelism(scan_dop);

    ops.emplace_back(std::move(scan_operator));

    if (!scan_node->conjunct_ctxs().empty() || ops.back()->has_runtime_filters()) {
        ops.emplace_back(
                std::make_shared<ChunkAccumulateOperatorFactory>(context->next_operator_id(), scan_node->id()));
    }

    size_t limit = scan_node->limit();
    if (limit != -1) {
        ops.emplace_back(
                std::make_shared<pipeline::LimitOperatorFactory>(context->next_operator_id(), scan_node->id(), limit));
    }

    return ops;
}
} // namespace starrocks::pipeline
