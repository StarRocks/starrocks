// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/olap_chunk_source.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"

namespace starrocks::pipeline {

using starrocks::workgroup::WorkGroupManager;

Status ScanOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));

    _state = state;
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

    RETURN_IF_ERROR(_capture_tablet_rowsets());

    // init filtered_ouput_columns
    for (const auto& col_name : _olap_scan_node.unused_output_column_name) {
        _unused_output_columns.emplace_back(col_name);
    }

    return Status::OK();
}

Status ScanOperator::close(RuntimeState* state) {
    DCHECK(_num_running_io_tasks == 0);

    if (_workgroup == nullptr) {
        state->exec_env()->decrement_num_scan_operators(1);
    }
    for (auto& chunk_source : _chunk_sources) {
        if (chunk_source != nullptr) {
            chunk_source->close(_state);
            chunk_source = nullptr;
        }
    }

    return Operator::close(state);
}

bool ScanOperator::has_output() const {
    if (_is_finished) {
        return false;
    }

    for (const auto& chunk_source : _chunk_sources) {
        if (chunk_source != nullptr && chunk_source->has_output()) {
            return true;
        }
    }

    if (_num_running_io_tasks >= MAX_IO_TASKS_PER_OP) {
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

    return false;
}

bool ScanOperator::pending_finish() const {
    DCHECK(_is_finished);
    // If there isn't next morsel, and any io task is active,
    // we just wait for the io thread to end.
    return _num_running_io_tasks > 0;
}

bool ScanOperator::is_finished() const {
    if (_is_finished) {
        return true;
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

void ScanOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
}

StatusOr<vectorized::ChunkPtr> ScanOperator::pull_chunk(RuntimeState* state) {
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

Status ScanOperator::_try_to_trigger_next_scan(RuntimeState* state) {
    if (_num_running_io_tasks >= MAX_IO_TASKS_PER_OP) {
        return Status::OK();
    }

    // Firstly, find the picked-up morsel, whose can commit an io task.
    for (int i = 0; i < MAX_IO_TASKS_PER_OP; ++i) {
        if (_chunk_sources[i] != nullptr && !_is_io_task_running[i] && _chunk_sources[i]->has_next_chunk()) {
            RETURN_IF_ERROR(_trigger_next_scan(state, i));
        }
    }

    // Secondly, find the unused position of _chunk_sources to pick up a new morsel.
    if (!_morsel_queue->empty()) {
        for (int i = 0; i < MAX_IO_TASKS_PER_OP; ++i) {
            if (_chunk_sources[i] == nullptr || (!_is_io_task_running[i] && !_chunk_sources[i]->has_output())) {
                RETURN_IF_ERROR(_pickup_morsel(state, i));
            }
        }
    }

    return Status::OK();
}

Status ScanOperator::_trigger_next_scan(RuntimeState* state, int chunk_source_index) {
    _num_running_io_tasks++;
    _is_io_task_running[chunk_source_index] = true;

    bool offer_task_success = false;
    if (_workgroup != nullptr) {
        workgroup::ScanTask task = workgroup::ScanTask(_workgroup, [this, state, chunk_source_index](int worker_id) {
            {
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());

                size_t num_read_chunks = 0;
                _chunk_sources[chunk_source_index]->buffer_next_batch_chunks_blocking_for_workgroup(
                        _buffer_size, _is_finished, &num_read_chunks, worker_id, _workgroup);
                // TODO (by laotan332): More detailed information is needed
                _workgroup->incr_period_scaned_chunk_num(num_read_chunks);
            }

            _num_running_io_tasks--;
            _is_io_task_running[chunk_source_index] = false;
        });

        offer_task_success = ExecEnv::GetInstance()->scan_executor()->submit(std::move(task));
    } else {
        PriorityThreadPool::Task task;
        task.work_function = [this, state, chunk_source_index]() {
            {
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
                _chunk_sources[chunk_source_index]->buffer_next_batch_chunks_blocking(_buffer_size, _is_finished);
            }

            _num_running_io_tasks--;
            _is_io_task_running[chunk_source_index] = false;
        };
        // TODO(by satanson): set a proper priority
        task.priority = 20;

        offer_task_success = _io_threads->try_offer(task);
    }

    if (offer_task_success) {
        _io_task_retry_cnt = 0;
    } else {
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

void ScanOperator::set_workgroup(starrocks::workgroup::WorkGroupPtr wg) {
    _workgroup = wg;
}

Status ScanOperator::_pickup_morsel(RuntimeState* state, int chunk_source_index) {
    DCHECK(_morsel_queue != nullptr);
    if (_chunk_sources[chunk_source_index] != nullptr) {
        _chunk_sources[chunk_source_index]->close(state);
        _chunk_sources[chunk_source_index] = nullptr;
    }

    auto maybe_morsel = _morsel_queue->try_get();
    if (maybe_morsel.has_value()) {
        auto morsel = std::move(maybe_morsel.value());
        DCHECK(morsel);

        bool enable_column_expr_predicate = false;
        if (_olap_scan_node.__isset.enable_column_expr_predicate) {
            enable_column_expr_predicate = _olap_scan_node.enable_column_expr_predicate;
        }

        _chunk_sources[chunk_source_index] = std::make_shared<OlapChunkSource>(
                std::move(morsel), _olap_scan_node.tuple_id, _limit, enable_column_expr_predicate, _conjunct_ctxs,
                runtime_in_filters(), runtime_bloom_filters(), _olap_scan_node.key_column_name,
                _olap_scan_node.is_preaggregation, &_unused_output_columns, _unique_metrics.get());
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

Status ScanOperator::_capture_tablet_rowsets() {
    const auto& morsels = _morsel_queue->morsels();
    _tablet_rowsets.resize(morsels.size());
    for (int i = 0; i < morsels.size(); ++i) {
        OlapMorsel* olap_morsel = (OlapMorsel*)morsels[i].get();
        auto* scan_range = olap_morsel->get_scan_range();

        // Get version.
        int64_t version = strtoul(scan_range->version.c_str(), nullptr, 10);

        // Get tablet.
        TTabletId tablet_id = scan_range->tablet_id;
        std::string err;
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (!tablet) {
            std::stringstream ss;
            SchemaHash schema_hash = strtoul(scan_range->schema_hash.c_str(), nullptr, 10);
            ss << "failed to get tablet. tablet_id=" << tablet_id << ", with schema_hash=" << schema_hash
               << ", reason=" << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // Capture row sets of this version tablet.
        {
            std::shared_lock l(tablet->get_header_lock());
            RETURN_IF_ERROR(tablet->capture_consistent_rowsets(Version(0, version), &_tablet_rowsets[i]));
        }
    }

    return Status::OK();
}

Status ScanOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));

    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(_olap_scan_node.tuple_id);
    vectorized::DictOptimizeParser::rewrite_descriptor(state, _conjunct_ctxs, _olap_scan_node.dict_string_id_to_int_ids,
                                                       &(tuple_desc->decoded_slots()));
    return Status::OK();
}

void ScanOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
