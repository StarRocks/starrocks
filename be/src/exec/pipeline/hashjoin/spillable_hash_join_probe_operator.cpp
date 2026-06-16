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

#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <numeric>

#include "base/failpoint/fail_point.h"
#include "common/config_exec_flow_fwd.h"
#include "common/runtime_profile.h"
#include "compute_env/spill/mem_tracker_guard.h"
#include "compute_env/spill/operator_mem_resource_manager.h"
#include "compute_env/spill/partition.h"
#include "compute_env/spill/spill_components.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller.hpp"
#include "compute_env/spill/task_executor.h"
#include "compute_env/spill/yield.h"
#include "exec/hash_joiner.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"
#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "exec/pipeline/query_context.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "runtime/runtime_state_helper.h"

namespace starrocks::pipeline {

DEFINE_FAIL_POINT(spill_hash_join_throw_bad_alloc)

Status SpillableHashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinProbeOperator::prepare(state));
    _need_post_probe = has_post_probe(_join_prober->join_type());
    _probe_spiller->set_metrics(
            spill::SpillProcessMetrics(_unique_metrics.get(), RuntimeStateHelper::mutable_total_spill_bytes(state)));
    metrics.hash_partitions = ADD_COUNTER(_unique_metrics.get(), "SpillPartitions", TUnit::UNIT);
    metrics.build_partition_peak_memory_usage = _unique_metrics->AddHighWaterMarkCounter(
            "SpillBuildPartitionPeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    metrics.prober_peak_memory_usage = _unique_metrics->AddHighWaterMarkCounter(
            "SpillProberPeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    metrics.peak_processing_partition_count = _unique_metrics->AddHighWaterMarkCounter(
            "SpillPeakProcessingPartitionCount", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    RETURN_IF_ERROR(_probe_spiller->prepare(state));
    auto wg = state->fragment_runtime_state()->workgroup();

    // Subscribe the probe driver to both lists of its own probe-spiller: this single driver is both a
    // writer (spilling probe rows through partitioned_spill -> sink list, parks WAIT_FLUSH) and a reader
    // (restoring probe partitions -> source list, parks WAIT_RESTORE). The notify layer allows one driver
    // on both lists. Unconditional: the poller-mode gate lives inside subscribe_*; observer() is valid here
    // (assigned before prepare). The build-done handoff (parks WAIT_BUILD) and the build-side load wakeup
    // (parks WAIT_LATCH) are delivered through the join builder's observable that the base
    // HashJoinProbeOperator::prepare already subscribed via attach_probe_observer(); the load-task
    // count_down defer notifies that same observer directly.
    _probe_spiller->observable().subscribe_sink(state, observer());
    _probe_spiller->observable().subscribe_source(state, observer());
    return Status::OK();
}

void SpillableHashJoinProbeOperator::close(RuntimeState* state) {
    _probe_spiller.reset();
    // all task finished reset partitions
    if (_latch.ready()) {
        _reset_load_partitions();
    }
    HashJoinProbeOperator::close(state);
    DCHECK(!has_output());
    DCHECK(is_finished());
}

bool SpillableHashJoinProbeOperator::has_output() const {
    if (!is_ready()) {
        return false;
    }
    if (_is_finished) {
        return false;
    }
    if (!spilled()) {
        return HashJoinProbeOperator::has_output();
    }

    // Batch gate (single carrier): while a batch is loading, return false -- the build-side load-task
    // count_down defer wakes this INTERMEDIATE_BLOCK driver when the batch finishes loading. kFailed,
    // kLoaded and kNone fall through (kFailed surfaces via the !_status().ok() check below; kLoaded/kNone
    // run the resident-partition logic).
    switch (_batch_state.load(std::memory_order_acquire)) {
    case BatchState::kLoading:
        return false;
    case BatchState::kFailed:
    case BatchState::kLoaded:
    case BatchState::kNone:
        break;
    }

    if (!_status().ok()) {
        return true;
    }

    if (_processing_partitions.empty()) {
        // No build partitions resident yet. If there is still build-side work, report output so the
        // driver runs pull_chunk, which orders the load (this predicate stays pure: no IO is ordered
        // here). Once a batch is in flight the latch is not ready and the branch above parks the driver.
        return !_all_partition_finished();
    }

    // if any hash_join_prober has data.
    for (auto prober : _probers) {
        if (!prober->probe_chunk_empty()) {
            return true;
        }
    }

    // writer full (parks WAIT_FLUSH): the flush-completion defer of the probe-spiller wakes both lists.
    if (_probe_spiller->is_full()) {
        return false;
    }

    if (_is_finishing) {
        if (_all_partition_finished()) {
            return false;
        }

        // reader is empty -> report output so pull_chunk orders the restore (init + trigger). The init
        // branch in pull_chunk is itself latch-gated, and pull_chunk is unreachable while the batch is
        // loading, so reporting output here is safe.
        if (_current_reader.empty()) {
            return true;
        }

        for (size_t i = 0; i < _probers.size(); ++i) {
            if (_current_reader[i]->has_output_data()) {
                // restore data buffered (a restore-completion source event woke us) -> pull_chunk drains it.
                return true;
            } else if (!_current_reader[i]->has_restore_task()) {
                // No data and no restore in flight: report output so pull_chunk orders the restore
                // (trigger_restore is ordered from pull_chunk, never from this predicate). When every
                // reader instead has a restore task in flight we fall through to false and park; the
                // restore-completion source event on its defer wakes the driver (parks WAIT_RESTORE).
                return true;
            }
        }
    }

    return false;
}

bool SpillableHashJoinProbeOperator::need_input() const {
    if (!is_ready()) {
        return false;
    }

    if (_is_finished) {
        return false;
    }

    if (!spilled()) {
        return HashJoinProbeOperator::need_input();
    }

    // Batch gate (single carrier): stop taking probe input while a batch loads; kFailed/kLoaded/kNone
    // fall through to the resident-partition logic.
    switch (_batch_state.load(std::memory_order_acquire)) {
    case BatchState::kLoading:
        return false;
    case BatchState::kFailed:
    case BatchState::kLoaded:
    case BatchState::kNone:
        break;
    }

    if (_processing_partitions.empty()) {
        // No build partitions resident: stop taking probe input. The load is not ordered here (this
        // predicate stays pure); has_output() reports true in this same state, which routes the driver
        // into pull_chunk where the load is ordered. Once a batch is in flight the latch above parks the
        // driver.
        return false;
    }

    if (_probe_spiller->is_full()) {
        return false;
    }

    for (auto prober : _probers) {
        if (!prober->probe_chunk_empty()) {
            return false;
        }
    }

    return true;
}

bool SpillableHashJoinProbeOperator::supports_intermediate_wakeup() const {
    // Delegate to the single definition on the factory. The operator is created only by
    // SpillableHashJoinProbeOperatorFactory::create(), so the down_cast is always valid and the operator-side
    // and factory-side answers cannot drift apart.
    return down_cast<const SpillableHashJoinProbeOperatorFactory*>(_factory)->supports_intermediate_wakeup();
}

BlockReason SpillableHashJoinProbeOperator::block_reason() const {
    // Read only when the probe is parked. Walk the parked branches in the same order as the predicates:
    // build handoff, then latch, then writer-full, then restore-in-flight. A non-spill probe, a finished
    // operator, an error, an empty-processing-partitions state (routed through has_output=true, not a
    // block), or a probe with buffered prober data (runnable) all return NONE. Every named return is passed
    // through named<R, kCoveredWakeups>(), so a reason this operator does not declare in its coverage mask
    // does not compile here, rather than slipping past the runtime park-time check.
    if (!is_ready()) {
        return named<BlockReason::WAIT_BUILD, kCoveredWakeups>();
    }
    if (_is_finished || !spilled()) {
        return named<BlockReason::NONE, kCoveredWakeups>();
    }
    switch (_batch_state.load(std::memory_order_acquire)) {
    case BatchState::kLoading:
        return named<BlockReason::WAIT_LATCH, kCoveredWakeups>();
    case BatchState::kFailed:
    case BatchState::kLoaded:
    case BatchState::kNone:
        break;
    }
    if (!_status().ok()) {
        return named<BlockReason::NONE, kCoveredWakeups>();
    }
    if (_processing_partitions.empty()) {
        // No build partitions resident: has_output() reports true (routes into pull_chunk to order the
        // load), so this is not a park state.
        return named<BlockReason::NONE, kCoveredWakeups>();
    }
    for (auto prober : _probers) {
        if (!prober->probe_chunk_empty()) {
            return named<BlockReason::NONE, kCoveredWakeups>();
        }
    }
    if (_probe_spiller->is_full()) {
        return named<BlockReason::WAIT_FLUSH, kCoveredWakeups>();
    }
    // Finishing with readers initialized and every reader's restore in flight (no buffered data, a restore
    // task pending): the WAIT_RESTORE park. Any reader with no task / buffered data would route into pull_chunk.
    if (_is_finishing && !_all_partition_finished() && !_current_reader.empty()) {
        return named<BlockReason::WAIT_RESTORE, kCoveredWakeups>();
    }
    return named<BlockReason::NONE, kCoveredWakeups>();
}

bool SpillableHashJoinProbeOperator::is_finished() const {
    if (_is_finished) {
        return true;
    }
    if (!spilled()) {
        return HashJoinProbeOperator::is_finished();
    }

    if (_is_finishing && _all_partition_finished()) {
        return true;
    }

    return false;
}

bool SpillableHashJoinProbeOperator::pending_finish() const {
    // The probe is an interior operator that owns two kinds of in-flight IO the driver must outlive,
    // and the driver core reads this only because the interior-operator aggregation sums pending_finish
    // over every operator (not just the edges):
    //   * its own external build-side load tasks (captured raw this + reader, counted by the latch).
    //     kLoading covers tasks still loading (the latch is reset
    //     to N at order time and only reaches ready when the batch leaves kLoading, to kLoaded OR kFailed
    //     -- so a kFailed batch is no longer pinned by this term, the latch having been driven to ready by
    //     the repair, and only the _pending_latch_io tail holds it). _pending_latch_io > 0 covers the
    //     window between the last count_down() (latch ready, kLoaded/kFailed published) and the
    //     source_trigger() the task defer fires -- without it finalize could destroy the driver/observer
    //     under the task (UAF), since these load tasks are not counted in the spiller in-flight IO.
    //   * the probe-spiller's flush/restore IO, via has_running_io_tasks().
    // has_running_io_tasks() survives reset_state. Null-safe on _probe_spiller: close() resets it, and
    // pending_finish() must already be false by then.
    return _batch_state.load(std::memory_order_acquire) == BatchState::kLoading ||
           _pending_latch_io.load(std::memory_order_acquire) > 0 ||
           (_probe_spiller != nullptr && _probe_spiller->has_running_io_tasks());
}

Status SpillableHashJoinProbeOperator::set_finishing(RuntimeState* state) {
    if (!spilled()) {
        return HashJoinProbeOperator::set_finishing(state);
    }
    if (state->is_cancelled()) {
        _probe_spiller->cancel();
    }
    _is_finishing = true;
    return Status::OK();
}

Status SpillableHashJoinProbeOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    return HashJoinProbeOperator::set_finished(state);
}

Status SpillableHashJoinProbeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_status());
    if (!spilled()) {
        return HashJoinProbeOperator::push_chunk(state, chunk);
    }

    RETURN_IF_ERROR(_push_probe_chunk(state, chunk));

    return Status::OK();
}

Status SpillableHashJoinProbeOperator::_push_probe_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    // compute hash
    size_t num_rows = chunk->num_rows();
    auto hash_column = spill::SpillHashColumn::create(num_rows);
    auto& hash_values = hash_column->get_data();

    // TODO: use another hash function
    for (auto& expr_ctx : _join_prober->probe_expr_ctxs()) {
        ASSIGN_OR_RETURN(auto res, expr_ctx->evaluate(chunk.get()));
        res->fnv_hash(hash_values.data(), 0, num_rows);
    }

    auto partition_processer = [&chunk, this, state, &hash_values](spill::SpilledPartition* probe_partition,
                                                                   const std::vector<uint32_t>& selection, int32_t from,
                                                                   int32_t size) {
        // nothing to do for empty partition
        if (could_short_circuit(_join_prober->join_type())) {
            // For left semi join and inner join we can just skip the empty partition
            auto build_partition_iter = _pid_to_build_partition.find(probe_partition->partition_id);
            if (build_partition_iter != _pid_to_build_partition.end()) {
                if (build_partition_iter->second->empty()) {
                    return;
                }
            }
        }

        for (size_t i = from; i < from + size; ++i) {
            DCHECK_EQ(hash_values[selection[i]] & probe_partition->mask(),
                      probe_partition->partition_id & probe_partition->mask());
        }

        auto iter = _pid_to_process_id.find(probe_partition->partition_id);
        if (iter == _pid_to_process_id.end()) {
            auto mem_table = probe_partition->spill_writer->mem_table();
            (void)mem_table->append_selective(*chunk, selection.data(), from, size);
        } else {
            // maybe has some small chunk problem
            // TODO: add chunk accumulator here
            auto partitioned_chunk = chunk->clone_empty();
            (void)partitioned_chunk->append_selective(*chunk, selection.data(), from, size);
            (void)_probers[iter->second]->push_probe_chunk(state, std::move(partitioned_chunk));
        }
        probe_partition->num_rows += size;
    };
    RETURN_IF_ERROR(_probe_spiller->partitioned_spill(state, chunk, hash_column.get(), partition_processer,
                                                      TRACKER_WITH_SPILLER_GUARD(state, _probe_spiller)));

    return Status::OK();
}

Status SpillableHashJoinProbeOperator::_load_partition_build_side(workgroup::YieldContext& ctx, RuntimeState* state,
                                                                  const std::shared_ptr<spill::SpillerReader>& reader,
                                                                  size_t idx) {
    using SyncTaskExecutor = spill::SyncTaskExecutor;
    using MemTrackerGuard = spill::MemTrackerGuard;
    TRY_CATCH_ALLOC_SCOPE_START()
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
    auto builder = _builders[idx];
    auto prober = _probers[idx];
    bool finish = false;
    int64_t hash_table_mem_usage = builder->ht_mem_usage();
    enum SpillLoadPartitionStage { BEGIN = 0, FINISH = 1 };
    ctx.total_yield_point_cnt = FINISH;
    auto wg = ctx.wg;
    while (!finish && !_is_finished) {
        BREAK_IF_YIELD(wg, &ctx.need_yield, ctx.time_spent_ns);
        {
            SCOPED_RAW_TIMER(&ctx.time_spent_ns);
            if (state->is_cancelled()) {
                return Status::Cancelled("cancelled");
            }

            RETURN_IF_ERROR(reader->trigger_restore<SyncTaskExecutor>(state, MemTrackerGuard(tls_mem_tracker)));
            auto chunk_st = reader->restore<SyncTaskExecutor>(state, MemTrackerGuard(tls_mem_tracker));

            FAIL_POINT_TRIGGER_EXECUTE(spill_hash_join_throw_bad_alloc, { throw std::bad_alloc(); });

            if (chunk_st.ok() && chunk_st.value() != nullptr && !chunk_st.value()->is_empty()) {
                int64_t old_mem_usage = hash_table_mem_usage;
                RETURN_IF_ERROR(builder->append_chunk(state, chunk_st.value()));
                hash_table_mem_usage = builder->ht_mem_usage();
                COUNTER_ADD(metrics.build_partition_peak_memory_usage, hash_table_mem_usage - old_mem_usage);
            } else if (chunk_st.status().is_end_of_file()) {
                RETURN_IF_ERROR(builder->build(state));
                prober->attach(builder, _join_prober->probe_metrics());
                finish = true;
            } else if (!chunk_st.ok()) {
                return chunk_st.status();
            }
        }
    }
    if (finish) {
        DCHECK_EQ(builder->hash_table_row_count(), _processing_partitions[idx]->num_rows);
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return Status::OK();
}

Status SpillableHashJoinProbeOperator::_load_all_partition_build_side(RuntimeState* state) {
    auto spill_readers = _join_builder->spiller()->get_partition_spill_readers(_processing_partitions);
    _latch.reset(_processing_partitions.size());
    // Publish kLoading on the pipeline thread before any submit, so the gates park the driver the moment a
    // batch is in flight. The last count_down (or the submit-failure repair) flips it to kLoaded/kFailed.
    _batch_state.store(BatchState::kLoading, std::memory_order_release);
    int32_t driver_id = CurrentThread::current().get_driver_id();
    auto query_ctx = state->query_ctx()->weak_from_this();
    // Build-side load wakeup target: these are the probe's own external load tasks, invisible to
    // any spiller event, so the latch's last count_down must notify the driver directly. observer() is
    // captured on the pipeline thread (stable here) and is null in poller mode, where the predicate is
    // ticked by polling instead -- the null check keeps the poller path side-effect free.
    auto* probe_observer = observer();
    for (size_t i = 0; i < _processing_partitions.size(); ++i) {
        std::shared_ptr<spill::SpillerReader> reader = std::move(spill_readers[i]);
        auto guard = TRACKER_WITH_SPILLER_RES_GUARD(state, _join_builder->spiller());
        // Claim a lifetime slot on the PIPELINE THREAD before submitting, so pending_finish() already
        // observes _pending_latch_io > 0 the instant the task can complete. Paired with the decrement in
        // the defer below (after source_trigger) or, if submit fails, with the repair loop.
        _pending_latch_io.fetch_add(1, std::memory_order_release);
        auto task = [this, state, reader, i, query_ctx, driver_id, guard, probe_observer](auto& yield_ctx) {
            auto yield_defer = yield_ctx.defer_finished();
            RETURN_IF(!guard.scoped_begin(), (void)0);
            DEFER_GUARD_END(guard);
            SCOPED_SET_TRACE_INFO(driver_id, state->query_id(), state->fragment_instance_id());
            SCOPED_SET_TRACE_PLAN_NODE_ID(get_plan_node_id());
            auto defer = CancelableDefer([&]() {
                // count_down() returns true on the last task: the batch is fully loaded or erroring out.
                // Publish-then-notify: the last task moves kLoading -> kLoaded (or kFailed when the
                // operator status is already non-OK, published by _update_status / _status() in the task
                // body before this defer). The BatchState flip and _operator_status are visible BEFORE
                // the trigger, so the probe driver woken on kLoading (INTERMEDIATE_BLOCK via
                // check_is_ready, or PENDING_FINISH via is_still_pending_finish) observes the terminal
                // state. The latch underneath stays the counting mechanism.
                const bool latch_ready = _latch.count_down();
                if (latch_ready) {
                    _batch_state.store(_status().ok() ? BatchState::kLoaded : BatchState::kFailed,
                                       std::memory_order_release);
                }
                if (latch_ready && _is_finished) {
                    _reset_load_partitions();
                }
                // Data wakeup, slot release and the releaser wakeup, run by this object's destructor as it
                // leaves the defer body -- after the BatchState publish and the optional reset above. The
                // observer stays valid under the task's query-lifetime guard, whose defer unwinds after this.
                LatchSlotCompletion completion(latch_ready, probe_observer, _pending_latch_io);
            });
            if (!_status().ok()) {
                return;
            }
            if (!yield_ctx.task_context_data.has_value()) {
                yield_ctx.task_context_data = std::make_shared<spill::SpillIOTaskContext>();
            }
            yield_ctx.time_spent_ns = 0;
            yield_ctx.need_yield = false;
            _update_status(_load_partition_build_side(yield_ctx, state, reader, i));
            if (yield_ctx.need_yield) {
                // Re-queued via force_submit: keep the lifetime slot and the latch slot held (defer
                // cancelled), they are released only when the re-submitted task finally runs its defer.
                defer.cancel();
                yield_defer.cancel();
            }
        };
        auto yield_func = [&](workgroup::ScanTask&& task) { spill::IOTaskExecutor::force_submit(std::move(task)); };
        auto io_task =
                workgroup::ScanTask(_join_builder->spiller()->options().wg, std::move(task), std::move(yield_func));
        io_task.set_query_type(state->query_options().query_type);
        if (Status submit_st = spill::IOTaskExecutor::submit(std::move(io_task)); !submit_st.ok()) {
            // Submit failure mid-loop: task i and everything after it will never run, so their latch
            // slots never count down. Repair on the PIPELINE THREAD (the driver is still alive): publish
            // the error, then count the latch down for the un-submitted slots [i, N). The latch was reset
            // to N upfront and slots [0, i) were submitted (their tasks count down on their own); only
            // slot i has an outstanding _pending_latch_io increment (slots [i+1, N) were never reached by
            // the loop, so they were never incremented), so release exactly that one lifetime slot here.
            // If the repair drove the latch to ready, publish kFailed and emit the load wakeup ourselves
            // (the probe sleeps on kLoading, which no spiller event will flip).
            _update_status(std::move(submit_st));
            bool latch_ready = false;
            for (size_t j = i; j < _processing_partitions.size(); ++j) {
                latch_ready = _latch.count_down();
            }
            // Publish kFailed before the notify (publish-then-notify): if the repair drove the latch to
            // ready, this is the terminal state the woken driver must observe; if some [0, i) task is still
            // loading, that task will be the last count_down and publish kFailed (the error is already in
            // _operator_status), so this store only matters on the latch-ready branch.
            if (latch_ready) {
                _batch_state.store(BatchState::kFailed, std::memory_order_release);
            }
            if (latch_ready && _is_finished) {
                _reset_load_partitions();
            }
            // Slot i's completion, symmetric with the happy-path defer. (The repair runs on the pipeline
            // thread, where the driver is trivially alive.) The nested scope runs the destructor's ordered
            // wakeups before _status() is read for the return, so the wakeups happen before the order error
            // surfaces.
            { LatchSlotCompletion completion(latch_ready, probe_observer, _pending_latch_io); }
            return _status();
        }
    }
    return Status::OK();
}

SpillableHashJoinProbeOperator::LatchSlotCompletion::~LatchSlotCompletion() noexcept {
    // Data wakeup: only the task whose count_down() made the latch ready carries one, and the lifetime
    // slot is still held (this object's fetch_sub below has not run yet) so finalize cannot run.
    if (_latch_ready && _observer != nullptr) {
        _observer->source_trigger();
    }
    // Release wakeup: fired by whichever task drops _pending_latch_io to zero -- with N > 1 concurrent load
    // tasks that need not be the task whose count_down() returned true, so gating on latch_ready would stay
    // silent at counter zero and wedge the driver in PENDING_FINISH forever. fetch_sub returns the prior
    // value, so == 1 identifies the zero-dropping task exactly: pending_finish() reads the zero, and a
    // release at a non-zero count is just a spurious wakeup.
    if (_pending_latch_io.fetch_sub(1, std::memory_order_release) == 1 && _observer != nullptr) {
        _observer->source_trigger();
    }
}

Status SpillableHashJoinProbeOperator::_acquire_and_load_partitions_if_needed(RuntimeState* state) {
    // Order the build-side load from the pull path, never from the predicates, so has_output() and
    // need_input() stay side-effect free. Guarded by the same conditions the predicates test: a batch in
    // flight or all partitions processed make this a no-op. _load_all_partition_build_side publishes
    // kLoading, resets the latch to N and submits N external load tasks; each task's count_down defer
    // publishes kLoaded/kFailed and emits the load wakeup that releases this INTERMEDIATE_BLOCK driver. A
    // synchronous submit failure is recorded via _update_status and returned here so the caller can
    // RETURN_IF_ERROR it (the [[nodiscard]] return is the side-channel-free path for the order error;
    // asynchronous task failures still surface through _operator_status / kFailed). Called only from
    // pull_chunk; push_chunk does not order the load (push cannot outrun the acquire: need_input() is false
    // while _processing_partitions is empty, and only this pull-side acquire can populate it).
    if (_latch.ready() && _processing_partitions.empty() && !_all_partition_finished()) {
        _acquire_next_partitions(state);
        // _load_all_partition_build_side already records a synchronous submit failure into
        // _operator_status (and publishes kFailed) before returning it; propagate it so pull_chunk's
        // RETURN_IF_ERROR aborts the order path rather than dropping into the kFailed gate on re-entry.
        RETURN_IF_ERROR(_load_all_partition_build_side(state));
    }
    return Status::OK();
}

void SpillableHashJoinProbeOperator::_update_status(Status&& status) const {
    if (!status.ok()) {
        std::lock_guard guard(_mutex);
        _operator_status = std::move(status);
    }
}

Status SpillableHashJoinProbeOperator::_status() const {
    RETURN_IF_ERROR(_join_builder->spiller()->task_status());
    std::lock_guard guard(_mutex);
    return _operator_status;
}

void SpillableHashJoinProbeOperator::_check_partitions() {
    if (_is_finishing) {
#ifndef NDEBUG
        auto partitioned_writer = down_cast<spill::PartitionedSpillerWriter*>(_probe_spiller->writer().get());
        size_t build_rows = 0;
        for (const auto& [level, partitions] : partitioned_writer->level_to_partitions()) {
            auto writer = down_cast<spill::PartitionedSpillerWriter*>(_join_builder->spiller()->writer().get());
            auto& build_partitions = writer->level_to_partitions().find(level)->second;
            DCHECK_EQ(build_partitions.size(), partitions.size());
            for (size_t i = 0; i < partitions.size(); ++i) {
                build_rows += build_partitions[i]->num_rows;
            }
            // CHECK if left table is the same as right table
            // for (size_t i = 0; i < partitions.size(); ++i) {
            //     DCHECK_EQ(partitions[i]->num_rows, build_partitions[i]->num_rows);
            // }
        }
        DCHECK_EQ(build_rows, _join_builder->spiller()->spilled_append_rows());
#endif
    }
}

void SpillableHashJoinProbeOperator::_reset_load_partitions() {
    DCHECK(_latch.ready());
    // Reset the batch carrier back to kNone: no batch is ordered until the next pull-side acquire moves it
    // to kLoading. The gates then route through the empty-_processing_partitions branch (order or finish).
    _batch_state.store(BatchState::kNone, std::memory_order_release);
    std::lock_guard guard(_mutex);
    // release memory
    _processing_partitions.clear();
    _current_reader.clear();
    _builders.clear();
    _probers.clear();
    _component_pool.clear();
}

Status SpillableHashJoinProbeOperator::_restore_probe_partition(RuntimeState* state) {
    for (size_t i = 0; i < _probers.size(); ++i) {
        auto guard = TRACKER_WITH_SPILLER_RES_GUARD(state, _probe_spiller, std::weak_ptr(_current_reader[i]));
        // probe partition has been processed
        if (_probe_read_eofs[i]) continue;
        if (!_current_reader[i]->has_restore_task()) {
            RETURN_IF_ERROR(_current_reader[i]->trigger_restore(state, guard));
        }
        if (_current_reader[i]->has_output_data()) {
            auto chunk_st = _current_reader[i]->restore(state, guard);
            if (chunk_st.ok() && chunk_st.value() && !chunk_st.value()->is_empty()) {
                RETURN_IF_ERROR(_probers[i]->push_probe_chunk(state, std::move(chunk_st.value())));
            } else if (chunk_st.status().is_end_of_file()) {
                _probe_read_eofs[i] = true;
            } else if (!chunk_st.ok()) {
                return chunk_st.status();
            }
        }
    }
    return Status::OK();
}

StatusOr<ChunkPtr> SpillableHashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_status());
    if (!spilled()) {
        return HashJoinProbeOperator::pull_chunk(state);
    }

    // Order the build-side partition load here, on the pull path, so has_output()/need_input() stay
    // side-effect free. has_output() reports true when a batch must be (re)loaded, which routes the
    // driver into this pull_chunk; the load publishes kLoading so the next predicate evaluation parks the
    // driver as INTERMEDIATE_BLOCK until the load-task wakeup flips it to kLoaded. The [[nodiscard]] order
    // status surfaces a synchronous submit failure here, before the gate below.
    RETURN_IF_ERROR(_acquire_and_load_partitions_if_needed(state));

    // Batch gate (single carrier): nothing below may run -- not _check_partitions, not the reader init,
    // not restore, not probe -- until the batch is loaded. The build->prober attach happens on the load
    // task's EOF (in _load_partition_build_side); entering the init/restore path while a batch is loading
    // would push into a prober whose builder is not yet attached (SIGSEGV). The switch is exhaustive
    // (-Werror=switch, no default):
    //   * kLoading -> return nullptr (parked, woken by the load-task wakeup);
    //   * kFailed  -> return the error (_operator_status was published before kFailed, so _status() is
    //                 guaranteed non-OK) -- never fall through into restore/probe against unloaded builders;
    //   * kLoaded  -> proceed (builders attached, latch ready);
    //   * kNone    -> proceed: the acquire above was a no-op (all partitions finished or no work), and the
    //                 downstream logic handles empty _processing_partitions / finish routing.
    switch (_batch_state.load(std::memory_order_acquire)) {
    case BatchState::kLoading:
        return nullptr;
    case BatchState::kFailed:
        RETURN_IF_ERROR(_status());
        return nullptr; // unreachable: kFailed implies non-OK _status(); keeps the switch total
    case BatchState::kLoaded:
    case BatchState::kNone:
        break;
    }

    _check_partitions();

    auto all_probe_partition_is_empty = [this]() {
        for (auto& _prober : _probers) {
            if (!_prober->probe_chunk_empty()) {
                return false;
            }
        }
        return true;
    };

    bool probe_has_no_output = all_probe_partition_is_empty() && !_has_probe_remain;

    // The reader-init branch is additionally batch-gated. The batch gate above already guarantees a
    // kLoaded batch here (we returned otherwise); the gate is duplicated in the branch itself as defense
    // against future code reorderings -- init must never run while the build batch is still loading, or it
    // would restore into probers whose builders are not yet attached.
    if (_batch_state.load(std::memory_order_acquire) == BatchState::kLoaded && _current_reader.empty() &&
        _is_finishing && probe_has_no_output) {
        // init spill reader
        _current_reader = _probe_spiller->get_partition_spill_readers(_processing_partitions);
        _probe_read_eofs.assign(_current_reader.size(), false);
        _probe_post_eofs.assign(_current_reader.size(), false);
        _has_probe_remain = true;
    }

    // restore chunk from spilled partition then push it to hash join prober
    if (!_current_reader.empty() && all_probe_partition_is_empty()) {
        RETURN_IF_ERROR(_restore_probe_partition(state));
    }

    // probe chunk
    for (size_t i = 0; i < _probers.size(); ++i) {
        if (!_probers[i]->probe_chunk_empty()) {
            ASSIGN_OR_RETURN(auto res, _probers[i]->probe_chunk(state));
            return res;
        }
    }

    size_t eofs = std::accumulate(_probe_read_eofs.begin(), _probe_read_eofs.end(), 0);
    if (_need_post_probe && _has_probe_remain) {
        if (_is_finishing) {
            bool has_remain = false;
            for (size_t i = 0; i < _probers.size(); ++i) {
                if (!_probe_post_eofs[i] && _probe_read_eofs[i]) {
                    bool has_remain = false;
                    ASSIGN_OR_RETURN(auto res, _probers[i]->probe_remain(state, &has_remain));
                    _probe_post_eofs[i] = !has_remain;
                    if (res && !res->is_empty()) {
                        return res;
                    }
                }
                has_remain |= !_probe_post_eofs[i];
            }
            _has_probe_remain = has_remain;
        }
    } else {
        _has_probe_remain = false;
    }

    // processing partitions
    if (_is_finishing && eofs == _processing_partitions.size() && !_has_probe_remain) {
        DCHECK(all_probe_partition_is_empty());
        // current partition is finished
        for (auto* partition : _processing_partitions) {
            _processed_partitions.emplace(partition->partition_id);
        }
        _has_probe_remain = false;
        COUNTER_SET(metrics.build_partition_peak_memory_usage, 0);
        COUNTER_SET(metrics.peak_processing_partition_count, 0);
        _reset_load_partitions();
    }

    return nullptr;
}

bool SpillableHashJoinProbeOperator::spilled() const {
    return _join_builder->spiller()->spilled();
}

void SpillableHashJoinProbeOperator::_acquire_next_partitions(RuntimeState* state) {
    // get all spill partition
    if (_build_partitions.empty()) {
        _join_builder->spiller()->get_all_partitions(&_build_partitions);
        for (const auto* partition : _build_partitions) {
            _pid_to_build_partition[partition->partition_id] = partition;
        }

        _probe_spiller->set_partition(_build_partitions);
        COUNTER_SET(metrics.hash_partitions, (int64_t)_build_partitions.size());
    }

    size_t bytes_usage = 0;
    size_t available_bytes =
            std::min<size_t>(spill::OperatorMemoryResourceManager::compute_available_memory_bytes(*state),
                             static_cast<size_t>(_spill_hash_join_probe_op_max_bytes / _degree_of_parallelism));
    // process the partition in memory firstly
    if (_processing_partitions.empty()) {
        for (auto partition : _build_partitions) {
            if (partition->in_mem && !_processed_partitions.count(partition->partition_id)) {
                if ((partition->mem_size + bytes_usage < available_bytes || _processing_partitions.empty()) &&
                    std::find(_processing_partitions.begin(), _processing_partitions.end(), partition) ==
                            _processing_partitions.end()) {
                    _processing_partitions.emplace_back(partition);
                    bytes_usage += partition->mem_size;
                    _pid_to_process_id.emplace(partition->partition_id, _processing_partitions.size() - 1);
                    COUNTER_ADD(metrics.peak_processing_partition_count, 1);
                }
            }
        }
    }

    // process the partition could be hold in memory
    if (_processing_partitions.empty()) {
        for (const auto* partition : _build_partitions) {
            if (!partition->in_mem && !_processed_partitions.count(partition->partition_id)) {
                if ((partition->bytes + bytes_usage < available_bytes || _processing_partitions.empty()) &&
                    std::find(_processing_partitions.begin(), _processing_partitions.end(), partition) ==
                            _processing_partitions.end()) {
                    _processing_partitions.emplace_back(partition);
                    bytes_usage += partition->bytes;
                    _pid_to_process_id.emplace(partition->partition_id, _processing_partitions.size() - 1);
                    COUNTER_ADD(metrics.peak_processing_partition_count, 1);
                }
            }
        }
    }
    _component_pool.clear();
    size_t process_partition_nums = _processing_partitions.size();
    _probers.resize(process_partition_nums);
    _builders.resize(process_partition_nums);
    for (size_t i = 0; i < process_partition_nums; ++i) {
        _probers[i] = _join_prober->new_prober(&_component_pool);
        _builders[i] = _join_builder->new_builder(&_component_pool);
        _builders[i]->create(_join_builder->hash_table_param());
        _probe_read_eofs.assign(process_partition_nums, true);
        _probe_post_eofs.assign(process_partition_nums, false);
    }
}

bool SpillableHashJoinProbeOperator::_all_loaded_partition_data_ready() {
    // check all loaded partition data ready
    return std::all_of(_builders.begin(), _builders.end(), [](const auto* builder) { return builder->ready(); });
}

bool SpillableHashJoinProbeOperator::_all_partition_finished() const {
    // In some cases has_output may be skipped.
    // So we call build_partitions.empty() first to make sure the parition loads
    return !_build_partitions.empty() && _processed_partitions.size() == _build_partitions.size();
}

Status SpillableHashJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinProbeOperatorFactory::prepare(state));

    _spill_options = std::make_shared<spill::SpilledOptions>(config::spill_init_partition, false);
    _spill_options->spill_mem_table_bytes_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->block_manager = state->query_runtime_state()->query_spill_manager()->block_manager();
    _spill_options->name = "hash-join-probe";
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();
    _spill_options->wg = state->fragment_runtime_state()->workgroup();
    _spill_options->enable_buffer_read = state->enable_spill_buffer_read();
    _spill_options->max_read_buffer_bytes = state->max_spill_read_buffer_bytes_per_driver();
    _spill_options->spill_hash_join_probe_op_max_bytes = state->spill_hash_join_probe_op_max_bytes();

    return Status::OK();
}

OperatorPtr SpillableHashJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto spiller = _spill_factory->create(*_spill_options);

    auto prober = std::make_shared<SpillableHashJoinProbeOperator>(
            this, _id, "spillable_hash_join_probe", _plan_node_id, driver_sequence,
            _hash_joiner_factory->create_prober(degree_of_parallelism, driver_sequence),
            _hash_joiner_factory->get_builder(degree_of_parallelism, driver_sequence));

    prober->set_probe_spiller(spiller);
    prober->set_degree_of_parallelism(degree_of_parallelism);
    prober->set_spill_hash_join_probe_op_max_bytes(_spill_options->spill_hash_join_probe_op_max_bytes);

    return prober;
}

} // namespace starrocks::pipeline
