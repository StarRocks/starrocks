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

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "compute_env/spill/partition.h"
#include "compute_env/spill/spill_components.h"
#include "compute_env/spill/spiller_factory.h"
#include "exec/hash_join_components.h"
#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"
#include "gutil/macros.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks::pipeline {

struct NoBlockCountDownLatch {
    void reset(int32_t total) { _count_down = total; }

    // return true if this is the last count down
    bool count_down() {
        DCHECK_GT(_count_down, 0);
        return _count_down.fetch_sub(1) == 1;
    }

    bool ready() const { return _count_down == 0; }

private:
    std::atomic_int32_t _count_down{};
};

// Single carrier of the build-side batch phase. The latch below stays the counting mechanism underneath
// it, but the gates read only this enum: latch.ready() on its own carries no meaning, because the repair
// drives the latch to ready on failure too, so latch.ready() cannot tell a loaded batch from a failed one
// (the pull would otherwise run restore/probe against builders that were never loaded). The last count_down
// (or the submit-failure repair) publishes kLoaded/kFailed before its notify, so a driver that is woken
// observes the terminal state. The enum lives here, local to the operator, rather than in the shared
// primitives -- the batch semantics are the operator's own.
enum class BatchState : uint8_t {
    kNone,    // no batch ordered (initial / after _reset_load_partitions)
    kLoading, // a batch is in flight: latch reset to N, load tasks submitted, builders loading
    kLoaded,  // the batch is fully loaded; builders attached, the latch reached ready
    kFailed,  // a load task or its submit failed; _operator_status published before this state
};

struct SpillableHashJoinProbeMetrics {
    RuntimeProfile::Counter* hash_partitions = nullptr;
    RuntimeProfile::Counter* probe_shuffle_timer = nullptr;
    RuntimeProfile::HighWaterMarkCounter* prober_peak_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* build_partition_peak_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* peak_processing_partition_count = nullptr;
};

class SpillableHashJoinProbeOperator final : public HashJoinProbeOperator {
public:
    template <class... Args>
    SpillableHashJoinProbeOperator(Args&&... args) : HashJoinProbeOperator(std::forward<Args>(args)...) {}
    ~SpillableHashJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool need_input() const override;

    bool is_finished() const override;

    bool pending_finish() const override;

    // The probe is an interior operator: its interior-position blocks (latch not ready, writer full,
    // restore in flight) are covered by its own build-side load-task notifications and its probe-spiller
    // subscriptions, so the driver core may park it in INTERMEDIATE_BLOCK. Returns its own factory's value
    // (the operator is created only by SpillableHashJoinProbeOperatorFactory), so the factory is the single
    // definition and the two can never disagree.
    bool supports_intermediate_wakeup() const override;

    // Names the phase the probe is parked in, mapping each parked branch onto BlockReason:
    // build not ready -> WAIT_BUILD (build-done handoff), latch not ready -> WAIT_LATCH (build-side load),
    // writer full -> WAIT_FLUSH, all readers restoring -> WAIT_RESTORE. All four are covered: WAIT_BUILD by
    // the join builder observable (attach_probe_observer), WAIT_LATCH by the load-task count_down defer
    // (source_trigger), WAIT_FLUSH/WAIT_RESTORE by the probe-spiller sink/source lists.
    BlockReason block_reason() const override;
    // Compile-time copy of the wakeup table: the declared coverage sits next to the class, and the
    // static_assert below keeps every reason this operator can name inside the mask.
    static constexpr uint32_t kCoveredWakeups =
            block_reason_bit(BlockReason::WAIT_FLUSH) | block_reason_bit(BlockReason::WAIT_RESTORE) |
            block_reason_bit(BlockReason::WAIT_LATCH) | block_reason_bit(BlockReason::WAIT_BUILD);
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    Status set_finishing(RuntimeState* state) override;

    Status set_finished(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    void set_probe_spiller(std::shared_ptr<spill::Spiller> spiller) { _probe_spiller = std::move(spiller); }

    void set_degree_of_parallelism(int32_t degree_of_parallelism) { _degree_of_parallelism = degree_of_parallelism; }

    void set_spill_hash_join_probe_op_max_bytes(int64_t op_bytes) { _spill_hash_join_probe_op_max_bytes = op_bytes; }

private:
    bool spilled() const;

    // acquire next build-side partitions
    void _acquire_next_partitions(RuntimeState* state);

    bool _all_loaded_partition_data_ready();

    // indicates that all partitions to be processed are complete
    bool _all_partition_finished() const;

    Status _load_all_partition_build_side(RuntimeState* state);

    // Orders the build-side partition load (acquire the next batch of build partitions and submit one
    // external load task per partition) when the previous batch is fully loaded and there is still work to
    // do. It has side effects; it is called only from pull_chunk, so has_output()/need_input() stay pure
    // (the block-reason copy combines predicate results and must not observe side effects). A no-op once a
    // batch is in flight (kLoading) or all partitions are processed. Returns the synchronous order error
    // directly (the asynchronous load-task failures travel through _operator_status / kFailed); the caller
    // must RETURN_IF_ERROR it, so a submit error is not silently dropped.
    [[nodiscard]] Status _acquire_and_load_partitions_if_needed(RuntimeState* state);

    // RAII owner of the latch-slot completion order, like the spiller's GuardedCompletion: the destructor
    // runs the one fixed order -- data wakeup while the lifetime slot is held, release the slot, then the
    // release wakeup that lets a PENDING_FINISH driver finalize -- so neither the load-task defer nor the
    // submit-failure repair has to write the order out by hand. Constructed on the thread that resolved the
    // count_down (the IO task at completion, or the pipeline thread for the repair), capturing whether that
    // count_down made the latch ready, the observer to notify (null in poller mode), and the slot counter to
    // drop. It holds the slot counter by reference, so the destructor's fetch_sub sees the live counter; a
    // non-owning observer pointer stays valid under the task's query-lifetime guard, which unwinds after
    // this object. Move/copy would duplicate the one-shot wakeups, so both are disallowed.
    class LatchSlotCompletion {
    public:
        LatchSlotCompletion(bool latch_ready, PipelineObserver* observer, std::atomic_int32_t& pending_latch_io)
                : _latch_ready(latch_ready), _observer(observer), _pending_latch_io(pending_latch_io) {}
        ~LatchSlotCompletion() noexcept;
        DISALLOW_COPY_AND_MOVE(LatchSlotCompletion);

    private:
        const bool _latch_ready;
        PipelineObserver* const _observer;
        std::atomic_int32_t& _pending_latch_io;
    };

    Status _load_partition_build_side(workgroup::YieldContext& ctx, RuntimeState* state,
                                      const std::shared_ptr<spill::SpillerReader>& reader, size_t idx);

    void _update_status(Status&& status) const;

    Status _status() const;

    Status _push_probe_chunk(RuntimeState* state, const ChunkPtr& chunk);

    Status _restore_probe_partition(RuntimeState* state);

    // some DCHECK for hash table/partition num_rows
    void _check_partitions();

    void _reset_load_partitions();

private:
    SpillableHashJoinProbeMetrics metrics;

    std::vector<const SpillPartitionInfo*> _build_partitions;
    std::unordered_map<int32_t, const SpillPartitionInfo*> _pid_to_build_partition;
    std::vector<const SpillPartitionInfo*> _processing_partitions;
    std::unordered_set<int32_t> _processed_partitions;

    std::vector<std::shared_ptr<spill::SpillerReader>> _current_reader;
    std::vector<bool> _probe_read_eofs;
    std::vector<bool> _probe_post_eofs;
    bool _has_probe_remain = true;
    std::shared_ptr<spill::Spiller> _probe_spiller;

    ObjectPool _component_pool;
    std::vector<HashJoinProber*> _probers;
    std::vector<HashJoinBuilder*> _builders;
    std::unordered_map<int32_t, int32_t> _pid_to_process_id;

    bool _is_finished = false;
    bool _is_finishing = false;

    int32_t _degree_of_parallelism;
    int64_t _spill_hash_join_probe_op_max_bytes;

    // Single carrier of the build-side batch phase read by every gate (pull_chunk load gate, has_output,
    // need_input, block_reason, pending_finish). Published on the pipeline thread at order time (kLoading,
    // before any submit) and by the IO threads at completion (kLoaded / kFailed, by the last count_down or
    // the submit-failure repair, before the notify), reset to kNone by _reset_load_partitions. The latch
    // below remains the counting mechanism underneath it.
    std::atomic<BatchState> _batch_state{BatchState::kNone};
    NoBlockCountDownLatch _latch;
    // Lifetime guard for the build-side latch load tasks. These tasks do not pass through the probe-spiller
    // checkpoints and are not counted in the spiller's in-flight IO, so while a load task can still
    // complete, pending_finish() could otherwise read false and finalize would destroy the driver (and its
    // observer) under the task -> UAF. Incremented on the pipeline thread before each latch-task submit;
    // each completion runs the LatchSlotCompletion order (data wakeup -> decrement this counter -> release
    // wakeup), so the decrement is bracketed by source_trigger()s rather than following the last one. The
    // submit-failure repair decrements the un-submitted slots on the pipeline thread through the same
    // object. pending_finish() reads it, so finalize cannot run while a load task is still in flight.
    std::atomic_int32_t _pending_latch_io{0};

    static_assert(SpillableHashJoinProbeOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_FLUSH));
    static_assert(SpillableHashJoinProbeOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_RESTORE));
    static_assert(SpillableHashJoinProbeOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_LATCH));
    static_assert(SpillableHashJoinProbeOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_BUILD));
    mutable std::mutex _mutex;
    mutable Status _operator_status;

    bool _need_post_probe = false;
};

class SpillableHashJoinProbeOperatorFactory : public HashJoinProbeOperatorFactory {
public:
    template <class... Args>
    SpillableHashJoinProbeOperatorFactory(Args&&... args) : HashJoinProbeOperatorFactory(std::forward<Args>(args)...){};

    ~SpillableHashJoinProbeOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    // Event-scheduler opt-in, gated on enable_spill_join_events (default false -> stays on the poller).
    // The probe is an intermediate operator: the fragment gate consults this only because the probe
    // declares an interior wakeup below, so once build flips the probe must flip too. Effective only with
    // the driver-core INTERMEDIATE_BLOCK mirrors and the pure probe predicates.
    bool support_event_scheduler() const override { return config::enable_spill_join_events; }

    // The probe parks in an interior position (latch not ready, writer full, restore in flight) and is
    // woken by its own load-task notifications and its probe-spiller subscriptions, so the fragment gate
    // must weigh its support_event_scheduler() opt-out. Unconditional: the interior wakeup capability is
    // independent of the config flip (which only toggles whether it opts in).
    bool supports_intermediate_wakeup() const override { return true; }

private:
    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
};

} // namespace starrocks::pipeline
