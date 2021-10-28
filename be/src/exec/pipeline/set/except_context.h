// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#pragma once

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "common/statusor.h"
#include "exec/olap_common.h"
#include "exec/vectorized/except_hash_set.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks::pipeline {

class ExceptContext;
using ExceptContextPtr = std::shared_ptr<ExceptContext>;

// Used as the shared context for ExceptBuildSinkOperator, ExceptEraseSinkOperator, and ExceptOutputSourceOperator.
class ExceptContext {
public:
    explicit ExceptContext(const int dst_tuple_id) : _dst_tuple_id(dst_tuple_id) {}

    /// The following methods are for Build phase.
    Status prepare(RuntimeState* state, MemTracker* mem_tracker);

    void finish_build_ht() {
        _next_processed_iter = _hash_set->begin();
        _is_build_finished.store(true, std::memory_order_release);
    }

    bool is_build_ht_finished() const { return _is_build_finished.load(std::memory_order_acquire); }

    bool is_ht_empty() const { return _hash_set->empty(); }

    Status append_chunk_to_ht(RuntimeState* state, const ChunkPtr& chunk, const std::vector<ExprContext*>& dst_exprs);

    /// The following methods are for Erase phase.
    void finish_one_erase_driver() { _finished_erase_drivers_num.fetch_add(1, std::memory_order_release); }

    // Used when creating drivers by FragmentExecutor::prepare().
    void create_one_erase_driver() { _erase_drivers_num++; }

    bool is_erase_ht_finished() {
        return _finished_erase_drivers_num.load(std::memory_order_acquire) == _erase_drivers_num;
    }

    Status erase_chunk_from_ht(RuntimeState* state, const ChunkPtr& chunk,
                               const std::vector<ExprContext*>& child_exprs);

    /// The following methods are for Output Phase.
    bool is_output_finished() const { return _next_processed_iter == _hash_set->end(); }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state);

    void close(RuntimeState* state);

private:
    std::unique_ptr<vectorized::ExceptHashSerializeSet> _hash_set =
            std::make_unique<vectorized::ExceptHashSerializeSet>();

    const int _dst_tuple_id;
    // Cache the dest tuple descriptor in the preparation phase of ExceptBuildSinkOperatorFactory.
    TupleDescriptor* _dst_tuple_desc = nullptr;
    // Indicate whether each dest column is nullable. It is set when appending the src chunk to
    // the hast set at the first time by _has_set_dst_nullables.
    std::vector<bool> _dst_nullables;
    bool _has_set_dst_nullables = false;

    // Used to allocate keys in the hash set.
    // _build_pool is created in the preparation phase of ExceptBuildSinkOperatorFactory by calling prepare().
    // It is used to allocate keys in ExceptBuildSinkOperator, and release all allocated keys
    // when ExceptOutputSourceOperator is finished by calling close().
    std::unique_ptr<MemPool> _build_pool = nullptr;

    vectorized::ExceptHashSerializeSet::KeyVector _remained_keys;
    // Used for traversal on the hash set to get the undeleted keys to dest chunk.
    // Init when the hash set is finished building in finish_build_ht().
    vectorized::ExceptHashSerializeSet::Iterator _next_processed_iter;

    // Async between the ExceptBuildSinkOperator threads and the ExceptEraseSinkOperator threads.
    std::atomic<bool> _is_build_finished{false};

    // Async between the ExceptEraseSinkOperator (ERASE) threads and the ExceptEraseSourceOperator (SOURCE) thread.
    // If SOURCE sees _finished_erase_drivers_num is equal to _erase_drivers_num, which means SOURCE sees every
    // increment of _finished_erase_drivers_num in finish_one_erase_driver() called by ERASE, then it is certain
    // to see all the erasing operations on hash set by ERASE, which happen before calling finish_one_erase_driver().
    std::atomic<int32_t> _finished_erase_drivers_num{0};
    // _erase_drivers_num is increased when creating drivers by FragmentExecutor::prepare() before appending them to
    // driver_queue, and read by the dispatcher thread after taking them from driver_queue. Therefore, it is guaranteed
    // by driver_queue that the dispatcher thread can see every increment of _erase_drivers_num by FragmentExecutor::prepare().
    int32_t _erase_drivers_num = 0;
};

} // namespace starrocks::pipeline
