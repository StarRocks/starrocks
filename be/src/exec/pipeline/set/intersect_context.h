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

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "common/statusor.h"
#include "exec/intersect_hash_set.h"
#include "exec/olap_common.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks::pipeline {

class IntersectContext;
using IntersectContextPtr = std::shared_ptr<IntersectContext>;

class IntersectPartitionContextFactory;
using IntersectPartitionContextFactoryPtr = std::shared_ptr<IntersectPartitionContextFactory>;

// Used as the shared context for IntersectBuildSinkOperator, IntersectProbeSinkOperator, and IntersectOutputSourceOperator.
class IntersectContext final : public ContextWithDependency {
public:
    IntersectContext(const int dst_tuple_id, const size_t intersect_times)
            : _dst_tuple_id(dst_tuple_id), _intersect_times(intersect_times) {}
    ~IntersectContext() override = default;

    bool is_ht_empty() const { return _is_hash_set_empty; }

    void finish_build_ht() {
        _is_hash_set_empty = _hash_set->empty();
        _next_processed_iter = _hash_set->begin();
        _hash_set_end_iter = _hash_set->end();
        _finished_dependency_index.fetch_add(1, std::memory_order_release);
    }

    void finish_probe_ht() { _finished_dependency_index.fetch_add(1, std::memory_order_release); }

    bool is_dependency_finished(const int32_t dependency_index) const {
        return _finished_dependency_index.load(std::memory_order_acquire) == dependency_index;
    }

    bool is_output_finished() const { return _next_processed_iter == _hash_set_end_iter; }

    // Called in the preparation phase of IntersectBuildSinkOperator.
    Status prepare(RuntimeState* state, const std::vector<ExprContext*>& build_exprs);

    void close(RuntimeState* state) override;

    Status append_chunk_to_ht(RuntimeState* state, const ChunkPtr& chunk, const std::vector<ExprContext*>& dst_exprs);

    Status refine_chunk_from_ht(RuntimeState* state, const ChunkPtr& chunk,
                                const std::vector<ExprContext*>& child_exprs, int hit_times);

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state);

private:
    std::unique_ptr<IntersectHashSerializeSet> _hash_set = std::make_unique<IntersectHashSerializeSet>();

    const int _dst_tuple_id;
    // Cache the dest tuple descriptor in the preparation phase of IntersectBuildSinkOperatorFactory.
    TupleDescriptor* _dst_tuple_desc = nullptr;
    // Indicate whether each dest column is nullable.
    std::vector<bool> _dst_nullables;

    const size_t _intersect_times;

    // Used to allocate keys in the hash set.
    // _build_pool is created in the preparation phase of IntersectBuildSinkOperatorFactory by calling prepare().
    // It is used to allocate keys in IntersectBuildSinkOperator, and release all allocated keys
    // when IntersectOutputSourceOperator is finished by calling close().
    std::unique_ptr<MemPool> _build_pool = nullptr;

    IntersectHashSerializeSet::KeyVector _remained_keys;
    // Used for traversal on the hash set to get the undeleted keys to dest chunk.
    // Init when the hash set is finished building in finish_build_ht().
    IntersectHashSerializeSet::Iterator _next_processed_iter;
    IntersectHashSerializeSet::Iterator _hash_set_end_iter;
    bool _is_hash_set_empty = false;

    // The BUILD, PROBES, and OUTPUT operators execute sequentially.
    // BUILD -> 1-th PROBE -> 2-th PROBE -> ... -> n-th PROBE -> OUTPUT.
    // _finished_dependency_index will increase by one when a BUILD or PROBE is finished.
    // i-th PROBE must wait for _finished_dependency_index becoming i-1,
    // and OUTPUT must wait for _finished_dependency_index becoming n.
    std::atomic<int32_t> _finished_dependency_index{-1};
};

// The input chunks of BUILD and PROBE are shuffled by the local shuffle operator.
// The number of shuffled partitions is the degree of parallelism (DOP), which means
// the number of partition hash sets and the number of BUILD drivers, PROBE drivers of one child, OUTPUT drivers
// are both DOP. And each pair of BUILD/PROBE/OUTPUT drivers shares a same intersect partition context.
class IntersectPartitionContextFactory {
public:
    IntersectPartitionContextFactory(const size_t dst_tuple_id, const size_t intersect_times)
            : _dst_tuple_id(dst_tuple_id), _intersect_times(intersect_times) {}

    IntersectContextPtr get_or_create(const int partition_id) {
        auto it = _partition_id2ctx.find(partition_id);
        if (it != _partition_id2ctx.end()) {
            return it->second;
        }

        auto ctx = std::make_shared<IntersectContext>(_dst_tuple_id, _intersect_times);
        _partition_id2ctx[partition_id] = ctx;
        return ctx;
    }

private:
    const size_t _dst_tuple_id;
    const size_t _intersect_times;
    std::unordered_map<size_t, IntersectContextPtr> _partition_id2ctx;
};

} // namespace starrocks::pipeline
