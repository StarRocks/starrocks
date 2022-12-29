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

#include <unordered_set>

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exec/intersect_hash_set.h"
#include "exec/olap_common.h"
#include "exprs/expr_context.h"
#include "runtime/mem_pool.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks {
class DescriptorTbl;
class SlotDescriptor;
class TupleDescriptor;
} // namespace starrocks

namespace starrocks {
class IntersectNode final : public ExecNode {
public:
    IntersectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    ~IntersectNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* row_batch, bool* eos) override;
    Status close(RuntimeState* state) override;

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;
    int64_t mem_usage() const {
        int64_t usage = 0;
        if (_hash_set != nullptr) {
            usage += _hash_set->mem_usage();
        }
        if (_build_pool != nullptr) {
            usage += _build_pool->total_reserved_bytes();
        }
        return usage;
    }

private:
    // Tuple id resolved in Prepare() to set tuple_desc_;
    const int _tuple_id;
    // Descriptor for tuples this union node constructs.
    const TupleDescriptor* _tuple_desc;
    // Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    std::vector<std::vector<ExprContext*>> _child_expr_lists;

    struct IntersectColumnTypes {
        TypeDescriptor result_type;
        bool is_nullable = false;
        bool is_constant = false;
    };
    std::vector<IntersectColumnTypes> _types;
    size_t _intersect_times = 0;

    std::unique_ptr<IntersectHashSerializeSet> _hash_set;
    IntersectHashSerializeSet::Iterator _hash_set_iterator;
    IntersectHashSerializeSet::KeyVector _remained_keys;

    // pool for allocate key.
    std::unique_ptr<MemPool> _build_pool;

    RuntimeProfile::Counter* _build_set_timer = nullptr; // time to build hash set
    RuntimeProfile::Counter* _refine_intersect_row_timer = nullptr;
    RuntimeProfile::Counter* _get_result_timer = nullptr;
};

} // namespace starrocks
