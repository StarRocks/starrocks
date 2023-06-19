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

#include <memory>

#include "column/vectorized_fwd.h"
#include "exec/sort_exec_exprs.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/block_manager.h"

namespace starrocks::spill {
struct SpilledChunkBuildSchema {
    void set_schema(const ChunkPtr& chunk) { _chunk = chunk->clone_empty(0); }
    bool empty() { return _chunk->num_columns() == 0; }
    ChunkUniquePtr new_chunk() { return _chunk->clone_unique(); }
    size_t column_number() const { return _chunk->num_columns(); }

private:
    ChunkPtr _chunk{new Chunk()};
};

struct ChunkBuilder {
    ChunkBuilder() = default;
    ChunkUniquePtr operator()() const { return _spill_chunk_schema->new_chunk(); }
    auto& chunk_schema() { return _spill_chunk_schema; }
    size_t column_number() const { return _spill_chunk_schema->column_number(); }

private:
    std::shared_ptr<SpilledChunkBuildSchema> _spill_chunk_schema;
};

// using ChunkBuilder = std::function<ChunkUniquePtr()>;
enum class SpillFormaterType { NONE, SPILL_BY_COLUMN };

// spill options
struct SpilledOptions {
    SpilledOptions() : SpilledOptions(-1) {}

    SpilledOptions(int init_partition_nums_, bool splittable_ = true)
            : init_partition_nums(init_partition_nums_),
              is_unordered(true),
              splittable(splittable_),
              sort_exprs(nullptr),
              sort_desc(nullptr) {}

    SpilledOptions(SortExecExprs* sort_exprs_, const SortDescs* sort_desc_)
            : init_partition_nums(-1),
              is_unordered(false),
              splittable(false),
              sort_exprs(sort_exprs_),
              sort_desc(sort_desc_) {}

    const int init_partition_nums;
    const std::vector<ExprContext*> partiton_exprs;

    // spilled data need with ordered
    bool is_unordered;

    bool splittable;

    // order by parameters
    const SortExecExprs* sort_exprs;
    const SortDescs* sort_desc;

    // max mem table size for each spiller
    size_t mem_table_pool_size{};
    // memory table peak mem usage
    size_t spill_mem_table_bytes_size{};
    // spilled format type
    SpillFormaterType spill_type{};

    std::string name;

    int32_t plan_node_id = 0;

    CompressionTypePB compress_type = CompressionTypePB::LZ4;

    size_t min_spilled_size = 1 * 1024 * 1024;

    bool read_shared = false;
    int encode_level = 0;

    BlockManager* block_manager = nullptr;
};

// spill strategy
enum class SpillStrategy {
    NO_SPILL,
    SPILL_ALL,
};
} // namespace starrocks::spill