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

#include "exec/vectorized/partition/chunks_partitioner.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

class HashPartitionContext;
class HashPartitionContextFactory;

using HashPartitionContextPtr = std::shared_ptr<HashPartitionContext>;
using HashPartitionContextFactoryPtr = std::shared_ptr<HashPartitionContextFactory>;

class HashPartitionContext {
public:
    HashPartitionContext(const std::vector<TExpr>& t_partition_exprs) : _t_partition_exprs(t_partition_exprs) {}

    Status prepare(RuntimeState* state);

    // Add one chunk to partitioner
    Status push_one_chunk_to_partitioner(RuntimeState* state, const vectorized::ChunkPtr& chunk);

    // Pull one chunk from sorters or passthrough_buffer
    StatusOr<vectorized::ChunkPtr> pull_one_chunk(RuntimeState* state);

    // Notify that there is no further input for partitiner
    void sink_complete();

    // Return true if at least one of the sorters has remaining data
    bool has_output();

    // Return true if sink completed and all the data in the chunks_sorters has been pulled out
    bool is_finished();

    int32_t num_partitions() const { return _chunks_partitioner->num_partitions(); }

private:
    const std::vector<TExpr>& _t_partition_exprs;
    std::vector<ExprContext*> _partition_exprs;
    std::vector<vectorized::PartitionColumnType> _partition_types;

    bool _has_nullable_key = false;
    // No more input chunks if after _is_sink_complete is set to true
    bool _is_sink_complete = false;

    vectorized::ChunksPartitionerPtr _chunks_partitioner;

    ChunkPipelineAccumulator _acc;
};

class HashPartitionContextFactory {
public:
    HashPartitionContextFactory(const std::vector<TExpr>& t_partition_exprs) : _t_partition_exprs(t_partition_exprs) {}

    HashPartitionContext* create(int32_t driver_sequence);

private:
    std::unordered_map<int32_t, HashPartitionContextPtr> _ctxs;

    const std::vector<TExpr>& _t_partition_exprs;
};
} // namespace starrocks::pipeline
