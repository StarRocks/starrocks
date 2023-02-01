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

#include <utility>

#include "exec/pipeline/operator.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks {

class PriorityThreadPool;

namespace pipeline {

class SourceOperator;
using SourceOperatorPtr = std::shared_ptr<SourceOperator>;
class MorselQueueFactory;

class SourceOperatorFactory : public OperatorFactory {
public:
    SourceOperatorFactory(int32_t id, const std::string& name, int32_t plan_node_id)
            : OperatorFactory(id, name, plan_node_id) {}
    bool is_source() const override { return true; }
    // with_morsels returning true means that the SourceOperator needs attach to MorselQueue, only
    // OlapScanOperator needs to do so.
    virtual bool with_morsels() const { return false; }
    // Set the DOP(degree of parallelism) of the SourceOperator, SourceOperator's DOP determine the Pipeline's DOP.
    void set_degree_of_parallelism(size_t degree_of_parallelism) { _degree_of_parallelism = degree_of_parallelism; }
    virtual size_t degree_of_parallelism() const { return _degree_of_parallelism; }

    MorselQueueFactory* morsel_queue_factory() { return _morsel_queue_factory; }
    void set_morsel_queue_factory(MorselQueueFactory* morsel_queue_factory) {
        _morsel_queue_factory = morsel_queue_factory;
    }
    size_t num_total_original_morsels() const { return _morsel_queue_factory->num_original_morsels(); }

    // When the pipeline of this source operator wants to insert a local shuffle for some complex operators,
    // such as hash join and aggregate, use this method to decide whether really need to insert a local shuffle.
    // There are two source operators returning false.
    // - The scan operator, which has been assigned tablets with the specific bucket sequences.
    // - The exchange source operator, partitioned by HASH_PARTITIONED or BUCKET_SHUFFLE_HASH_PARTITIONED.
    virtual bool could_local_shuffle() const { return _could_local_shuffle; }
    void set_could_local_shuffle(bool could_local_shuffle) { _could_local_shuffle = could_local_shuffle; }
    virtual TPartitionType::type partition_type() const { return _partition_type; }
    void set_partition_type(TPartitionType::type partition_type) { _partition_type = partition_type; }
    virtual const std::vector<ExprContext*>& partition_exprs() const { return _partition_exprs; }
    void set_partition_exprs(const std::vector<ExprContext*>& partition_exprs) { _partition_exprs = partition_exprs; }

protected:
    size_t _degree_of_parallelism = 1;
    bool _could_local_shuffle = true;
    TPartitionType::type _partition_type = TPartitionType::type::HASH_PARTITIONED;
    MorselQueueFactory* _morsel_queue_factory = nullptr;
    std::vector<ExprContext*> _partition_exprs;
};

class SourceOperator : public Operator {
public:
    SourceOperator(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id,
                   int32_t driver_sequence)
            : Operator(factory, id, name, plan_node_id, driver_sequence) {}
    ~SourceOperator() override = default;

    bool need_input() const override { return false; }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override {
        return Status::InternalError("Shouldn't push chunk to source operator");
    }

    virtual void add_morsel_queue(MorselQueue* morsel_queue) { _morsel_queue = morsel_queue; };
    const MorselQueue* morsel_queue() const { return _morsel_queue; }

    size_t degree_of_parallelism() const { return _source_factory()->degree_of_parallelism(); }

protected:
    const SourceOperatorFactory* _source_factory() const { return down_cast<const SourceOperatorFactory*>(_factory); }

    MorselQueue* _morsel_queue = nullptr;
};

} // namespace pipeline
} // namespace starrocks
