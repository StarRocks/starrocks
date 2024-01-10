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

#include "exec/pipeline/adaptive/adaptive_fwd.h"
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
    void adjust_max_dop(size_t new_dop) { _degree_of_parallelism = std::min(new_dop, _degree_of_parallelism); }
    virtual void adjust_dop() {}
    size_t degree_of_parallelism() const { return _degree_of_parallelism; }

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
    // This information comes from the hint of the sql, like `sum(v1) over ([skewed] partition by v2 order by v3)`
    // and if this pipeline is connected by a full sort node and it can have better performance with a uniformed distribution.
    // So we will use this method to determine whether a random local exchange should be introduced.
    bool is_skewed() const { return _is_skewed; }
    void set_skewed(bool is_skewed) { _is_skewed = is_skewed; }
    virtual TPartitionType::type partition_type() const { return _partition_type; }
    void set_partition_type(TPartitionType::type partition_type) { _partition_type = partition_type; }
    virtual const std::vector<ExprContext*>& partition_exprs() const { return _partition_exprs; }
    void set_partition_exprs(const std::vector<ExprContext*>& partition_exprs) { _partition_exprs = partition_exprs; }

    /// The pipelines of a fragment instance are organized by groups.
    /// - The operator tree is broken into several groups by CollectStatsSourceOperator (CsSource)
    ///   and the operators with multiple children, such as HashJoin, NLJoin, Except, and Intersect.
    /// - The group leader is the source operator of the most downstream pipeline in the group,
    ///   including CsSource, Scan, and Exchange.
    /// - The adaptive_initial_state of group leader is ACTIVE or INACTIVE,
    ///   and that of the other pipelines is NONE.
    ///
    /// Dependent relations between groups.
    /// - As for the operator who depends on other operators, e.g. HashJoinProbe depends on HashJoinBuild,
    ///   the group of this operator also depends on the group of the dependent operators.
    ///
    /// A group cannot instantiate drivers until three conditions satisfy:
    /// 1. The adaptive_initial_state of leader source operator is READY.
    /// 2. The adaptive_initial_state of dependent pipelines is READY.
    /// 3. All the drivers of dependent pipelines are finished.
    ///
    /// For example, the following fragment contains three pipeline groups: [pipe#1], [pipe#2, pipe#3], [pipe#4],
    /// and [pipe#2, pipe#3] depends on [pipe#4].
    ///          ExchangeSink
    ///               |
    ///          HashJoinProbe                       HashJoinBuild
    ///  pipe#3       |                                    |                  pipe#4
    ///          BlockingAggSource(NONE)          ExchangeSource(ACTIVE)
    ///
    ///          BlockingAggSink
    ///  pipe#2       |
    ///          CsSource(INACTIVE)
    ///
    ///          CsSink
    ///  pipe#1     |
    ///          ScanNode(ACTIVE)
    enum class AdaptiveState : uint8_t { ACTIVE, INACTIVE, NONE };
    virtual AdaptiveState adaptive_initial_state() const { return AdaptiveState::NONE; }
    bool is_adaptive_group_initial_active() const;

    EventPtr adaptive_blocking_event() const { return _adaptive_blocking_event; }
    void set_adaptive_blocking_event(EventPtr event) { _adaptive_blocking_event = std::move(event); }
    void set_group_initialize_event(EventPtr event) { _group_initialize_event = std::move(event); }

    void add_group_dependent_pipeline(const Pipeline* dependent_op);
    const std::vector<const Pipeline*>& group_dependent_pipelines() const;

    void set_group_leader(SourceOperatorFactory* parent);
    SourceOperatorFactory* group_leader() const;

protected:
    size_t _degree_of_parallelism = 1;
    bool _could_local_shuffle = true;
    bool _is_skewed = false;
    TPartitionType::type _partition_type = TPartitionType::type::HASH_PARTITIONED;
    MorselQueueFactory* _morsel_queue_factory = nullptr;

    std::vector<ExprContext*> _partition_exprs;

    SourceOperatorFactory* _group_leader = this;
    std::vector<const Pipeline*> _group_dependent_pipelines;
    EventPtr _group_initialize_event = nullptr;
    EventPtr _adaptive_blocking_event = nullptr;
};

class SourceOperator : public Operator {
public:
    SourceOperator(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id,
                   bool is_subordinate, int32_t driver_sequence)
            : Operator(factory, id, name, plan_node_id, is_subordinate, driver_sequence) {}
    ~SourceOperator() override = default;

    bool need_input() const override { return false; }

    // Return true if the output of `has_output` shift frequently between true and false.
    // We need to avoid this kind of mutable pipeline being moved frequently between ready queue and pending queue,
    // which will lead to drastic performance deduction (the "ScheduleTime" in profile will be super high).
    virtual bool is_mutable() const { return false; }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override {
        return Status::InternalError("Shouldn't push chunk to source operator");
    }

    virtual void add_morsel_queue(MorselQueue* morsel_queue) { _morsel_queue = morsel_queue; };
    const MorselQueue* morsel_queue() const { return _morsel_queue; }

    size_t degree_of_parallelism() const { return _source_factory()->degree_of_parallelism(); }
    const std::vector<const Pipeline*>& group_dependent_pipelines() const {
        return _source_factory()->group_dependent_pipelines();
    }

protected:
    const SourceOperatorFactory* _source_factory() const { return down_cast<const SourceOperatorFactory*>(_factory); }

    MorselQueue* _morsel_queue = nullptr;
};

} // namespace pipeline
} // namespace starrocks
