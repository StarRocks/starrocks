// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once
#include <string>
#include <vector>

#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks {
namespace pipeline {
// OperatorWithDependency is used to decompose multi-input ExecNode, such as HashJoinNode, CrossJoinNode and etc.
// In multi-input ExecNode, right child is evaluated in ExecNode::open() at first, then chunks are pulled from
// left child one by one in ExecNode::get_next(). Every multi-input ExecNode(e.g. HashJoinNode) is decomposed into
// two operators (HashJoinProbeOperator and HashJoinBuildOperator) for both sides. The operators corresponding
// left side and right side are denoted as left operator(HashJoinProbeOperator) and right operator(HashJoinBuildOperator)
// respectively. The right operator(HashJoinBuildOperator) should be full materialized, so it is used as the SinkOperator
// of a pipeline, The left operator(HashJoinProbeOperator) can work in streaming-style, so it can appear in the middle
// of another pipeline. Denotes pipeline contains left operator and right operator as left pipeline and right pipeline
// respectively, so left pipeline always has a data dependency on right pipeline, left pipeline can be scheduled to
// execute on core until right pipeline has finished. For a left pipeline, it shall get stuck in left operator.
// OperatorWithDependency is introduced so that left operators should inherit from it. When a left pipeline get stuck
// in left operators, it should be guarded by PipelineDriverPoller thread that will inspect std::vector<OperatorWithDependency*>
// PipelineDriver::_dependencies to ensure that left pipeline is put back into multi-level feedback queue after all
// right operators have been ready, i.e. OperatorWithDependency::is_ready() returns true.
class OperatorWithDependency;
using DriverDependencyPtr = OperatorWithDependency*;
using DriverDependencies = std::vector<DriverDependencyPtr>;

class OperatorWithDependency : public Operator {
public:
    OperatorWithDependency(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id,
                           int32_t driver_sequence)
            : Operator(factory, id, name, plan_node_id, driver_sequence) {}
    ~OperatorWithDependency() = default;
    // return true if the corresponding right operator is full materialized, otherwise return false.
    virtual bool is_ready() const = 0;
};

class OperatorWithDependencyFactory : public OperatorFactory {
public:
    OperatorWithDependencyFactory(int32_t id, const std::string& name, int32_t plan_node_id)
            : OperatorFactory(id, name, plan_node_id) {}

    ~OperatorWithDependencyFactory() = default;
};

} // namespace pipeline
} // namespace starrocks
