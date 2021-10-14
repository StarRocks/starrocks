// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "exec/pipeline/operator_with_dependency.h"

#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks {
namespace pipeline {

OperatorWithDependency::OperatorWithDependency(int32_t id, const std::string& name, int32_t plan_node_id)
        : Operator(id, name, plan_node_id) {}

OperatorWithDependencyFactory::OperatorWithDependencyFactory(int32_t id, const std::string& name, int32_t plan_node_id)
        : OperatorFactory(id, name, plan_node_id) {}
} // namespace pipeline
} // namespace starrocks