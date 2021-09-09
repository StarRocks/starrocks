// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_builder.h"

#include "exec/exec_node.h"

namespace starrocks::pipeline {
Pipelines PipelineBuilder::build(const FragmentContext& fragment, ExecNode* exec_node) {
    pipeline::OpFactories operators = exec_node->decompose_to_pipeline(&_context);
    _context.add_pipeline(std::move(operators));
    return _context.get_pipelines();
}
} // namespace starrocks::pipeline