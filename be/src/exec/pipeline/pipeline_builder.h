// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline.h"

namespace starrocks {
class ExecNode;
class MemTracker;
namespace pipeline {

class PipelineBuilderContext {
public:
    PipelineBuilderContext(const FragmentContext& fragment_context, uint32_t driver_instance_count)
            : _fragment_context(fragment_context), _driver_instance_count(driver_instance_count) {}

    void add_pipeline(const OpFactories& operators) {
        _pipelines.emplace_back(std::make_unique<Pipeline>(next_pipe_id(), _driver_instance_count, operators));
    }

    uint32_t next_pipe_id() { return _next_pipeline_id++; }

    uint32_t next_operator_id() { return _next_operator_id++; }

    uint32_t driver_instance_count() const { return _driver_instance_count; }

    void set_driver_instance_count(uint32_t driver_instance_count) { _driver_instance_count = driver_instance_count; }

    Pipelines get_pipelines() const { return _pipelines; }

private:
    const FragmentContext& _fragment_context;
    Pipelines _pipelines;
    uint32_t _next_pipeline_id = 0;
    uint32_t _next_operator_id = 0;
    uint32_t _driver_instance_count = 1;
};

class PipelineBuilder {
public:
    PipelineBuilder(PipelineBuilderContext& context) : _context(context) {}

    // Build pipeline from exec node tree
    Pipelines build(const FragmentContext& fragment, ExecNode* exec_node);

private:
    PipelineBuilderContext& _context;
};
} // namespace pipeline
} // namespace starrocks