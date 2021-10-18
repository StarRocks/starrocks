// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_sink_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline.h"

namespace starrocks {
class ExecNode;
class MemTracker;
namespace pipeline {

class PipelineBuilderContext {
public:
    PipelineBuilderContext(FragmentContext* fragment_context, size_t degree_of_parallelism)
            : _fragment_context(fragment_context), _degree_of_parallelism(degree_of_parallelism) {}

    void add_pipeline(const OpFactories& operators) {
        _pipelines.emplace_back(std::make_unique<Pipeline>(next_pipe_id(), operators));
    }

    OpFactories maybe_interpolate_local_exchange(OpFactories& pred_operators);

    uint32_t next_pipe_id() { return _next_pipeline_id++; }

    uint32_t next_operator_id() { return _next_operator_id++; }

    size_t degree_of_parallelism() const { return _degree_of_parallelism; }

    Pipelines get_pipelines() const { return _pipelines; }

    FragmentContext* fragment_context() { return _fragment_context; }

private:
    FragmentContext* _fragment_context;
    Pipelines _pipelines;
    uint32_t _next_pipeline_id = 0;
    uint32_t _next_operator_id = 0;
    size_t _degree_of_parallelism = 1;
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
