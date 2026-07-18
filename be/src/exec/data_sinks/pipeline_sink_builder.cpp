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

#include "exec/data_sinks/pipeline_sink_builder.h"

#include <utility>

#include "exec/pipeline/fragment_execution_params.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "fmt/format.h"
#include "gen_cpp/DataSinks_types.h"

namespace starrocks {

namespace {

std::string sink_type_name(TDataSinkType::type type) {
    auto it = _TDataSinkType_VALUES_TO_NAMES.find(type);
    return it == _TDataSinkType_VALUES_TO_NAMES.end() ? fmt::format("unknown data sink type {}", static_cast<int>(type))
                                                      : it->second;
}

} // namespace

Status PipelineSinkBuilder::build(pipeline::PipelineBuilderContext* context, pipeline::OpFactories upstream,
                                  const pipeline::UnifiedExecPlanFragmentParams& request,
                                  const RowDescriptor& row_desc) {
    const TDataSink& thrift_sink = request.output_sink();

    switch (thrift_sink.type) {
    case TDataSinkType::NOOP_SINK:
        upstream.emplace_back(std::make_shared<pipeline::NoopSinkOperatorFactory>(context->next_operator_id(),
                                                                                  upstream.back()->plan_node_id()));
        context->add_pipeline(upstream);
        return Status::OK();
    case TDataSinkType::SCHEMA_TABLE_SINK:
        return Status::NotSupported("SCHEMA_TABLE_SINK is not supported by the pipeline engine");
    case TDataSinkType::DATA_SPLIT_SINK:
        return Status::NotSupported("DATA_SPLIT_SINK is not implemented");
    default:
        return Status::InternalError(fmt::format("{} is not implemented", sink_type_name(thrift_sink.type)));
    }
}

} // namespace starrocks
