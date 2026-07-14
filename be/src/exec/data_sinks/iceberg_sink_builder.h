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
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec_primitive/pipeline/pipeline_fwd.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Exprs_types.h"

namespace starrocks {

class IcebergTableDescriptor;
class RuntimeState;
struct TExprNode;

namespace connector {
class ConnectorSinkProvider;
}

class IcebergSinkBuilder {
public:
    explicit IcebergSinkBuilder(const std::vector<TExpr>& output_exprs) : _output_exprs(output_exprs) {}

    Status build(pipeline::OpFactories prev_operators, const TDataSink& thrift_sink,
                 pipeline::PipelineBuilderContext* context) const;

private:
    // Helper function to update slot references using column_slot_map
    Status update_partition_expr_slot_refs_by_map(std::vector<TExpr>& partition_expr,
                                                  const std::unordered_map<std::string, TExprNode>& column_slot_map,
                                                  const std::vector<std::string>& partition_source_column_names) const;

    // Helper functions to create sink contexts
    Status create_delete_sink_context(const TDataSink& thrift_sink, RuntimeState* runtime_state,
                                      pipeline::PipelineBuilderContext* context,
                                      IcebergTableDescriptor* iceberg_table_desc,
                                      std::unique_ptr<connector::ConnectorSinkProvider>& sink_provider,
                                      std::vector<TExpr>& partition_expr) const;

    Status create_data_sink_context(const TDataSink& thrift_sink, RuntimeState* runtime_state,
                                    pipeline::PipelineBuilderContext* context,
                                    IcebergTableDescriptor* iceberg_table_desc,
                                    std::unique_ptr<connector::ConnectorSinkProvider>& sink_provider,
                                    std::vector<TExpr>& partition_expr) const;

    Status create_row_delta_sink_context(const TDataSink& thrift_sink, RuntimeState* runtime_state,
                                         pipeline::PipelineBuilderContext* context,
                                         IcebergTableDescriptor* iceberg_table_desc,
                                         std::unique_ptr<connector::ConnectorSinkProvider>& sink_provider,
                                         std::vector<TExpr>& partition_expr) const;

    const std::vector<TExpr>& _output_exprs;
};

} // namespace starrocks
