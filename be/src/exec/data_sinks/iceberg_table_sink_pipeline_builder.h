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

#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec_primitive/pipeline/pipeline_fwd.h"
#include "gen_cpp/Exprs_types.h"

namespace starrocks {

class IcebergTableSink;
class TDataSink;
class TIcebergTableSink;

namespace connector {
struct IcebergChunkSinkContext;
struct IcebergDeleteSinkContext;
} // namespace connector

// Translate the FE's encryption signal onto a sink context. Declared here rather than kept file-local
// so they can be tested directly: the builder class itself lives entirely in the .cpp, and reaching
// these through create_*_sink_context would need a RuntimeState and a PipelineBuilderContext.
//
// Both refuse rather than default a missing DEK length: choosing one could write a key weaker than the
// table's encryption.data-key-length policy and commit it with nothing surfaced.
Status apply_iceberg_encryption_signal(const TIcebergTableSink& t_iceberg_sink,
                                       connector::IcebergChunkSinkContext* ctx);

// Same signal for the position-delete sinks, used by both DELETE and UPDATE/MERGE.
// IcebergDeleteSinkContext is a sibling of IcebergChunkSinkContext rather than a subclass, and its
// writer factory consumes ctx->options directly, so the keys go into that map instead of fields.
Status apply_iceberg_encryption_signal_to_delete_ctx(const TIcebergTableSink& t_iceberg_sink,
                                                     connector::IcebergDeleteSinkContext* ctx);

Status decompose_iceberg_table_sink_to_pipeline(const IcebergTableSink& sink, pipeline::OpFactories prev_operators,
                                                const TDataSink& thrift_sink,
                                                pipeline::PipelineBuilderContext* context);

Status update_iceberg_partition_expr_slot_refs_by_map(std::vector<TExpr>& partition_expr,
                                                      const std::unordered_map<std::string, TExprNode>& column_slot_map,
                                                      const std::vector<std::string>& partition_source_column_names);

} // namespace starrocks
