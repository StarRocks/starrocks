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
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"

namespace starrocks {

class ExprContext;
class HdfsScanner;
class TPaimonTableSink;

namespace connector {
class ConnectorSink;
struct PaimonChunkSinkContext;
} // namespace connector

// Creates a PaimonScanner through the lazily-dlopen()ed paimon-cpp shim
// (libstarrocks_paimon.so). The shim and its paimon libraries are not linked
// into starrocks_be, so the BE starts without them; the first paimon-native
// scan triggers the load, and a missing or mismatched shim fails that query
// with an actionable error instead of failing BE startup.
StatusOr<HdfsScanner*> create_paimon_cpp_scanner();

// Always returns a sink object; errors loading the optional writer surface from its init().
std::unique_ptr<connector::ConnectorSink> create_paimon_cpp_chunk_sink(
        std::shared_ptr<connector::PaimonChunkSinkContext> context, int32_t driver_id);

// Used by the pre-exchange routing operator. The shim performs conversion and native hashing;
// neither bundled Arrow objects nor SDK objects cross the dynamic-library boundary.
// output_exprs describe every native field, in the frozen column_names order.
StatusOr<std::vector<int32_t>> calculate_paimon_cpp_bucket_ids(const TPaimonTableSink& sink, const ChunkPtr& chunk,
                                                               const std::vector<ExprContext*>& output_exprs);

} // namespace starrocks
