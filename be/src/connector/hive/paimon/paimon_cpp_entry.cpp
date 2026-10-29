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

// Entry points of libstarrocks_paimon.so. Compiled only into the shim, never
// into starrocks_be.

#include "connector/hive/paimon/paimon_chunk_sink.h"
#include "connector/hive/paimon/paimon_cpp_shim_api.h"
#include "connector/hive/paimon/paimon_scanner.h"

extern "C" starrocks::HdfsScanner* starrocks_paimon_create_scanner() {
    return new starrocks::PaimonScanner();
}

extern "C" starrocks::connector::ConnectorSink* starrocks_paimon_create_sink_v1(
        std::shared_ptr<starrocks::connector::PaimonChunkSinkContext> context, int32_t driver_id) {
    return new starrocks::connector::PaimonChunkSink(std::move(context), driver_id);
}

extern "C" starrocks::StatusOr<std::vector<int32_t>> starrocks_paimon_bucket_ids_v1(
        const starrocks::TPaimonTableSink& sink, const starrocks::ChunkPtr& chunk,
        const std::vector<starrocks::ExprContext*>& output_exprs) {
    return starrocks::calculate_paimon_bucket_ids_in_shim(sink, chunk, output_exprs);
}
