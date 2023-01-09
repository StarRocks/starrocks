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

#include "stream_scan_operator.h"

#include "exec/connector_scan_node.h"

namespace starrocks::pipeline {

StreamScanOperatorFactory::StreamScanOperatorFactory(int32_t id, ScanNode* scan_node, size_t dop,
                                                     ChunkBufferLimiterPtr buffer_limiter)
        : ConnectorScanOperatorFactory(id, scan_node, dop, std::move(buffer_limiter)) {}

OperatorPtr StreamScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<StreamScanOperator>(this, _id, driver_sequence, dop, _scan_node);
}

StreamScanOperator::StreamScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                                       ScanNode* scan_node)
        : ConnectorScanOperator(factory, id, driver_sequence, dop, scan_node) {}

ChunkSourcePtr StreamScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    auto* scan_node = down_cast<ConnectorScanNode*>(_scan_node);
    auto* factory = down_cast<StreamScanOperatorFactory*>(_factory);
    return std::make_shared<StreamChunkSource>(_driver_sequence, _chunk_source_profiles[chunk_source_index].get(),
                                               std::move(morsel), this, scan_node, factory->get_chunk_buffer());
}

StreamChunkSource::StreamChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                                     ScanOperator* op, ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer)
        : ConnectorChunkSource(scan_operator_id, runtime_profile, std::move(morsel), op, scan_node, chunk_buffer) {}

} // namespace starrocks::pipeline
