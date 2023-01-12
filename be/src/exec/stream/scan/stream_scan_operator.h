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

#include "common/logging.h"
#include "connector/connector.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/workgroup/work_group_fwd.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

class StreamScanOperatorFactory final : public ConnectorScanOperatorFactory {
public:
    StreamScanOperatorFactory(int32_t id, ScanNode* scan_node, size_t dop, ChunkBufferLimiterPtr buffer_limiter,
                              bool is_stream_pipeline);

    ~StreamScanOperatorFactory() override = default;

    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;

private:
    bool _is_stream_pipeline;
};

class StreamScanOperator final : public ConnectorScanOperator {
public:
    StreamScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop, ScanNode* scan_node,
                       bool is_stream_pipeline);

    ~StreamScanOperator() override = default;

    bool is_finished() const override;
    bool has_output() const override;
    Status reset_epoch(RuntimeState* state) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    bool is_epoch_finished() const override { return _is_epoch_finished; }

    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

private:
    StatusOr<ChunkPtr> _mark_mock_data_finished();

    std::atomic<int32_t> _chunk_num{0};
    bool _is_stream_pipeline{false};
    bool _is_epoch_finished{true};
    int64_t _run_time = 0;
};

class StreamChunkSource : public ConnectorChunkSource {
public:
    StreamChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel, ScanOperator* op,
                      ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer);
};

} // namespace starrocks::pipeline
