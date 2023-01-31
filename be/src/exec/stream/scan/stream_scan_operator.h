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
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/workgroup/work_group_fwd.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

class FragmentContext;

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

    ~StreamScanOperator() override;
    Status set_epoch_finishing(RuntimeState* state) override {
        std::lock_guard guard(_task_mutex);
        _detach_chunk_sources();
        _is_epoch_finished = true;
        return Status::OK();
    }

    Status set_epoch_finished(RuntimeState* state) override;

    Status do_prepare(RuntimeState* state) override {
        ConnectorScanOperator::do_prepare(state);
        _stream_epoch_manager = state->query_ctx()->stream_epoch_manager();
        DCHECK(_stream_epoch_manager);
        return Status::OK();
    }

    Status _pickup_morsel(RuntimeState* state, int chunk_source_index) override;

    Status reset_epoch(RuntimeState* runtime_state) override;
    bool is_finished() const override;
    bool has_output() const override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    bool is_epoch_finished() const override;

    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

private:
    StatusOr<ChunkPtr> _mark_mock_data_finished();
    void _reset_chunk_source(RuntimeState* state, int chunk_source_index);

    std::atomic<int32_t> _chunk_num{0};
    bool _is_epoch_start{false};
    int64_t _run_time = 0;

    StreamEpochManager* _stream_epoch_manager;
    starrocks::EpochInfo _current_epoch_info;
};

class StreamChunkSource : public ConnectorChunkSource {
public:
    StreamChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel, ScanOperator* op,
                      ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer);
};

} // namespace starrocks::pipeline