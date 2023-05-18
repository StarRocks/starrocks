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

#include <queue>

#include "common/logging.h"
#include "connector/connector.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "exec/workgroup/work_group_fwd.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

class FragmentContext;
class StreamChunkSource;

class StreamScanOperatorFactory final : public ConnectorScanOperatorFactory {
public:
    StreamScanOperatorFactory(int32_t id, ScanNode* scan_node, RuntimeState* state, size_t dop,
                              ChunkBufferLimiterPtr buffer_limiter, bool is_stream_pipeline);

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

    void _close_chunk_source_unlocked(RuntimeState* state, int index) override;
    void _finish_chunk_source_task(RuntimeState* state, int chunk_source_index, int64_t cpu_time_ns, int64_t scan_rows,
                                   int64_t scan_bytes) override;

    bool is_epoch_finished() const override;

    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

private:
    StatusOr<ChunkPtr> _mark_mock_data_finished();
    void _reset_chunk_source(RuntimeState* state, int chunk_source_index);

    std::atomic<int32_t> _chunk_num{0};
    bool _is_epoch_start{false};
    bool _is_epoch_finished{true};
    bool _is_stream_pipeline{false};
    int64_t _run_time = 0;

    StreamEpochManager* _stream_epoch_manager = nullptr;
    starrocks::EpochInfo _current_epoch_info;

    std::queue<ChunkSourcePtr> _old_chunk_sources;
    std::queue<ChunkSourcePtr> _closed_chunk_sources;
};

class StreamChunkSource : public ConnectorChunkSource {
public:
    StreamChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                      ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer);

    Status prepare(RuntimeState* state) override;

    Status set_stream_offset(int64_t table_version, int64_t changelog_id);
    void set_epoch_limit(int64_t epoch_rows_limit, int64_t epoch_time_limit);
    void reset_status();
    int64_t get_lane_owner() {
        auto [lane_owner, version] = _morsel->get_lane_owner_and_version();
        return lane_owner;
    }

protected:
    bool _reach_eof() const override;

    connector::StreamDataSource* _get_stream_data_source() const {
        return down_cast<connector::StreamDataSource*>(_data_source.get());
    }

    int64_t _epoch_rows_limit = -1; // -1: not limit;
    int64_t _epoch_time_limit = -1; // -1: not limit;
};

} // namespace starrocks::pipeline