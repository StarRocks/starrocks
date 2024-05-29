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
    StreamChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
<<<<<<< HEAD
                      ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer);
=======
                      ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer, bool enable_adaptive_io_task);

    [[nodiscard]] Status prepare(RuntimeState* state) override;

    [[nodiscard]] Status set_stream_offset(int64_t table_version, int64_t changelog_id);
    void set_epoch_limit(int64_t epoch_rows_limit, int64_t epoch_time_limit);
    Status reset_status();
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
>>>>>>> 5967988192 ([BugFix] Fix down_cast failed in adaptive io task (#46372))
};

} // namespace starrocks::pipeline
