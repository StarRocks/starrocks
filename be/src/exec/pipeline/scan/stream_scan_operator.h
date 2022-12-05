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
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/workgroup/work_group_fwd.h"
#include "storage/chunk_helper.h"

namespace starrocks {

class ScanNode;

namespace pipeline {

struct EpochInfo {
    int64_t max_batch_rows;
    int64_t max_time;

    int64_t table_version;
    // tablet_id -> change_log_id
    std::map<int64_t, int64_t> binlog_offset;

    int64_t get_start_offset(int64 tablet_id) {
        // the key must exists
        return binlog_offset[tablet_id];
    }
};

struct CommitOffset {
    int64_t tablet_id;
    int64_t next_version;
    int64_t change_log_id;
    CommitOffset(int64 id, int64_t version, int64_t log_id) : tablet_id(id), next_version(version), change_log_id(log_id){};
};

class StreamScanOperator final : public ConnectorScanOperator {
public:
    StreamScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                          ScanNode* scan_node);

    ~StreamScanOperator() override = default;

    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

    // Start/End epoch implement
    bool is_epoch_finished();
    bool is_finished() const override;

    bool has_output() const override;

    void start_epoch(const EpochInfo& epoch);
    CommitOffset get_latest_offset();

    Status _pickup_morsel(RuntimeState* state, int chunk_source_index) override;

protected:
    std::atomic_bool _is_epoch_finished{true};
    std::atomic_bool _is_finished{false};
    bool _is_epoch_start;
    EpochInfo _current_epoch;

    std::queue<ChunkSourcePtr> _old_chunk_sources;
    std::queue<ChunkSourcePtr> _closed_chunk_sources;

};

} // namespace pipeline
} // namespace starrocks
