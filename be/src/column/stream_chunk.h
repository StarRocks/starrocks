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

#include "column/chunk.h"
#include "column/chunk_extra_data.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"

namespace starrocks {

using Int8ColumnPtr = Int8Column::Ptr;

using StreamChunk = Chunk;
using StreamChunkPtr = std::shared_ptr<StreamChunk>;

struct EpochInfo;
using EpochInfoPtr = std::shared_ptr<EpochInfo>;

/**
 * `StreamRowOp` represents a row's operation kind used in Incremental Materialized View.
 * 
 * `INSERT`                         : Add a new row.
 * `DELETE`                         : Delete an existed row.
 * `UPDATE_BEFORE`/`UPDATE_AFTER`   : Represents previous and postvious detail of `UPDATE`
 *                                    which always come in pair and next to each other.
 */
enum StreamRowOp : std::int8_t { OP_INSERT = 0, OP_DELETE = 1, OP_UPDATE_BEFORE = 2, OP_UPDATE_AFTER = 3 };

/**
 * Epoch trigger mode represents a different kind of incremental source consume method:
 * - `PROCESSTIME_OFFSET`   :  `Source` consumes the max offsets or process time in this epoch 
 *                              which wins who comes first. This is the method by default.
 * - `OFFSET`               :  `Source` consumes the max offsets in this epoch.
 * - `PROCESSTIME`          :  `Source` consumes the max process time in this epoch.
 * - `MANUAL`               :  `Source` consumes the max offsets in this epoch.
 */
enum TriggerMode { PROCESSTIME_OFFSET = 0, OFFSET = 1, PROCESSTIME = 2, MANUAL = 3 };

struct BinlogOffset {
    int64_t tablet_id;
    int64_t tablet_version;
    int64_t lsn;
};

class TMVMaintenanceTasks;
class TMVStartEpochTask;

/**
 *  `MVMaintenanceTaskInfo` contains the basic MV maintenance tasks info for all task types.
 */
struct MVMaintenanceTaskInfo {
    int64_t signature;
    std::string db_name;
    std::string mv_name;
    int64_t db_id;
    int64_t mv_id;
    int64_t job_id;
    int64_t task_id;
    TUniqueId query_id;

    static MVMaintenanceTaskInfo from_maintenance_task(const TMVMaintenanceTasks& maintenance_task);
};

/**
 * Epoch is an unit of an incremental compute. At the beginning of each incremental compute,
 * an `EpochInfo` will be triggered for each source operator, then the source operator will
 * consume the binlog offsets as the `EpochInfo`'s description. At the end, the source operator
 * enters into `epoch_finished` state and passes through to the next, until to the last sink
 * operator, the epoch is computed done at last.
 */
struct EpochInfo {
    // transaction id
    int64_t txn_id;
    // load_id
    TUniqueId load_id;
    // epoch marker id
    int64_t epoch_id;
    // max binlog duration which this epoch will run
    int64_t max_exec_millis;
    // max binlog offset which this epoch will run
    int64_t max_scan_rows;
    // Trigger mode
    TriggerMode trigger_mode = PROCESSTIME_OFFSET;

    static EpochInfo from_start_epoch_task(const TMVStartEpochTask& start_epoch);

    std::string debug_string() const;
};

/**
 * `StreamChunk` is used in Incremental MV which contains a hidden `ops` column, the `ops` column indicates
 * the row's operation kind, eg: INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER.
 * 
 * `StreamChunkConverter` is used as a converter between the common `Chunk` and the `StreamChunk`.
 */
class StreamChunkConverter {
public:
    static StreamChunkPtr make_stream_chunk(ChunkPtr chunk, Int8ColumnPtr ops);

    static bool has_ops_column(const StreamChunk& chunk);
    static bool has_ops_column(const StreamChunkPtr& chunk_ptr);
    static bool has_ops_column(const StreamChunk* chunk_ptr);

    static Int8Column* ops_col(const StreamChunk& stream_chunk);
    static Int8Column* ops_col(const StreamChunkPtr& stream_chunk_ptr);
    static Int8Column* ops_col(const StreamChunk* stream_chunk_ptr);

    static const StreamRowOp* ops(const StreamChunk& stream_chunk);
    static const StreamRowOp* ops(const StreamChunk* stream_chunk);
    static const StreamRowOp* ops(const StreamChunkPtr& stream_chunk);

    static ChunkPtr to_chunk(const StreamChunkPtr& stream_chunk);
};

} // namespace starrocks