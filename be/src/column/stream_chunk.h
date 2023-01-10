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

class TMVStartEpochTask;
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
    // epoch marker id
    int64_t epoch_id;
    // max binlog duration which this epoch will run
    int64_t max_exec_millis;
    // max binlog offset which this epoch will run
    int64_t max_scan_rows;
    // Trigger mode
    TriggerMode trigger_mode = PROCESSTIME_OFFSET;

    static EpochInfo from_start_epoch_task(const TMVStartEpochTask& start_epoch);

    std::string debug_string() const {
        std::stringstream ss;
        ss << "epoch_id=" << epoch_id << ", max_exec_millis=" << max_exec_millis << ", max_scan_rows=" << max_scan_rows
           << ", trigger_mode=" << (int)(trigger_mode);
        return ss.str();
    }
};

/**
 * `StreamChunk` is used in Incremental MV which contains a hidden `ops` column, the `ops` column indicates
 * the row's operation kind, eg: INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER.
 * 
 * `StreamChunkConverter` is used as a converter between the common `Chunk` and the `StreamChunk`.
 */
class StreamChunkConverter {
public:
    static StreamChunkPtr make_stream_chunk(ChunkPtr chunk, Int8ColumnPtr ops) {
        static std::vector<ChunkExtraColumnsMeta> stream_extra_data_meta = {
                ChunkExtraColumnsMeta{.type = TypeDescriptor(TYPE_TINYINT), .is_null = false, .is_const = false}};
        std::vector<ColumnPtr> stream_extra_data = {ops};
        auto extra_data = std::make_shared<ChunkExtraColumnsData>(std::move(stream_extra_data_meta),
                                                                  std::move(stream_extra_data));
        chunk->set_extra_data(std::move(extra_data));
        return chunk;
    }

    static bool has_ops_column(const StreamChunk& chunk) {
        if (chunk.has_extra_data() && typeid(*chunk.get_extra_data()) == typeid(ChunkExtraColumnsData)) {
            return true;
        }
        return false;
    }

    static bool has_ops_column(const StreamChunkPtr& chunk_ptr) {
        if (!chunk_ptr) {
            return false;
        }
        return has_ops_column(*chunk_ptr);
    }

    static bool has_ops_column(StreamChunk* chunk_ptr) {
        if (!chunk_ptr) {
            return false;
        }
        return has_ops_column(*chunk_ptr);
    }

    static Int8Column* ops_col(const StreamChunk& stream_chunk) {
        DCHECK(has_ops_column(stream_chunk));
        auto extra_column_data = down_cast<ChunkExtraColumnsData*>(stream_chunk.get_extra_data().get());
        DCHECK(extra_column_data);
        DCHECK_EQ(extra_column_data->columns().size(), 1);
        auto* op_col = ColumnHelper::as_raw_column<Int8Column>(extra_column_data->columns()[0]);
        DCHECK(op_col);
        DCHECK_EQ(stream_chunk.num_rows(), op_col->size());
        return op_col;
    }

    static Int8Column* ops_col(const StreamChunkPtr& stream_chunk_ptr) {
        DCHECK(stream_chunk_ptr);
        return ops_col(*stream_chunk_ptr);
    }

    static Int8Column* ops_col(StreamChunk* stream_chunk_ptr) {
        DCHECK(stream_chunk_ptr);
        return ops_col(*stream_chunk_ptr);
    }

    static const StreamRowOp* ops(const StreamChunk& stream_chunk) {
        auto* op_col = ops_col(stream_chunk);
        return (StreamRowOp*)(op_col->get_data().data());
    }

    static const StreamRowOp* ops(StreamChunk* stream_chunk) {
        auto* op_col = ops_col(stream_chunk);
        return (StreamRowOp*)(op_col->get_data().data());
    }

    static const StreamRowOp* ops(const StreamChunkPtr& stream_chunk) {
        auto* op_col = ops_col(stream_chunk);
        return (StreamRowOp*)(op_col->get_data().data());
    }
};

} // namespace starrocks
