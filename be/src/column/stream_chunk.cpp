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

#include "column/stream_chunk.h"

#include "gen_cpp/MVMaintenance_types.h"

namespace starrocks {

MVMaintenanceTaskInfo MVMaintenanceTaskInfo::from_maintenance_task(const TMVMaintenanceTasks& maintenance_task) {
    MVMaintenanceTaskInfo res;
    res.signature = maintenance_task.signature;
    res.db_name = maintenance_task.db_name;
    res.mv_name = maintenance_task.mv_name;
    res.db_id = maintenance_task.db_id;
    res.mv_id = maintenance_task.mv_id;
    res.job_id = maintenance_task.job_id;
    res.task_id = maintenance_task.task_id;
    res.query_id = maintenance_task.query_id;
    return res;
}

EpochInfo EpochInfo::from_start_epoch_task(const TMVStartEpochTask& start_epoch) {
    EpochInfo res;
    res.epoch_id = start_epoch.epoch.epoch_id;
    res.txn_id = start_epoch.epoch.txn_id;
    res.max_exec_millis = start_epoch.max_exec_millis;
    res.max_scan_rows = start_epoch.max_scan_rows;
    return res;
}

std::string EpochInfo::debug_string() const {
    std::stringstream ss;
    ss << "epoch_id=" << epoch_id << ", txn_id=" << txn_id << ", max_exec_millis=" << max_exec_millis
       << ", max_scan_rows=" << max_scan_rows << ", trigger_mode=" << (int)(trigger_mode);
    return ss.str();
}

StreamChunkPtr StreamChunkConverter::make_stream_chunk(ChunkPtr chunk, const Int8ColumnPtr& ops) {
    std::vector<ChunkExtraColumnsMeta> stream_extra_data_meta = {
            ChunkExtraColumnsMeta{.type = TypeDescriptor(TYPE_TINYINT), .is_null = false, .is_const = false}};
    std::vector<ColumnPtr> stream_extra_data = {ops};
    auto extra_data =
            std::make_shared<ChunkExtraColumnsData>(std::move(stream_extra_data_meta), std::move(stream_extra_data));
    chunk->set_extra_data(std::move(extra_data));
    return chunk;
}

bool StreamChunkConverter::has_ops_column(const StreamChunk& chunk) {
    if (chunk.has_extra_data() && down_cast<ChunkExtraColumnsData*>(chunk.get_extra_data().get())) {
        return true;
    }
    return false;
}

bool StreamChunkConverter::has_ops_column(const StreamChunkPtr& chunk_ptr) {
    if (!chunk_ptr) {
        return false;
    }
    return has_ops_column(*chunk_ptr);
}

bool StreamChunkConverter::has_ops_column(const StreamChunk* chunk_ptr) {
    if (!chunk_ptr) {
        return false;
    }
    return has_ops_column(*chunk_ptr);
}

Int8Column* StreamChunkConverter::ops_col(const StreamChunk& stream_chunk) {
    DCHECK(has_ops_column(stream_chunk));
    auto extra_column_data = down_cast<ChunkExtraColumnsData*>(stream_chunk.get_extra_data().get());
    DCHECK(extra_column_data);
    DCHECK_EQ(extra_column_data->columns().size(), 1);
    auto* op_col = ColumnHelper::as_raw_column<Int8Column>(extra_column_data->columns()[0]);
    DCHECK(op_col);
    DCHECK_EQ(stream_chunk.num_rows(), op_col->size());
    return op_col;
}

Int8Column* StreamChunkConverter::ops_col(const StreamChunkPtr& stream_chunk_ptr) {
    DCHECK(stream_chunk_ptr);
    return ops_col(*stream_chunk_ptr);
}

Int8Column* StreamChunkConverter::ops_col(const StreamChunk* stream_chunk_ptr) {
    DCHECK(stream_chunk_ptr);
    return ops_col(*stream_chunk_ptr);
}

const StreamRowOp* StreamChunkConverter::ops(const StreamChunk& stream_chunk) {
    auto* op_col = ops_col(stream_chunk);
    return (StreamRowOp*)(op_col->get_data().data());
}

const StreamRowOp* StreamChunkConverter::ops(const StreamChunk* stream_chunk) {
    auto* op_col = ops_col(stream_chunk);
    return (StreamRowOp*)(op_col->get_data().data());
}

const StreamRowOp* StreamChunkConverter::ops(const StreamChunkPtr& stream_chunk) {
    auto* op_col = ops_col(stream_chunk);
    return (StreamRowOp*)(op_col->get_data().data());
}

ChunkPtr StreamChunkConverter::to_chunk(const StreamChunkPtr& stream_chunk) {
    if (has_ops_column(stream_chunk)) {
        auto extra_column_data = down_cast<ChunkExtraColumnsData*>(stream_chunk->get_extra_data().get());
        DCHECK(extra_column_data);
        auto mock_slot_id = stream_chunk->num_columns();
        for (auto& extra_column : extra_column_data->columns()) {
            stream_chunk->append_column(extra_column, mock_slot_id++);
        }
        stream_chunk->set_extra_data(nullptr);
    }
    return stream_chunk;
}

} // namespace starrocks