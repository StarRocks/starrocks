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

#include "exec/schema_scanner/schema_be_tablet_write_log_scanner.h"

#include "column/column_helper.h"
#include "column/datum.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaBeTabletWriteLogScanner::_s_tbls_columns[] = {
        {"be_id", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"begin_time", TypeDescriptor(TYPE_DATETIME), sizeof(int64_t), false},
        {"finish_time", TypeDescriptor(TYPE_DATETIME), sizeof(int64_t), false},
        {"txn_id", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"tablet_id", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"table_id", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"partition_id", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"log_type", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"input_rows", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"input_bytes", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"output_rows", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"output_bytes", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"input_segments", TypeDescriptor(TYPE_INT), sizeof(int32_t), true},
        {"output_segments", TypeDescriptor(TYPE_INT), sizeof(int32_t), false},
        {"label", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"compaction_score", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), true},
        {"compaction_type", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
};

SchemaBeTabletWriteLogScanner::SchemaBeTabletWriteLogScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaBeTabletWriteLogScanner::~SchemaBeTabletWriteLogScanner() = default;

Status SchemaBeTabletWriteLogScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    // Get all logs
    // TODO: Support predicate pushdown filtering
    _logs = lake::TabletWriteLogManager::instance()->get_logs();
    _log_index = 0;
    _ctz = state->timezone_obj();
    return Status::OK();
}

Status SchemaBeTabletWriteLogScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (chunk == nullptr || eos == nullptr) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_log_index >= _logs.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    *chunk = ChunkHelper::new_chunk(_slot_descs, _runtime_state->chunk_size());

    size_t i = 0;
    for (; i < _runtime_state->chunk_size() && _log_index < _logs.size(); ++i, ++_log_index) {
        const auto& log = _logs[_log_index];
        int col_idx = 0;

        // be_id
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.backend_id)); }
        // begin_time
        {
            DateTimeValue ts;
            ts.from_unixtime(log.begin_time / 1000, _ctz); // Convert ms to seconds
            fill_column_with_slot<TYPE_DATETIME>((*chunk)->columns()[col_idx++].get(), (void*)&ts);
        }
        // finish_time
        {
            DateTimeValue ts;
            ts.from_unixtime(log.finish_time / 1000, _ctz); // Convert ms to seconds
            fill_column_with_slot<TYPE_DATETIME>((*chunk)->columns()[col_idx++].get(), (void*)&ts);
        }
        // txn_id
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.txn_id)); }
        // tablet_id
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.tablet_id)); }
        // table_id
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.table_id)); }
        // partition_id
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.partition_id)); }
        // log_type
        {
            std::string type_str = (log.log_type == lake::LogType::LOAD) ? "LOAD" : "COMPACTION";
            Slice s(type_str);
            ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(s));
        }
        // input_rows
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.input_rows)); }
        // input_bytes
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.input_bytes)); }
        // output_rows
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.output_rows)); }
        // output_bytes
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.output_bytes)); }
        // input_segments
        {
            auto* column = (*chunk)->columns()[col_idx++].get();
            if (log.log_type == lake::LogType::COMPACTION) {
                ColumnHelper::get_data_column(column)->append_datum(Datum(log.input_segments));
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
        }
        // output_segments
        { ColumnHelper::get_data_column((*chunk)->columns()[col_idx++].get())->append_datum(Datum(log.output_segments)); }
        // label
        {
            auto* column = (*chunk)->columns()[col_idx++].get();
            if (log.log_type == lake::LogType::LOAD) {
                Slice s(log.label);
                ColumnHelper::get_data_column(column)->append_datum(Datum(s));
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
        }
        // compaction_score
        {
            auto* column = (*chunk)->columns()[col_idx++].get();
            if (log.log_type == lake::LogType::COMPACTION) {
                ColumnHelper::get_data_column(column)->append_datum(Datum(log.compaction_score));
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
        }
        // compaction_type
        {
            auto* column = (*chunk)->columns()[col_idx++].get();
            if (log.log_type == lake::LogType::COMPACTION) {
                Slice s(log.compaction_type);
                ColumnHelper::get_data_column(column)->append_datum(Datum(s));
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks
