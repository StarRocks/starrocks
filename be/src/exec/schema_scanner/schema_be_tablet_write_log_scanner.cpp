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
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "types/datetime_value.h"
#include "types/datum.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaBeTabletWriteLogScanner::_s_tbls_columns[] = {
        {"BE_ID", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"BEGIN_TIME", TypeDescriptor(TYPE_DATETIME), sizeof(DateTimeValue), false},
        {"FINISH_TIME", TypeDescriptor(TYPE_DATETIME), sizeof(DateTimeValue), false},
        {"TXN_ID", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"TABLET_ID", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"TABLE_ID", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"PARTITION_ID", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"LOG_TYPE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"INPUT_ROWS", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"INPUT_BYTES", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"OUTPUT_ROWS", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"OUTPUT_BYTES", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), false},
        {"INPUT_SEGMENTS", TypeDescriptor(TYPE_INT), sizeof(int32_t), true},
        {"OUTPUT_SEGMENTS", TypeDescriptor(TYPE_INT), sizeof(int32_t), false},
        {"LABEL", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"COMPACTION_SCORE", TypeDescriptor(TYPE_BIGINT), sizeof(int64_t), true},
        {"COMPACTION_TYPE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
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
        size_t col_idx = 0;

        // be_id
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.backend_id));
        }
        // begin_time
        {
            DateTimeValue ts;
            ts.from_unixtime(log.begin_time / 1000, _ctz); // Convert ms to seconds
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            fill_column_with_slot<TYPE_DATETIME>(col, (void*)&ts);
        }
        // finish_time
        {
            DateTimeValue ts;
            ts.from_unixtime(log.finish_time / 1000, _ctz); // Convert ms to seconds
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            fill_column_with_slot<TYPE_DATETIME>(col, (void*)&ts);
        }
        // txn_id
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.txn_id));
        }
        // tablet_id
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.tablet_id));
        }
        // table_id
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.table_id));
        }
        // partition_id
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.partition_id));
        }
        // log_type
        {
            std::string type_str = (log.log_type == lake::LogType::LOAD) ? "LOAD" : "COMPACTION";
            Slice s(type_str);
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(s));
        }
        // input_rows
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.input_rows));
        }
        // input_bytes
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.input_bytes));
        }
        // output_rows
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.output_rows));
        }
        // output_bytes
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.output_bytes));
        }
        // input_segments
        {
            auto& column = (*chunk)->get_column_by_index(col_idx++);
            Column* col = (Column*)column.get();
            if (log.log_type == lake::LogType::COMPACTION) {
                col->append_datum(Datum(log.input_segments));
            } else {
                col->append_nulls(1);
            }
        }
        // output_segments
        {
            Column* col = (Column*)(*chunk)->get_column_by_index(col_idx++).get();
            ColumnHelper::get_data_column(col)->append_datum(Datum(log.output_segments));
        }
        // label
        {
            auto& column = (*chunk)->get_column_by_index(col_idx++);
            Column* col = (Column*)column.get();
            if (log.log_type == lake::LogType::LOAD) {
                Slice s(log.label);
                col->append_datum(Datum(s));
            } else {
                col->append_nulls(1);
            }
        }
        // compaction_score
        {
            auto& column = (*chunk)->get_column_by_index(col_idx++);
            Column* col = (Column*)column.get();
            if (log.log_type == lake::LogType::COMPACTION) {
                col->append_datum(Datum(log.compaction_score));
            } else {
                col->append_nulls(1);
            }
        }
        // compaction_type
        {
            auto& column = (*chunk)->get_column_by_index(col_idx++);
            Column* col = (Column*)column.get();
            if (log.log_type == lake::LogType::COMPACTION) {
                Slice s(log.compaction_type);
                col->append_datum(Datum(s));
            } else {
                col->append_nulls(1);
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks
