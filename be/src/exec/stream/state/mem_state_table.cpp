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

#include "mem_state_table.h"

namespace starrocks::stream {
namespace {

void convert_datum_rows_to_chunk(const std::vector<DatumRow>& rows, Chunk* chunk) {
    // Chunk should be already allocated, no need create column any more.
    DCHECK(chunk);
    for (size_t row_num = 0; row_num < rows.size(); row_num++) {
        auto& row = rows[row_num];
        for (size_t i = 0; i < row.size(); i++) {
            VLOG_ROW << "[convert_datum_rows_to_chunk] row_num:" << row_num << ", column_size:" << row.size()
                     << ", i:" << i;
            DCHECK_LT(i, chunk->num_columns());
            auto& col = chunk->get_column_by_index(i);
            col->append_datum(row[i]);
        }
    }
}

} // namespace

Status DatumRowIterator::do_get_next(Chunk* chunk) {
    if (!_is_eos) {
        convert_datum_rows_to_chunk(_rows, chunk);
        _is_eos = true;
        return Status::OK();
    }
    return Status::EndOfFile("end of empty iterator");
}

Status MemStateTable::prepare(RuntimeState* state) {
    return Status::OK();
}

Status MemStateTable::open(RuntimeState* state) {
    return Status::OK();
}

Status MemStateTable::commit(RuntimeState* state) {
    return Status::OK();
}

bool MemStateTable::_equal_keys(const DatumKeyRow& m_k, const DatumKeyRow& keys) const {
    for (auto i = 0; i < keys.size(); i++) {
        Datum datum(keys[i]);
        if (!datum.equal_datum_key(m_k[i])) {
            return false;
        }
    }
    return true;
}

Status MemStateTable::seek(const Columns& keys, StateTableResult& values) const {
    auto num_rows = keys[0]->size();
    auto& found = values.found;
    auto& result_chunk = values.result_chunk;
    found.resize(num_rows, 0);
    result_chunk = ChunkHelper::new_chunk(_v_schema, num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        auto key_row = _convert_columns_to_key(keys, i);
        if (auto iter = _kv_mapping.find(key_row); iter != _kv_mapping.end()) {
            found[i] = 1;
            RETURN_IF_ERROR(_append_datum_row_to_chunk(iter->second, result_chunk));
        }
    }
    return Status::OK();
}

Status MemStateTable::_append_null_to_chunk(ChunkPtr& result_chunk) const {
    for (auto& column : result_chunk->columns()) {
        column->append_nulls(1);
    }
    return Status::OK();
}

Status MemStateTable::_append_datum_row_to_chunk(const DatumRow& v_row, ChunkPtr& result_chunk) const {
    DCHECK_EQ(v_row.size(), result_chunk->num_columns());
    auto& columns = result_chunk->columns();
    for (size_t i = 0; i < result_chunk->num_columns(); i++) {
        columns[i]->append_datum(v_row[i]);
    }
    return Status::OK();
}

Status MemStateTable::seek(const Columns& keys, const std::vector<uint8_t>& selection, StateTableResult& values) const {
    DCHECK_LT(0, keys.size());
    auto num_rows = keys[0]->size();
    DCHECK_EQ(selection.size(), num_rows);

    auto& found = values.found;
    auto& result_chunk = values.result_chunk;
    found.resize(num_rows, 0);
    result_chunk = ChunkHelper::new_chunk(_v_schema, num_rows);
    VLOG_ROW << "selection size:" << selection.size() << ", num_rows:" << num_rows
             << ", num_columns:" << result_chunk->num_columns();
    for (size_t i = 0; i < num_rows; i++) {
        if (selection[i]) {
            auto key_row = _convert_columns_to_key(keys, i);
            if (auto iter = _kv_mapping.find(key_row); iter != _kv_mapping.end()) {
                VLOG_ROW << "append key with selection";
                found[i] = 1;
                RETURN_IF_ERROR(_append_datum_row_to_chunk(iter->second, result_chunk));
            } else {
                VLOG_ROW << "append null without selection";
            }
        } else {
            VLOG_ROW << "append null";
        }
    }
    return Status::OK();
}

Status MemStateTable::seek(const Columns& keys, const std::vector<std::string>& projection_columns,
                           StateTableResult& values) const {
    return Status::NotSupported("Seek with projection columns is not supported yet.");
}

ChunkIteratorPtrOr MemStateTable::prefix_scan(const Columns& keys, size_t row_idx) const {
    auto key_row = _convert_columns_to_key(keys, row_idx);
    DCHECK_LE(key_row.size(), _k_num);
    // prefix scan
    std::vector<DatumRow> rows;
    for (auto iter = _kv_mapping.begin(); iter != _kv_mapping.end(); iter++) {
        // if equal
        auto m_k = iter->first;
        if (_equal_keys(m_k, key_row)) {
            DatumRow row;
            // add extra key cols + value cols
            for (int32_t s = key_row.size(); s < m_k.size(); s++) {
                row.push_back(Datum(m_k[s]));
            }
            for (auto& datum : iter->second) {
                row.push_back(datum);
            }
            rows.push_back(std::move(row));
        }
    }
    if (rows.empty()) {
        return Status::EndOfFile("");
    }
    auto schema = _make_schema_from_slots(std::vector<SlotDescriptor*>{_slots.begin() + key_row.size(), _slots.end()});
    return std::make_shared<DatumRowIterator>(schema, std::move(rows));
}

ChunkIteratorPtrOr MemStateTable::prefix_scan(const std::vector<std::string>& projection_columns, const Columns& keys,
                                              size_t row_idx) const {
    return Status::NotSupported("PrefixScan with projection columns is not supported yet.");
}

Schema MemStateTable::_make_schema_from_slots(const std::vector<SlotDescriptor*>& slots) const {
    Fields fields;
    for (auto& slot : slots) {
        auto type_desc = slot->type();
        VLOG_ROW << "[make_schema_from_slots] type:" << type_desc;
        // TODO: Must be nullable!
        auto field = std::make_shared<Field>(slot->id(), slot->col_name(), type_desc.type, slot->is_nullable());
        fields.emplace_back(std::move(field));
    }
    return Schema(std::move(fields), KeysType::PRIMARY_KEYS, {});
}

DatumRow MemStateTable::_make_datum_row(const ChunkPtr& chunk, size_t start, size_t end, int row_idx) {
    DatumRow row;
    for (size_t i = start; i < end; i++) {
        DCHECK_LT(i, chunk->num_columns());
        auto& column = chunk->get_column_by_index(i);
        row.push_back(column->get(row_idx));
    }
    return row;
}

DatumKeyRow MemStateTable::_make_datum_key_row(const ChunkPtr& chunk, size_t start, size_t end, int row_idx) {
    DatumKeyRow row;
    for (size_t i = start; i < end; i++) {
        DCHECK_LT(i, chunk->num_columns());
        auto& column = chunk->get_column_by_index(i);
        row.push_back((column->get(row_idx)).convert2DatumKey());
    }
    return row;
}

DatumKeyRow MemStateTable::_convert_columns_to_key(const Columns& cols, size_t idx) const {
    DatumKeyRow key_row;
    for (size_t i = 0; i < cols.size(); i++) {
        auto datum = cols[i]->get(idx);
        key_row.push_back(datum.convert2DatumKey());
    }
    return key_row;
}

Status MemStateTable::write(RuntimeState* state, const StreamChunkPtr& chunk) {
    DCHECK(chunk);
    auto chunk_size = chunk->num_rows();
    if (StreamChunkConverter::has_ops_column(chunk)) {
        auto ops = StreamChunkConverter::ops(chunk);
        for (auto i = 0; i < chunk_size; i++) {
            if (ops[i] == StreamRowOp::OP_UPDATE_BEFORE) {
                continue;
            }
            auto k = _make_datum_key_row(chunk, 0, _k_num, i);
            if (ops[i] == StreamRowOp::OP_DELETE) {
                _kv_mapping.erase(k);
                continue;
            }
            auto v = _make_datum_row(chunk, _k_num, _cols_num, i);
            _kv_mapping[k] = std::move(v);
        }
    } else {
        for (auto i = 0; i < chunk_size; i++) {
            auto k = _make_datum_key_row(chunk, 0, _k_num, i);
            auto v = _make_datum_row(chunk, _k_num, _cols_num, i);
            _kv_mapping[k] = std::move(v);
        }
    }
    return Status::OK();
}

Status MemStateTable::reset_epoch(RuntimeState* state) {
    return Status::OK();
}

} // namespace starrocks::stream