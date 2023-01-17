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

Status MemStateTable::init() {
    return Status::OK();
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

bool MemStateTable::_equal_keys(const DatumKeyRow& m_k, const DatumRow key) const {
    for (auto i = 0; i < key.size(); i++) {
        if (!key[i].equal_datum_key(m_k[i])) {
            return false;
        }
    }
    return true;
}

ChunkPtrOr MemStateTable::seek(const DatumRow& key) const {
    // point seek
    DCHECK_EQ(key.size(), _k_num);
    auto key_row = _convert_datum_row_to_key(key, 0, _k_num);
    if (auto iter = _kv_mapping.find(key_row); iter != _kv_mapping.end()) {
        auto chunk_ptr = ChunkHelper::new_chunk(_v_schema, 1);
        convert_datum_rows_to_chunk({iter->second}, chunk_ptr.get());
        return std::move(chunk_ptr);
    } else {
        return Status::EndOfFile("NotFound");
    }
}

std::vector<ChunkPtrOr> MemStateTable::seek(const std::vector<DatumRow>& keys) const {
    std::vector<ChunkPtrOr> ans;
    ans.reserve(keys.size());
    for (auto& key : keys) {
        ans.emplace_back(seek(key));
    }
    return ans;
}

ChunkIteratorPtrOr MemStateTable::prefix_scan(const DatumRow& key) const {
    DCHECK_LE(key.size(), _k_num);
    // prefix scan
    std::vector<DatumRow> rows;
    for (auto iter = _kv_mapping.begin(); iter != _kv_mapping.end(); iter++) {
        // if equal
        auto m_k = iter->first;
        if (_equal_keys(m_k, key)) {
            DatumRow row;
            // add extra key cols + value cols
            for (int32_t s = key.size(); s < m_k.size(); s++) {
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
    auto schema = _make_schema_from_slots(std::vector<SlotDescriptor*>{_slots.begin() + key.size(), _slots.end()});
    return std::make_shared<DatumRowIterator>(schema, std::move(rows));
}

VectorizedSchema MemStateTable::_make_schema_from_slots(const std::vector<SlotDescriptor*>& slots) const {
    VectorizedFields fields;
    for (auto& slot : slots) {
        auto type_desc = slot->type();
        VLOG_ROW << "[make_schema_from_slots] type:" << type_desc;
        auto field = std::make_shared<VectorizedField>(slot->id(), slot->col_name(), type_desc.type, false);
        fields.emplace_back(std::move(field));
    }
    return VectorizedSchema(std::move(fields), KeysType::PRIMARY_KEYS, {});
}

std::vector<ChunkIteratorPtrOr> MemStateTable::prefix_scan(const std::vector<DatumRow>& keys) const {
    std::vector<ChunkIteratorPtrOr> ans;
    ans.reserve(keys.size());
    for (auto& key : keys) {
        ans.emplace_back(prefix_scan(key));
    }
    return ans;
}

DatumRow MemStateTable::_make_datum_row(Chunk* chunk, size_t start, size_t end, int row_idx) {
    DatumRow row;
    for (size_t i = start; i < end; i++) {
        DCHECK_LT(i, chunk->num_columns());
        auto& column = chunk->get_column_by_index(i);
        row.push_back(column->get(row_idx));
    }
    return row;
}

DatumKeyRow MemStateTable::_make_datum_key_row(Chunk* chunk, size_t start, size_t end, int row_idx) {
    DatumKeyRow row;
    for (size_t i = start; i < end; i++) {
        DCHECK_LT(i, chunk->num_columns());
        auto& column = chunk->get_column_by_index(i);
        row.push_back((column->get(row_idx)).convert2DatumKey());
    }
    return row;
}

DatumKeyRow MemStateTable::_convert_datum_row_to_key(const DatumRow& row, size_t start, size_t end) {
    DatumKeyRow key_row;
    for (size_t i = start; i < end; i++) {
        auto datum = row[i];
        key_row.push_back(datum.convert2DatumKey());
    }
    return key_row;
}

Status MemStateTable::flush(RuntimeState* state, StreamChunk* chunk) {
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

} // namespace starrocks::stream