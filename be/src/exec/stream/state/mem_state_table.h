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

#include "column/datum.h"
#include "column/field.h"
#include "column/schema.h"
#include "exec/stream/state/state_table.h"
#include "storage/chunk_iterator.h"

namespace starrocks::stream {

using DatumRow = std::vector<Datum>;
using DatumKeyRow = std::vector<DatumKey>;
using DatumRowPtr = std::shared_ptr<DatumRow>;
using DatumRowOpt = std::optional<DatumRow>;

// NOTE: This class is only used in testing. DatumRowIterator is used to convert datum to chunk iter.
class DatumRowIterator final : public ChunkIterator {
public:
    explicit DatumRowIterator(Schema schema, std::vector<DatumRow>&& rows)
            : ChunkIterator(schema, rows.size()), _rows(std::move(rows)) {}
    void close() override {}

protected:
    [[nodiscard]] Status do_get_next(Chunk* chunk) override;
    [[nodiscard]] Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override {
        return Status::EndOfFile("end of empty iterator");
    }

private:
    std::vector<DatumRow> _rows;
    bool _is_eos{false};
};

// NOTE: MemStateTable is only used for testing to mock `StateTable`.
class MemStateTable : public StateTable {
public:
    // For MemStateTable, we assume flushed chunk's columns is assigned as:
    // _k_num | _v_num
    MemStateTable(std::vector<SlotDescriptor*> slots, size_t k_num)
            : _slots(slots), _k_num(k_num), _cols_num(slots.size()) {
        for (auto i = 0; i < _slots.size(); i++) {
            auto& slot = _slots[i];
            if (i < k_num) {
                VLOG_ROW << "[MemStateTable] [Key] slot:" << slot->debug_string();
            } else {
                VLOG_ROW << "[MemStateTable] [Value] slot:" << slot->debug_string();
            }
        }
        _v_schema = _make_schema_from_slots(std::vector<SlotDescriptor*>{_slots.begin() + _k_num, _slots.end()});
    }
    ~MemStateTable() override = default;

    [[nodiscard]] Status prepare(RuntimeState* state) override;
    [[nodiscard]] Status open(RuntimeState* state) override;

    [[nodiscard]] Status seek(const Columns& keys, StateTableResult& values) const override;
    [[nodiscard]] Status seek(const Columns& keys, const std::vector<uint8_t>& selection,
                              StateTableResult& values) const override;

    [[nodiscard]] Status seek(const Columns& keys, const std::vector<std::string>& projection_columns,
                              StateTableResult& values) const override;
    ChunkIteratorPtrOr prefix_scan(const Columns& keys, size_t row_idx) const override;
    ChunkIteratorPtrOr prefix_scan(const std::vector<std::string>& projection_columns, const Columns& keys,
                                   size_t row_idx) const override;

    [[nodiscard]] Status write(RuntimeState* state, const StreamChunkPtr& chunk) override;
    [[nodiscard]] Status commit(RuntimeState* state) override;
    [[nodiscard]] Status reset_epoch(RuntimeState* state) override;

private:
    DatumKeyRow _convert_columns_to_key(const Columns& cols, size_t idx) const;
    [[nodiscard]] Status _append_datum_row_to_chunk(const DatumRow& v_row, ChunkPtr& result_chunk) const;
    [[nodiscard]] Status _append_null_to_chunk(ChunkPtr& result_chunk) const;

    Schema _make_schema_from_slots(const std::vector<SlotDescriptor*>& slots) const;
    static DatumKeyRow _make_datum_key_row(const ChunkPtr& chunk, size_t start, size_t end, int row_idx);
    static DatumRow _make_datum_row(const ChunkPtr& chunk, size_t start, size_t end, int row_idx);
    bool _equal_keys(const DatumKeyRow& m_k, const DatumKeyRow& keys) const;

private:
    std::vector<SlotDescriptor*> _slots;
    size_t _k_num;
    size_t _cols_num;
    std::map<DatumKeyRow, DatumRow> _kv_mapping;
    // value's schema
    Schema _v_schema;
};

} // namespace starrocks::stream
