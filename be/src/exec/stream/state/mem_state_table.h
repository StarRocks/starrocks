// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/datum.h"
#include "column/vectorized_field.h"
#include "column/vectorized_schema.h"
#include "exec/stream/state/state_table.h"
#include "storage/chunk_iterator.h"

namespace starrocks::stream {

using VectorizedFields = vectorized::VectorizedFields;
using VectorizedSchema = vectorized::VectorizedSchema;
using DatumKeyRow = std::vector<vectorized::DatumKey>;

// NOTE: This class is only used in testing. DatumRowIterator is used to convert datum to chunk iter.
class DatumRowIterator final : public vectorized::ChunkIterator {
public:
    explicit DatumRowIterator(VectorizedSchema schema, std::vector<DatumRow>&& rows)
            : ChunkIterator(schema, rows.size()), _rows(std::move(rows)) {}
    void close() override {}

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override {
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

    Status init() override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status commit(RuntimeState* state) override;
    ChunkPtrOr seek_key(const DatumRow& key) const override;
    std::vector<ChunkPtrOr> seek_keys(const std::vector<DatumRow>& keys) const override;
    ChunkIteratorPtrOr prefix_scan_key(const DatumRow& key) const override;
    std::vector<ChunkIteratorPtrOr> prefix_scan_keys(const std::vector<DatumRow>& keys) const override;
    Status flush(RuntimeState* state, vectorized::StreamChunk* chunk) override;
    Status flush(RuntimeState* state, vectorized::Chunk* chunk) override;

private:
    VectorizedSchema _make_schema_from_slots(const std::vector<SlotDescriptor*>& slots) const;
    static DatumKeyRow _convert_datum_row_to_key(const DatumRow& row, size_t start, size_t end);
    static DatumKeyRow _make_datum_key_row(vectorized::Chunk* chunk, size_t start, size_t end, int row_idx);
    static DatumRow _make_datum_row(vectorized::Chunk* chunk, size_t start, size_t end, int row_idx);
    bool _equal_keys(const DatumKeyRow& m_k, const DatumRow key) const;

private:
    TupleDescriptor* _tuple_desc;
    std::vector<SlotDescriptor*> _slots;
    size_t _k_num;
    size_t _cols_num;
    std::map<DatumKeyRow, DatumRow> _kv_mapping;
    // value's schema
    VectorizedSchema _v_schema;
};

} // namespace starrocks::stream
