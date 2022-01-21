// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/fixed_length_column.h"
#include "common/statusor.h"

namespace starrocks::vectorized {

// RowSourceMask stores a uint16_t data that represents the source (segment iterator in compaction) of each row
// and the aggregation state of the row.
//
// RowSourceMask is used to merge non-key columns without comparing keys in vertical compaction,
// and to improve compaction efficiency and reduce memory usage.
//
// The lower 15 bits represent the source number, so the count of sources must be be less than or equal to 0x7FFF.
// The higher 1 bit represents the aggregation flag, and is used in agg and uniq data model.
// Agg flag is 1 means that the key of the row is the same as the previous row.
struct RowSourceMask {
    RowSourceMask(uint16_t data) : data(data) {}

    RowSourceMask(uint16_t source_num, bool agg_flag) {
        set_source_num(source_num);
        set_agg_flag(agg_flag);
    }

    uint16_t get_source_num() const { return data & MASK_NUMBER; }

    bool get_agg_flag() const { return (data & MASK_FLAG) != 0; }

    void set_source_num(uint16_t source_num) { data = (data & MASK_FLAG) | (source_num & MASK_NUMBER); }

    void set_agg_flag(bool agg_flag) { data = agg_flag ? data | MASK_FLAG : data & ~MASK_FLAG; }

    uint16_t data = 0;

    static constexpr size_t MAX_SOURCES = 0x7FFF;
    static constexpr uint16_t MASK_NUMBER = 0x7FFF;
    static constexpr uint16_t MASK_FLAG = 0x8000;
};

// RowSourceMaskBuffer is responsible for storing a series of row source masks.
// When the buffer exceeds vertical_compaction_max_memory_mask_size, it will be persisted to a temporary file on the disk.
//
// Usage Example:
//     // create
//     RowSourceMaskBuffer buffer;
//
//     // write masks
//     buffer.write(masks1);
//     buffer.write(masks2);
//     ...
//     buffer.flush();
//
//     // read masks
//     buffer.flip_to_read();
//     while (buffer.has_remaining().value()) {
//         RowSourceMask mask = buffer.current();
//         buffer.advance();
//     }
//
class RowSourceMaskBuffer {
public:
    explicit RowSourceMaskBuffer(int64_t tablet_id, const std::string& storage_root_path);
    ~RowSourceMaskBuffer();

    Status write(const std::vector<RowSourceMask>& source_masks);
    StatusOr<bool> has_remaining();
    bool has_same_source(uint16_t source, size_t count) const;

    RowSourceMask current() const { return RowSourceMask(_mask_column->get(_current_index).get_uint16()); }
    void advance() { ++_current_index; }

    Status flip_to_read();
    Status flush();

private:
    void _reset_mask_column() { _mask_column->reset_column(); }
    Status _create_tmp_file();
    Status _serialize_masks();
    Status _deserialize_masks();

    UInt16Column::MutablePtr _mask_column;

    // for read
    uint64_t _current_index = 0;

    // temporary file for persistence
    int _tmp_file_fd = -1;
    int64_t _tablet_id;
    std::string _storage_root_path;
};

} // namespace starrocks::vectorized
