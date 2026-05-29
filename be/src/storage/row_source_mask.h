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

#include <cstdint>
#include <string>
#include <vector>

#include "column/fixed_length_column.h"
#include "common/statusor.h"
#include "storage/primitive/row_source_mask.h"

namespace starrocks {

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
    explicit RowSourceMaskBuffer(int64_t tablet_id, std::string storage_root_path);
    ~RowSourceMaskBuffer();

    Status write(const std::vector<RowSourceMask>& source_masks);
    StatusOr<bool> has_remaining();
    bool has_same_source(uint16_t source, size_t count) const;
    size_t max_same_source_count(uint16_t source, size_t upper_bound) const;

    RowSourceMask current() const { return {_mask_column->get(_current_index).get_uint16()}; }
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

} // namespace starrocks
