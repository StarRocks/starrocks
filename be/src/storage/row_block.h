// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/row_block.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef STARROCKS_BE_SRC_OLAP_ROW_BLOCK_H
#define STARROCKS_BE_SRC_OLAP_ROW_BLOCK_H

#include <exception>
#include <iterator>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "runtime/vectorized_row_batch.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/row_cursor.h"
#include "storage/utils.h"

namespace starrocks {

class ExprContext;

struct RowBlockInfo {
    RowBlockInfo() {}
    RowBlockInfo(uint32_t value, uint32_t num) : checksum(value), row_num(num) {}

    uint32_t checksum{0};
    uint32_t row_num{0};
    bool null_supported = false;
    std::vector<uint32_t> column_ids;
};

// To be removed
class RowBlock {
    // Please keep these classes as 'friend'.  They have to use lots of private fields for
    // faster operation.
    friend class RowBlockChanger;
    friend class VectorizedRowBatch;

public:
    RowBlock(const TabletSchema* schema);

    ~RowBlock();

    OLAPStatus init(const RowBlockInfo& block_info);

    inline void get_row(uint32_t row_index, RowCursor* cursor) const {
        cursor->attach(_mem_buf + row_index * _mem_row_bytes);
    }

    template <typename RowType>
    inline void set_row(uint32_t row_index, const RowType& row) const {
        memcpy(_mem_buf + row_index * _mem_row_bytes, row.row_ptr(), _mem_row_bytes);
    }

    // called when finished fill this row_block
    OLAPStatus finalize(uint32_t row_num);

    const uint32_t row_num() const { return _info.row_num; }
    const RowBlockInfo& row_block_info() const { return _info; }
    const TabletSchema& tablet_schema() const { return *_schema; }
    size_t capacity() const { return _capacity; }

    // Return field pointer, this pointer point to the nullbyte before the field
    // layout is nullbyte|Field
    inline char* field_ptr(size_t row, size_t col) const {
        return _mem_buf + _mem_row_bytes * row + _field_offset_in_memory[col];
    }

    MemPool* mem_pool() const { return _mem_pool.get(); }

    void clear();

    size_t pos() const { return _pos; }
    void set_pos(size_t pos) { _pos = pos; }
    void pos_inc() { _pos++; }
    size_t limit() const { return _limit; }
    void set_limit(size_t limit) { _limit = limit; }
    size_t remaining() const { return _limit - _pos; }
    bool has_remaining() const { return _pos < _limit; }
    uint8_t block_status() const { return _block_status; }
    void set_block_status(uint8_t status) { _block_status = status; }

private:
    bool has_nullbyte() { return _null_supported; }

    // Compute layout for storage buffer and  memory buffer
    void _compute_layout();

    uint32_t _capacity;
    RowBlockInfo _info;
    const TabletSchema* _schema;

    bool _null_supported;

    // Data in memory is construct from row cursors, these row cursors's size is equal
    char* _mem_buf = nullptr;
    // equal with _mem_row_bytes * _info.row_num
    size_t _mem_buf_bytes = 0;
    // row's size in bytes, in one block, all rows's size is equal
    size_t _mem_row_bytes = 0;

    // Field offset of memory row format, used to get field ptr in memory row
    std::vector<size_t> _field_offset_in_memory;

    // only used for SegmentReader to covert VectorizedRowBatch to RowBlock
    // Be careful to use this
    size_t _pos = 0;
    size_t _limit = 0;
    uint8_t _block_status = DEL_PARTIAL_SATISFIED;

    std::unique_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _mem_pool;

    RowBlock(const RowBlock&) = delete;
    const RowBlock& operator=(const RowBlock&) = delete;
};

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_ROW_BLOCK_H
