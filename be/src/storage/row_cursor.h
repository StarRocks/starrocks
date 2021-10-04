// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/row_cursor.h

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

#ifndef STARROCKS_BE_SRC_OLAP_ROW_CURSOR_H
#define STARROCKS_BE_SRC_OLAP_ROW_CURSOR_H

#include <string>
#include <vector>

#include "storage/field.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/row_cursor_cell.h"
#include "storage/schema.h"
#include "storage/tuple.h"

namespace starrocks {
class Field;

class RowCursor {
public:
    RowCursor();

    ~RowCursor();

    OLAPStatus init(const TabletSchema& schema);
    OLAPStatus init(const std::vector<TabletColumn>& schema);

    OLAPStatus init(const std::vector<TabletColumn>& schema, size_t column_count);
    OLAPStatus init(const TabletSchema& schema, size_t column_count);

    OLAPStatus init(const TabletSchema& schema, const std::vector<uint32_t>& columns);

    OLAPStatus init(std::unique_ptr<Schema> schema);

    OLAPStatus init_scan_key(const TabletSchema& schema, const std::vector<std::string>& keys);

    //allocate memory for string type, which include char, varchar, hyperloglog
    OLAPStatus allocate_memory_for_string_type(const TabletSchema& schema);

    RowCursorCell cell(uint32_t cid) const { return RowCursorCell(nullable_cell_ptr(cid)); }

    inline void attach(char* buf) { _fixed_buf = buf; }

    void write_index_by_index(size_t index, char* index_ptr) const {
        auto dst_cell = RowCursorCell(index_ptr);
        column_schema(index)->to_index(&dst_cell, cell(index));
    }

    // deep copy field content (ignore null-byte)
    void set_field_content(size_t index, const char* buf, MemPool* mem_pool) {
        char* dest = cell_ptr(index);
        column_schema(index)->deep_copy_content(dest, buf, mem_pool);
    }

    // shallow copy field content (ignore null-byte)
    void set_field_content_shallow(size_t index, const char* buf) {
        char* dst_cell = cell_ptr(index);
        column_schema(index)->shallow_copy_content(dst_cell, buf);
    }
    // convert and deep copy field content
    OLAPStatus convert_from(size_t index, const char* src, const TypeInfoPtr& src_type, MemPool* mem_pool) {
        char* dest = cell_ptr(index);
        return column_schema(index)->convert_from(dest, src, src_type, mem_pool);
    }

    OLAPStatus from_tuple(const OlapTuple& tuple);

    size_t field_count() const { return _schema->column_ids().size(); }

    std::string to_string() const;
    OlapTuple to_tuple() const;

    const size_t get_index_size(size_t index) const { return column_schema(index)->index_size(); }

    // set max/min for key field in _field_array
    OLAPStatus build_max_key();
    OLAPStatus build_min_key();

    inline char* get_buf() const { return _fixed_buf; }

    // this two functions is used in unit test
    inline size_t get_fixed_len() const { return _fixed_len; }
    inline size_t get_variable_len() const { return _variable_len; }

    // Get column nullable pointer with column id
    // TODO(zc): make this return const char*
    char* nullable_cell_ptr(uint32_t cid) const { return _fixed_buf + _schema->column_offset(cid); }
    char* cell_ptr(uint32_t cid) const { return _fixed_buf + _schema->column_offset(cid) + 1; }

    bool is_null(size_t index) const { return *reinterpret_cast<bool*>(nullable_cell_ptr(index)); }

    inline void set_null(size_t index) const { *reinterpret_cast<bool*>(nullable_cell_ptr(index)) = true; }

    inline void set_not_null(size_t index) const { *reinterpret_cast<bool*>(nullable_cell_ptr(index)) = false; }

    size_t column_size(uint32_t cid) const { return _schema->column_size(cid); }

    const Field* column_schema(uint32_t cid) const { return _schema->column(cid); }

    const Schema* schema() const { return _schema.get(); }

    char* row_ptr() const { return _fixed_buf; }

    Status convert_to(const RowCursor** outpout, const std::vector<FieldType>& new_types, ObjectPool* obj_pool) const;

private:
    // common init function
    OLAPStatus _init(const std::vector<TabletColumn>& schema, const std::vector<uint32_t>& columns);
    OLAPStatus _init_with_schema();

    std::unique_ptr<Schema> _schema;

    char* _fixed_buf = nullptr; // point to fixed buf
    size_t _fixed_len{0};
    char* _owned_fixed_buf = nullptr; // point to buf allocated in init function

    char* _variable_buf = nullptr;
    size_t _variable_len{0};

    RowCursor(const RowCursor&) = delete;
    const RowCursor& operator=(const RowCursor&) = delete;
};
} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_ROW_CURSOR_H
