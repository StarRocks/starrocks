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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_reader.cpp

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

#include "storage/rowset/default_value_column_iterator.h"

#include "column/column.h"
#include "storage/range.h"
#include "storage/types.h"
#include "util/mem_util.hpp"

namespace starrocks {

Status DefaultValueColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    // be consistent with segment v1
    // if _has_default_value, we should create default column iterator for this column, and
    // "NULL" is a special default value which means the default value is null.
    if (_has_default_value) {
        if (_default_value == "NULL") {
            DCHECK(_is_nullable);
            _is_default_value_null = true;
        } else {
            _type_size = _type_info->size();
            _mem_value = reinterpret_cast<void*>(_pool.allocate(static_cast<int64_t>(_type_size)));
            if (UNLIKELY(_mem_value == nullptr)) {
                return Status::InternalError("Mem usage has exceed the limit of BE");
            }
            Status status = Status::OK();
            if (_type_info->type() == TYPE_CHAR) {
                auto length = static_cast<int32_t>(_schema_length);
                char* string_buffer = reinterpret_cast<char*>(_pool.allocate(length));
                if (UNLIKELY(string_buffer == nullptr)) {
                    return Status::InternalError("Mem usage has exceed the limit of BE");
                }
                memset(string_buffer, 0, length);
                memory_copy(string_buffer, _default_value.c_str(), _default_value.length());
                (static_cast<Slice*>(_mem_value))->size = length;
                (static_cast<Slice*>(_mem_value))->data = string_buffer;
            } else if (_type_info->type() == TYPE_VARCHAR || _type_info->type() == TYPE_HLL ||
                       _type_info->type() == TYPE_OBJECT || _type_info->type() == TYPE_PERCENTILE) {
                auto length = static_cast<int32_t>(_default_value.length());
                char* string_buffer = reinterpret_cast<char*>(_pool.allocate(length));
                if (UNLIKELY(string_buffer == nullptr)) {
                    return Status::InternalError("Mem usage has exceed the limit of BE");
                }
                memory_copy(string_buffer, _default_value.c_str(), length);
                (static_cast<Slice*>(_mem_value))->size = length;
                (static_cast<Slice*>(_mem_value))->data = string_buffer;
            } else if (_type_info->type() == TYPE_ARRAY || _type_info->type() == TYPE_MAP ||
                       _type_info->type() == TYPE_STRUCT) {
                // @todo: need support complex type literal
                return Status::NotSupported("Array/Map/Struct default type is unsupported");
            } else {
                RETURN_IF_ERROR(_type_info->from_string(_mem_value, _default_value));
            }
        }
    } else if (_is_nullable) {
        // if _has_default_value is false but _is_nullable is true, we should return null as default value.
        _is_default_value_null = true;
    } else {
        return Status::InternalError("invalid default value column for no default value and not nullable");
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::next_batch(size_t* n, Column* dst) {
    if (_is_default_value_null) {
        [[maybe_unused]] bool ok = dst->append_nulls(*n);
        _current_rowid += *n;
        DCHECK(ok) << "cannot append null to non-nullable column";
    } else {
        if (_type_info->type() == TYPE_OBJECT || _type_info->type() == TYPE_HLL ||
            _type_info->type() == TYPE_PERCENTILE) {
            std::vector<Slice> slices;
            slices.reserve(*n);
            for (size_t i = 0; i < *n; i++) {
                slices.emplace_back(*reinterpret_cast<const Slice*>(_mem_value));
            }
            (void)dst->append_strings(slices);
        } else {
            dst->append_value_multiple_times(_mem_value, *n);
        }
        _current_rowid += *n;
    }
    if (_may_contain_deleted_row) {
        dst->set_delete_state(DEL_PARTIAL_SATISFIED);
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    size_t to_read = range.span_size();
    if (_is_default_value_null) {
        [[maybe_unused]] bool ok = dst->append_nulls(to_read);
        _current_rowid = range.end();
        DCHECK(ok) << "cannot append null to non-nullable column";
    } else {
        if (_type_info->type() == TYPE_OBJECT || _type_info->type() == TYPE_HLL ||
            _type_info->type() == TYPE_PERCENTILE) {
            std::vector<Slice> slices;
            slices.reserve(to_read);
            for (size_t i = 0; i < to_read; i++) {
                slices.emplace_back(*reinterpret_cast<const Slice*>(_mem_value));
            }
            [[maybe_unused]] auto ret = dst->append_strings(slices);
        } else {
            dst->append_value_multiple_times(_mem_value, to_read);
        }
        _current_rowid = range.end();
    }
    if (_may_contain_deleted_row) {
        dst->set_delete_state(DEL_PARTIAL_SATISFIED);
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    return next_batch(&size, values);
}

Status DefaultValueColumnIterator::get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                              const ColumnPredicate* del_predicate,
                                                              SparseRange<>* row_ranges) {
    DCHECK(row_ranges->empty());
    // TODO
    row_ranges->add({0, static_cast<rowid_t>(_num_rows)});
    // TODO: Setting `_may_contained_deleted_row` to true is a temporary fix,
    // which will affect performance in some scenarios.
    // It is best to filter according to DefaultValue,
    // but the current Expr framework does not support filter for a single line, which will be added later.
    _may_contain_deleted_row = true;
    return Status::OK();
}

} // namespace starrocks
