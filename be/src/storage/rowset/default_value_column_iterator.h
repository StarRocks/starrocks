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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_reader.h

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

#pragma once

#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {

class TypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;

// This iterator is used to read default value column
class DefaultValueColumnIterator final : public ColumnIterator {
public:
    DefaultValueColumnIterator(bool has_default_value, std::string default_value, bool is_nullable,
                               TypeInfoPtr type_info, size_t schema_length, ordinal_t num_rows)
            : _has_default_value(has_default_value),
              _default_value(std::move(default_value)),
              _is_nullable(is_nullable),
              _type_info(std::move(type_info)),
              _schema_length(schema_length),

              _pool(),
              _num_rows(num_rows) {}

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override;

    [[nodiscard]] Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = ord_idx;
        return Status::OK();
    }

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override;

    bool all_page_dict_encoded() const override { return false; }

    int dict_lookup(const Slice& word) override { return -1; }

    [[nodiscard]] Status next_dict_codes(size_t* n, Column* dst) override {
        return Status::NotSupported("DefaultValueColumnIterator does not support");
    }

    [[nodiscard]] Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) override {
        return Status::NotSupported("DefaultValueColumnIterator does not support");
    }

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

private:
    bool _has_default_value;
    std::string _default_value;
    bool _is_nullable;
    TypeInfoPtr _type_info;
    size_t _schema_length;
    bool _is_default_value_null{false};
    size_t _type_size{0};
    void* _mem_value = nullptr;
    MemPool _pool;

    // current rowid
    ordinal_t _current_rowid = 0;
    ordinal_t _num_rows = 0;
    bool _may_contain_deleted_row = false;
};

} // namespace starrocks
