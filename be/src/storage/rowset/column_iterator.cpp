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

#include "storage/rowset/column_iterator.h"

#include "column/fixed_length_column.h"
#include "column/nullable_column.h"

namespace starrocks {

Status ColumnIterator::decode_dict_codes(const Column& codes, Column* words) {
    if (codes.is_nullable()) {
        const ColumnPtr& data_column = down_cast<const NullableColumn&>(codes).data_column();
        const Buffer<int32_t>& v = Int32Column::static_pointer_cast(data_column)->get_data();
        return this->decode_dict_codes(v.data(), v.size(), words);
    } else {
        const Buffer<int32_t>& v = down_cast<const Int32Column&>(codes).get_data();
        return this->decode_dict_codes(v.data(), v.size(), words);
    }
}

Status ColumnIterator::fetch_values_by_rowid(const Column& rowids, Column* values) {
    static_assert(std::is_same_v<uint32_t, rowid_t>);
    const auto& numeric_col = down_cast<const FixedLengthColumn<rowid_t>&>(rowids);
    const auto* p = reinterpret_cast<const rowid_t*>(numeric_col.get_data().data());
    return fetch_values_by_rowid(p, rowids.size(), values);
}

Status ColumnIterator::fetch_dict_codes_by_rowid(const Column& rowids, Column* values) {
    static_assert(std::is_same_v<uint32_t, rowid_t>);
    const auto& numeric_col = down_cast<const FixedLengthColumn<rowid_t>&>(rowids);
    const auto* p = reinterpret_cast<const rowid_t*>(numeric_col.get_data().data());
    return fetch_dict_codes_by_rowid(p, rowids.size(), values);
}

Status ColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    auto iter = range.new_iterator();
    auto to_read = range.span_size();
    while (to_read > 0) {
        RETURN_IF_ERROR(seek_to_ordinal(iter.begin()));
        auto r = Range<>{iter.next(to_read)};
        auto n = size_t{r.span_size()};
        RETURN_IF_ERROR(next_batch(&n, dst));
        CHECK_EQ(r.span_size(), n);
        to_read -= n;
    }
    return Status::OK();
}

Status ColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    auto n = size_t{1};
    for (auto i = size_t{0}; i < size; i++) {
        RETURN_IF_ERROR(seek_to_ordinal(rowids[i]));
        RETURN_IF_ERROR(next_batch(&n, values));
        DCHECK_EQ(1, n);
    }
    return Status::OK();
}

} // namespace starrocks
