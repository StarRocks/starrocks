// This file is made available under Elastic License 2.0.
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

#include "storage/rowset/segment_v2/column_iterator.h"

#include "column/fixed_length_column.h"
#include "column/nullable_column.h"

namespace starrocks {
namespace segment_v2 {

Status ColumnIterator::decode_dict_codes(const vectorized::Column& codes, vectorized::Column* words) {
    if (codes.is_nullable()) {
        const vectorized::ColumnPtr& data_column = down_cast<const vectorized::NullableColumn&>(codes).data_column();
        const vectorized::Buffer<int32_t>& v =
                std::static_pointer_cast<vectorized::Int32Column>(data_column)->get_data();
        return this->decode_dict_codes(v.data(), v.size(), words);
    } else {
        const vectorized::Buffer<int32_t>& v = down_cast<const vectorized::Int32Column&>(codes).get_data();
        return this->decode_dict_codes(v.data(), v.size(), words);
    }
}

Status ColumnIterator::fetch_values_by_rowid(const vectorized::Column& rowids, vectorized::Column* values) {
    static_assert(std::is_same_v<uint32_t, rowid_t>);
    const auto& numeric_col = down_cast<const vectorized::FixedLengthColumn<rowid_t>&>(rowids);
    const auto* p = reinterpret_cast<const rowid_t*>(numeric_col.get_data().data());
    return fetch_values_by_rowid(p, rowids.size(), values);
}

Status ColumnIterator::fetch_dict_codes_by_rowid(const vectorized::Column& rowids, vectorized::Column* values) {
    static_assert(std::is_same_v<uint32_t, rowid_t>);
    const auto& numeric_col = down_cast<const vectorized::FixedLengthColumn<rowid_t>&>(rowids);
    const auto* p = reinterpret_cast<const rowid_t*>(numeric_col.get_data().data());
    return fetch_dict_codes_by_rowid(p, rowids.size(), values);
}

} // namespace segment_v2
} // namespace starrocks