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

#include "formats/csv/decimalv3_converter.h"

#include "column/decimalv3_column.h"
#include "common/logging.h"
#include "runtime/decimalv3.h"

namespace starrocks::csv {

template <typename T>
Status DecimalV3Converter<T>::write_string(OutputStream* os, const Column& column, size_t row_num,
                                           const Options& options) const {
    auto decimalv3_column = down_cast<const DecimalV3Column<T>*>(&column);
    // TODO(zhuming): avoid this string construction
    auto s = DecimalV3Cast::to_string<T>(decimalv3_column->get_data()[row_num], _precision, _scale);
    return os->write(Slice(s));
}

template <typename T>
Status DecimalV3Converter<T>::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                                  const Options& options) const {
    return write_string(os, column, row_num, options);
}

template <typename T>
bool DecimalV3Converter<T>::read_string(Column* column, Slice s, const Options& options) const {
    auto decimalv3_column = down_cast<DecimalV3Column<T>*>(column);
    T v;
    bool fail = DecimalV3Cast::from_string<T>(&v, _precision, _scale, s.data, s.size);
    if (!fail) {
        decimalv3_column->append(v);
        return true;
    }
    return false;
}

template <typename T>
bool DecimalV3Converter<T>::read_quoted_string(Column* column, Slice s, const Options& options) const {
    return read_string(column, s, options);
}

/// Explicit template instantiations
template class DecimalV3Converter<int32_t>;
template class DecimalV3Converter<int64_t>;
template class DecimalV3Converter<int128_t>;

} // namespace starrocks::csv
