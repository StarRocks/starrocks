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

#include "formats/csv/float_converter.h"

#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "util/string_parser.hpp"

namespace starrocks::csv {

template <typename T>
Status FloatConverter<T>::write_string(OutputStream* os, const Column& column, size_t row_num,
                                       const Options& options) const {
    auto float_column = down_cast<const FixedLengthColumn<DataType>*>(&column);
    return os->write(float_column->get_data()[row_num]);
}

template <typename T>
Status FloatConverter<T>::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                              const Options& options) const {
    return write_string(os, column, row_num, options);
}

template <typename T>
bool FloatConverter<T>::read_string(Column* column, Slice s, const Options& options) const {
    StringParser::ParseResult r;
    auto v = StringParser::string_to_float<DataType>(s.data, s.size, &r);
    if (r == StringParser::PARSE_SUCCESS) {
        down_cast<FixedLengthColumn<DataType>*>(column)->append_numbers(&v, sizeof(v));
    }
    return r == StringParser::PARSE_SUCCESS;
}

template <typename T>
bool FloatConverter<T>::read_quoted_string(Column* column, Slice s, const Options& options) const {
    return read_string(column, s, options);
}

/// Explicit template instantiations
template class FloatConverter<float>;
template class FloatConverter<double>;

} // namespace starrocks::csv
