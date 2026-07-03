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

#include "formats/csv/nullable_converter.h"

#include "column/adaptive_nullable_column.h"
#include "column/nullable_column.h"
#include "common/logging.h"

namespace starrocks::csv {

namespace {
// Raw-bytes null check for LazySimpleSerDe: matches the literal 2-byte sequence "\N",
// decided BEFORE unescaping (an escaped "\\N" unescapes to a literal "\N" string that
// must NOT be treated as null).
inline bool is_raw_null_literal(const Slice& s) {
    return s.size == 2 && s.data[0] == '\\' && s.data[1] == 'N';
}

// Unescape `s` (escape makes the following byte literal; a trailing escape at the end
// of `s` stays literal) into `buf`, returning a Slice into it. Mirrors the top-level
// field unescape LazyUtils performs on a scalar leaf.
inline Slice unescape_into(const Slice& s, char escape, std::string* buf) {
    buf->clear();
    buf->reserve(s.size);
    for (size_t i = 0; i < s.size; i++) {
        if (s.data[i] == escape && i + 1 < s.size) {
            i++;
        }
        buf->push_back(s.data[i]);
    }
    return Slice(buf->data(), buf->size());
}
} // namespace

Status NullableConverter::write_string(formats::FormattedOutputStream* os, const Column& column, size_t row_num,
                                       const Options& options) const {
    if (column.is_nullable()) {
        auto nullable_column = down_cast<const NullableColumn*>(&column);
        auto data_column = nullable_column->data_column().get();
        auto nulls = nullable_column->immutable_null_column_data();
        if (nulls[row_num] != 0) {
            return os->write("\\N");
        } else {
            return _base_converter->write_string(os, *data_column, row_num, options);
        }
    } else {
        return _base_converter->write_string(os, column, row_num, options);
    }
}

Status NullableConverter::write_quoted_string(formats::FormattedOutputStream* os, const Column& column, size_t row_num,
                                              const Options& options) const {
    if (column.is_nullable()) {
        auto nullable_column = down_cast<const NullableColumn*>(&column);
        auto data_column = nullable_column->data_column().get();
        auto nulls = nullable_column->immutable_null_column_data();
        if (nulls[row_num] != 0) {
            return os->write("null");
        } else {
            return _base_converter->write_quoted_string(os, *data_column, row_num, options);
        }
    } else {
        return _base_converter->write_quoted_string(os, column, row_num, options);
    }
}

bool NullableConverter::read_string_for_adaptive_null_column(Column* column, Slice s, const Options& options) const {
    auto* nullable_column = down_cast<AdaptiveNullableColumn*>(column);

    if (options.escape != 0) {
        if (is_raw_null_literal(s)) {
            return nullable_column->append_nulls(1);
        }
        if (!_base_converter->consumes_raw_bytes()) {
            s = unescape_into(s, options.escape, &_unescape_buf);
        }
    } else if (s == "\\N") {
        return nullable_column->append_nulls(1);
    }
    auto* data = nullable_column->begin_append_not_default_value();
    if (_base_converter->read_string(data, s, options)) {
        nullable_column->finish_append_one_not_default_value();
        return true;
    } else if (options.invalid_field_as_null) {
        return nullable_column->append_nulls(1);
    } else {
        return false;
    }
}

bool NullableConverter::read_string(Column* column, const Slice& s_in, const Options& options) const {
    auto* nullable = down_cast<NullableColumn*>(column);
    auto* data = nullable->data_column_raw_ptr();
    Slice s = s_in;

    if (options.escape != 0) {
        // LazySimpleSerDe (ESCAPED BY): decide null on the RAW bytes before touching
        // escapes at all, then only unescape if the base converter wants clean bytes.
        // A Map/ArrayConverter answers consumes_raw_bytes()=true and does its OWN
        // escape-aware separator scanning on `s` unescaped -- e.g. an escaped array
        // element separator like "a\|b|c" must stay intact for it to find the real
        // boundary.
        if (is_raw_null_literal(s)) {
            return nullable->append_nulls(1);
        }
        if (!_base_converter->consumes_raw_bytes()) {
            s = unescape_into(s, options.escape, &_unescape_buf);
        }
    } else if (s == "\\N") {
        return nullable->append_nulls(1);
    }

    if (_base_converter->read_string(data, s, options)) {
        nullable->null_column_raw_ptr()->append(0);
        return true;
    } else if (options.invalid_field_as_null) {
        return nullable->append_nulls(1);
    } else {
        return false;
    }
}

bool NullableConverter::read_quoted_string(Column* column, const Slice& s, const Options& options) const {
    auto* nullable = down_cast<NullableColumn*>(column);
    auto* data = nullable->data_column_raw_ptr();

    if (s == "null") {
        return nullable->append_nulls(1);
    } else if (_base_converter->read_quoted_string(data, s, options)) {
        nullable->null_column_raw_ptr()->append(0);
        return true;
    } else if (options.invalid_field_as_null) {
        return nullable->append_nulls(1);
    } else {
        return false;
    }
}

} // namespace starrocks::csv
