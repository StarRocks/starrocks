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

#include "column/column.h"
#include "formats/csv/converter.h"

namespace starrocks::csv {

class MapConverter final : public Converter {
public:
    explicit MapConverter(std::unique_ptr<Converter> key_converter, std::unique_ptr<Converter> value_converter)
            : _key_converter(std::move(key_converter)), _value_converter(std::move(value_converter)) {}

    Status write_string(formats::FormattedOutputStream* os, const Column& column, size_t row_num,
                        const Options& options) const override;
    Status write_quoted_string(formats::FormattedOutputStream* os, const Column& column, size_t row_num,
                               const Options& options) const override;
    bool read_string(Column* column, const Slice& s, const Options& options) const override;
    bool read_quoted_string(Column* column, const Slice& s, const Options& options) const override;
    bool consumes_raw_bytes() const override { return true; }

private:
    bool validate(const Slice& s) const;
    bool split_map_key_value(Slice s, std::vector<Slice>& keys, std::vector<Slice>& values) const;
    // Hive text map (LazyMap): no braces or quotes; entries split on this nesting
    // level's separator and each entry splits at the first occurrence of the next
    // level's separator (top level: collection delimiter / mapkey delimiter).
    bool read_hive_map(Column* column, const Slice& s, const Options& options) const;
    std::unique_ptr<Converter> _key_converter;
    std::unique_ptr<Converter> _value_converter;
    // read_hive_map's scratch column: all of a map field's keys are CONVERTED into it
    // first, so duplicate keys can be detected on the converted values (raw slices are
    // not comparable identities under ESCAPED BY -- "\a" and "a" unescape to the same
    // logical key). Lazily cloned from the real keys column and reused across rows;
    // safe for the same reason as NullableConverter::_unescape_buf (one converter per
    // column position, invoked sequentially per row, never reentrant).
    mutable MutableColumnPtr _tmp_keys;
    char _map_delimiter{','};
    char _kv_delimiter{':'};
};

} // namespace starrocks::csv
