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

#include "formats/csv/converter.h"

namespace starrocks::csv {

class MapConverter final : public Converter {
public:
    explicit MapConverter(std::unique_ptr<Converter> key_converter, std::unique_ptr<Converter> value_converter)
            : _key_converter(std::move(key_converter)),
              _value_converter(std::move(value_converter)),
              _map_delimiter(','),
              _kv_delimiter(':') {}

    Status write_string(OutputStream* os, const Column& column, size_t row_num, const Options& options) const override;
    Status write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                               const Options& options) const override;
    bool read_string(Column* column, Slice s, const Options& options) const override;
    bool read_quoted_string(Column* column, Slice s, const Options& options) const override;

private:
    bool validate(const Slice& s) const;
    bool split_map_key_value(Slice s, std::vector<Slice>& keys, std::vector<Slice>& values) const;
    std::unique_ptr<Converter> _key_converter;
    std::unique_ptr<Converter> _value_converter;
    char _map_delimiter;
    char _kv_delimiter;
};

} // namespace starrocks::csv
