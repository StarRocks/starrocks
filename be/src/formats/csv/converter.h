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

#include <fmt/compile.h>
#include <fmt/format.h>
#include <ryu/ryu.h>

#include <limits>
#include <memory>
#include <type_traits>

#include "common/status.h"
#include "formats/csv/output_stream.h"
#include "runtime/decimalv2_value.h"
#include "runtime/time_types.h"
#include "types/date_value.hpp"
#include "types/timestamp_value.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {
struct TypeDescriptor;
} // namespace starrocks

namespace starrocks {

class Column;

namespace csv {

enum class ArrayFormatType {
    kDefault = 0,
    kHive,
};

class Converter {
public:
    struct Options {
        // When loading data, specifies whether to insert SQL NULL for invalid fields in an input file.
        bool invalid_field_as_null = true;

        // Exporting option.
        // If the bool_alpha format flag is true, bool values are exported by their textual
        // representation: either true or false, instead of integral values.
        // For importing, both the integral values and textual representation are supported.
        // TODO: user configurable.
        bool bool_alpha = true;

        // Here used to control array parse format.
        // Considering Hive array format is different from traditional array format,
        // so here we provide some variables to customize array format, and you can
        // also add variables to customize array format in the future.
        // If you decide to parse default array format, you don't need to change variable's value,
        // default value is enough.
        // Default array format like: [[1,2], [3, 4]]
        // Hive array format like: 1^C2^B3^4
        ArrayFormatType array_format_type = ArrayFormatType::kDefault;
        // [Only used in Hive now!]
        // Control hive array's element delimiter.
        char array_hive_collection_delimiter = '\002';
        // [Only used in Hive now!]
        // mapkey_delimiter is the separator between key and value in map.
        // For example, {"smith": age} mapkey_delimiter is ':', array_hive_mapkey_delimiter is
        // used to generate collection delimiter in Hive.
        char array_hive_mapkey_delimiter = '\003';
        // [Only used in Hive now!]
        // Control array nested level, used to generate collection delimiter in Hive.
        size_t array_hive_nested_level = 1;

        // type desc of the slot we are dealing with now.
        const TypeDescriptor* type_desc = nullptr;
    };

    virtual ~Converter() = default;

    virtual Status write_string(OutputStream* buff, const Column& column, size_t row_num,
                                const Options& options) const = 0;

    virtual Status write_quoted_string(OutputStream* buff, const Column& column, size_t row_num,
                                       const Options& options) const = 0;

    virtual bool read_string_for_adaptive_null_column(Column* column, Slice s, const Options& options) const {
        __builtin_unreachable();
        return true;
    }

    virtual bool read_string(Column* column, Slice s, const Options& options) const = 0;

    virtual bool read_quoted_string(Column* column, Slice s, const Options& options) const = 0;

protected:
    template <char quote>
    static inline bool remove_enclosing_quotes(Slice* s) {
        if (s->size < 2) {
            return false;
        }
        if ((*s)[0] != quote || (*s)[s->size - 1] != quote) {
            return false;
        }
        s->remove_prefix(1);
        s->remove_suffix(1);
        return true;
    }
};

std::unique_ptr<Converter> get_converter(const TypeDescriptor& type_desc, bool nullable);

} // namespace csv
} // namespace starrocks
