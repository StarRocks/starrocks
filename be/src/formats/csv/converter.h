// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <fmt/compile.h>
#include <fmt/format.h>
#include <ryu/ryu.h>

#include <limits>
#include <memory>
#include <type_traits>

#include "common/status.h"
#include "formats/csv/output_stream.h"
#include "runtime/date_value.hpp"
#include "runtime/decimalv2_value.h"
#include "runtime/timestamp_value.h"
#include "runtime/vectorized/time_types.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {
struct TypeDescriptor;
} // namespace starrocks

namespace starrocks::vectorized {

class Column;

namespace csv {

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

        // type desc of the slot we are dealing with now.
        const TypeDescriptor* type_desc = nullptr;
    };

    virtual ~Converter() = default;

    virtual Status write_string(OutputStream* buff, const Column& column, size_t row_num,
                                const Options& options) const = 0;

    virtual Status write_quoted_string(OutputStream* buff, const Column& column, size_t row_num,
                                       const Options& options) const = 0;

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
} // namespace starrocks::vectorized