// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/csv/json_converter.h"

#include "column/json_column.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "types/date_value.hpp"

namespace starrocks::vectorized::csv {

Status JsonConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                   const Options& options) const {
    auto data_column = down_cast<const JsonColumn*>(&column);
    const std::vector<JsonValue>& pool = data_column->get_pool();
    return os->write(pool[row_num].get_slice());
}

Status JsonConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                          const Options& options) const {
    RETURN_IF_ERROR(os->write('"'));
    RETURN_IF_ERROR(write_string(os, column, row_num, options));
    return os->write('"');
}

bool JsonConverter::read_string(Column* column, Slice s, const Options& options) const {
    auto json = JsonValue::parse(s);
    if (json.ok()) {
        auto json_column = down_cast<JsonColumn*>(column);
        json_column->append(&json.value());
        return true;
    }
    return false;
}

bool JsonConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    if (!remove_enclosing_quotes<'"'>(&s)) {
        return false;
    }
    return read_string(column, s, options);
}

} // namespace starrocks::vectorized::csv
