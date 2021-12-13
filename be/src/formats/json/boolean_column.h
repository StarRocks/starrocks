// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "column/binary_column.h"
#include "column/column.h"
#include "common/status.h"
#include "numeric_column.h"
#include "runtime/types.h"
#include "simdjson.h"

namespace starrocks::vectorized {

static auto literal_0_slice{Slice{"0"}};
static auto literal_1_slice{Slice{"1"}};

Status add_boolean_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                         simdjson::ondemand::value& value) {
    auto binary_column = down_cast<BinaryColumn*>(column);

    try {
        bool ok = value.get_bool();
        if (ok) {
            binary_column->append(literal_1_slice);
        } else {
            binary_column->append(literal_0_slice);
        }
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as boolean, column=$0", name);
        return Status::DataQualityError(err_msg);
    }
}

} // namespace starrocks::vectorized
