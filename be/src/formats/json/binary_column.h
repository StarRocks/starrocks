// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "column/column.h"
#include "common/status.h"
#include "runtime/types.h"
#include "simdjson.h"

namespace starrocks::vectorized {

Status add_binary_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                         simdjson::ondemand::value* value);

Status add_native_json_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                              simdjson::ondemand::value* value);
Status add_native_json_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                              simdjson::ondemand::object* value);
Status add_binary_column_from_json_object(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                          simdjson::ondemand::object* value);

} // namespace starrocks::vectorized
