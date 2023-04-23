// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "binary_column.h"
#include "column/column.h"
#include "common/status.h"
#include "numeric_column.h"
#include "runtime/types.h"
#include "simdjson.h"

namespace starrocks::vectorized {

Status add_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                           simdjson::ondemand::value* value, bool invalid_as_null);

Status add_nullable_column_by_json_object(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                          simdjson::ondemand::object* value, bool invalid_as_null);

Status add_adaptive_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                    simdjson::ondemand::value* value, bool invalid_as_null);

Status add_adaptive_nullable_column_by_json_object(Column* column, const TypeDescriptor& type_desc,
                                                   const std::string& name, simdjson::ondemand::object* value,
                                                   bool invalid_as_null);
} // namespace starrocks::vectorized
