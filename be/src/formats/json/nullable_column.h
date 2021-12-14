// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "binary_column.h"
#include "boolean_column.h"
#include "column/column.h"
#include "common/status.h"
#include "numeric_column.h"
#include "runtime/types.h"
#include "simdjson.h"

namespace starrocks::vectorized {

template <typename T>
Status add_nullable_numeric_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                   simdjson::ondemand::value& value, bool invalid_as_null);

Status add_nullable_binary_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                  simdjson::ondemand::value& value, bool invalid_as_null);

Status add_nullable_boolean_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                   simdjson::ondemand::value& value, bool invalid_as_null);

Status add_nullable_array_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                 simdjson::ondemand::value& value, bool invalid_as_null);

} // namespace starrocks::vectorized
