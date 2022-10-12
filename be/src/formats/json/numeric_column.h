// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <limits>
#include <string>

#include "column/column.h"
#include "common/status.h"
#include "runtime/types.h"
#include "simdjson.h"

namespace starrocks::vectorized {

template <typename T>
Status add_numeric_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                          simdjson::ondemand::value* value);

} // namespace starrocks::vectorized
