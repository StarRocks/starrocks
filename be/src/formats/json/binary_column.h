// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "column/column.h"
#include "common/status.h"
#include "runtime/types.h"
#include "simdjson.h"

namespace starrocks::vectorized {

Status add_binary_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                         simdjson::ondemand::value* value);

} // namespace starrocks::vectorized
