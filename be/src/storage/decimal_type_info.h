// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/types.h"

namespace starrocks {

TypeInfoPtr get_decimal_type_info(FieldType type, int precision, int scale);

std::string get_decimal_zone_map_string(TypeInfo* type_info, const char* value);

} // namespace starrocks
