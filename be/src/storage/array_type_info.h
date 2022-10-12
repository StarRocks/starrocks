// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/types.h"

namespace starrocks {

TypeInfoPtr get_array_type_info(const TypeInfoPtr& item_type);

const TypeInfoPtr& get_item_type_info(const TypeInfo* type_info);
} // namespace starrocks
