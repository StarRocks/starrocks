// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/datum.h"
#include "storage/types.h"
namespace starrocks::vectorized {

Status datum_from_string(Datum* dst, FieldType type, const std::string& str, MemPool* mem_pool);
Status datum_from_string(TypeInfo* type_info, Datum* dst, const std::string& str, MemPool* mem_pool);

std::string datum_to_string(const Datum& datum, FieldType type);
std::string datum_to_string(TypeInfo* type_info, const Datum& datum);

} // namespace starrocks::vectorized
