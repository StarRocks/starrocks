// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/datum.h"
#include "storage/types.h"
namespace starrocks::vectorized {
Status datum_from_string(TypeInfo* type_info, Datum* dst, const std::string& str, MemPool* mem_pool);

std::string datum_to_string(TypeInfo* type_info, const Datum& datum);

Status convert_datum(TypeInfo* src_type_info, const Datum& src_datum, TypeInfo* dst_type_info, Datum& dst_datum);

} // namespace starrocks::vectorized