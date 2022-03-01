// This file is made available under Elastic License 2.0.

#pragma once

#include "runtime/datetime_value.h"
#include "runtime/decimal_value.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
#include "runtime/large_int_value.h"
#include "storage/olap_common.h"

namespace starrocks {

PrimitiveType scalar_field_type_to_primitive_type(FieldType field_type);

} // namespace starrocks
