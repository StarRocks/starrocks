// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <simdjson.h>

#include "common/statusor.h"
#include "util/json.h"

namespace starrocks {

using SimdJsonValue = simdjson::ondemand::value;
using SimdJsonObject = simdjson::ondemand::object;

// Convert SIMD-JSON object/value to a JsonValue
StatusOr<JsonValue> convert_from_simdjson(SimdJsonValue value);
StatusOr<JsonValue> convert_from_simdjson(SimdJsonObject value);

} // namespace starrocks