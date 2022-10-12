// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "util/phmap/phmap.h"

namespace starrocks {
namespace vectorized {

using AggDataPtr = uint8_t*;
using Int32AggHashMap = phmap::flat_hash_map<int32_t, AggDataPtr>;
using Int32AggTwoLevelHashMap = phmap::parallel_flat_hash_map<int32_t, AggDataPtr>;

} // namespace vectorized
} // namespace starrocks
