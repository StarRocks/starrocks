// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>
#include <memory>
#include <utility>

#include "column/vectorized_fwd.h"
#include "runtime/global_dict/types.h"

namespace starrocks {
namespace vectorized {

std::pair<std::shared_ptr<NullableColumn>, std::vector<int32_t>> extract_column_with_codes(
        const GlobalDictMap& dict_map);

} // namespace vectorized
} // namespace starrocks
