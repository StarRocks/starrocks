// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

struct CompatibleGlobalDictTraits {
    static constexpr int compatible_version = 3;
    static constexpr auto LowCardDictType = TYPE_INT;
    using LowCardDictColumn = vectorized::Int32Column;
};

struct GlobalDictTraits {
    static constexpr auto LowCardDictType = TYPE_SMALLINT;
    using LowCardDictColumn = vectorized::Int16Column;
};

using DictIdType = int16_t;
static constexpr int DICT_DECODE_MAX_SIZE = 256;

} // namespace starrocks::vectorized
#define DISPATCH_DICT(version, caller, ...)                                                    \
    [&]() {                                                                                    \
        if (version > starrocks::vectorized::CompatibleGlobalDictTraits::compatible_version) { \
            return caller<starrocks::vectorized::GlobalDictTraits>(__VA_ARGS__);               \
        } else {                                                                               \
            return caller<starrocks::vectorized::CompatibleGlobalDictTraits>(__VA_ARGS__);     \
        }                                                                                      \
    }()
