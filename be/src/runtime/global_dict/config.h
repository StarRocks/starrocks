// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

// TODO: we need to change the dict type to int16 later
using DictId = int32_t;

constexpr auto LowCardDictType = TYPE_INT;
constexpr int DICT_DECODE_MAX_SIZE = 256;

} // namespace starrocks::vectorized
