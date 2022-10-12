// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>
#include <vector>

#include "util/raw_container.h"

namespace starrocks::vectorized {

// Bytes is a special vector<uint8_t> in which the internal memory is always allocated with an additional 16 bytes,
// to make life easier with 128 bit instructions.
typedef starrocks::raw::RawVectorPad16<uint8_t> Bytes;

} // namespace starrocks::vectorized
