// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <cstdint>

namespace orc {
void bit_unpack(const uint8_t* in, int fb, int64_t* data, int nums);
}
