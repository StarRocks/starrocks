// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstddef>

namespace starrocks {

#define SMOOTHSTEP_NSTEPS 180
#define SMOOTHSTEP_BFP 24

size_t get_smoothstep_at(size_t index);

} // namespace starrocks
