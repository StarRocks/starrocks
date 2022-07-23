// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

namespace starrocks {

// default value of chunk_size, it's a value decided at compile time
constexpr const int DEFAULT_CHUNK_SIZE = 4096;

// Chunk size for some huge type(HLL, JSON)
constexpr inline int CHUNK_SIZE_FOR_HUGE_TYPE = 4096;

constexpr inline int NUM_LOCK_SHARD_LOG = 5;

} // namespace starrocks
