// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

namespace starrocks {

// default value of chunk_size, it's a value decided at compile time
constexpr const int DEFAULT_CHUNK_SIZE = 4096;

// Chunk size for some huge type(HLL, JSON)
constexpr inline int CHUNK_SIZE_FOR_HUGE_TYPE = 4096;

// Lock is sharded into 32 shards
constexpr int NUM_LOCK_SHARD_LOG = 5;
constexpr int NUM_LOCK_SHARD = 1 << NUM_LOCK_SHARD_LOG;

} // namespace starrocks
