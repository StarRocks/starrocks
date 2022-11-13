// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <butil/iobuf.h>
#include "util/hash_util.hpp"
#include "common/config.h"
#include "star_cache/types.h"

namespace starrocks {

inline int off2block(off_t offset) {
    return offset / config::star_cache_block_size;
}

inline uint32_t block_lower(int block_index) {
    return block_index * config::star_cache_block_size;
}

inline uint32_t block_upper(int block_index) {
    return (block_index + 1) * config::star_cache_block_size - 1;
}

inline int off2slice(off_t offset) {
    return offset / config::star_cache_slice_size;
}

inline uint32_t slice_lower(int slice_index) {
    return slice_index * config::star_cache_slice_size;
}

inline uint32_t slice_upper(int slice_index) {
    return (slice_index + 1) * config::star_cache_slice_size - 1;
}

uint32_t crc32(const butil::IOBuf& buf);

inline uint64_t cachekey2id(const std::string& key) {
     return HashUtil::hash64(key.data(), key.size(), 0);
}

inline uint32_t block_slice_count() {
    static uint32_t slice_count = config::star_cache_block_size / config::star_cache_slice_size;
    return slice_count;
}

inline uint32_t file_block_count() {
    static uint32_t block_count = config::star_cache_block_file_size / config::star_cache_block_size;
    return block_count;
}
    
} // namespace starrocks
