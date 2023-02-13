// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <boost/align/is_aligned.hpp>
#include <butil/iobuf.h>
#include "star_cache/types.h"
#include "star_cache/common/config.h"

namespace starrocks::starcache {

uint64_t cachekey2id(const CacheKey& key);

inline int off2block(off_t offset) {
    return offset / config::FLAGS_block_size;
}

inline uint32_t block_lower(int block_index) {
    return block_index * config::FLAGS_block_size;
}

inline uint32_t block_upper(int block_index) {
    return (block_index + 1) * config::FLAGS_block_size - 1;
}

inline int off2slice(off_t offset) {
    return offset / config::FLAGS_slice_size;
}

inline uint32_t slice_lower(int slice_index) {
    return slice_index * config::FLAGS_slice_size;
}

inline uint32_t slice_upper(int slice_index) {
    return (slice_index + 1) * config::FLAGS_slice_size - 1;
}

inline uint64_t block_shard(const BlockKey& key) {
    return key.cache_id + key.block_index;
}

uint32_t crc32(const butil::IOBuf& buf);

inline uint32_t block_slice_count() {
    return 16;
    //static uint32_t slice_count = config::FLAGS_block_size / config::FLAGS_slice_size;
    //return slice_count;
}

inline uint32_t file_block_count() {
    static uint32_t block_count = config::FLAGS_block_file_size / config::FLAGS_block_size;
    return block_count;
}

inline bool mem_need_align(const void* data, size_t size) {
    if (config::FLAGS_enable_os_page_cache) {
        return false;
    }
    const size_t aligned_unit = config::FLAGS_io_align_unit_size;
    if (boost::alignment::is_aligned(data, aligned_unit) && (size % aligned_unit == 0)) {
        return false;
    }
    return true;
}

size_t align_buf(const butil::IOBuf& buf, void** aligned_buf);

} // namespace starrocks::starcache
