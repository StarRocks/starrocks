// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <fmt/format.h>
#include <butil/iobuf.h>

namespace starrocks::starcache {

using IOBuf = butil::IOBuf;
using CacheId = uint64_t;
using CacheKey = std::string;

struct BlockId {
    uint8_t dir_index;
    uint32_t block_index;

    std::string str() const {
        std::string block_id = fmt::format("{}_{}", dir_index, block_index);
        return block_id;
    }
};

struct BlockKey {
    CacheId cache_id;
    uint32_t block_index;

    std::string str() const {
        std::string block_key = fmt::format("{}_{}", cache_id, block_index);
        return block_key;
    }
};

struct DirSpace {
    std::string path;
    size_t quota_bytes;
};

inline std::ostream& operator<<(std::ostream& os, const BlockKey& block_key) {
    os << block_key.cache_id << "_" << block_key.block_index;
    return os;
}

const size_t KB = 1024;
const size_t MB = KB * 1024;
const size_t GB = MB * 1024;
    
} // namespace starrocks::starcache
