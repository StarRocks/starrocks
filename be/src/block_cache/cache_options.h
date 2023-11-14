// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>
#include <vector>

namespace starrocks {

struct DirSpace {
    std::string path;
    size_t size;
};

struct CacheOptions {
    // basic
    size_t mem_space_size;
    std::vector<DirSpace> disk_spaces;
    std::string meta_path;

    // advanced
    size_t block_size;
    bool checksum;
    size_t max_concurrent_inserts;
    // The following options are only valid for cachelib engine currently
    size_t max_parcel_memory_mb;
    uint8_t lru_insertion_point;
};

} // namespace starrocks
