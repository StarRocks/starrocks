// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once

#include <string>
#include <unordered_map>
namespace starrocks {
namespace query_cache {

using CacheKeySuffixMap = std::unordered_map<int64_t, std::string>;
using SlotRemapping = std::unordered_map<int32_t, int32_t>;
struct CacheParam {
    int num_lanes;
    int32_t plan_node_id;
    std::string digest;
    CacheKeySuffixMap cache_key_prefixes;
    SlotRemapping slot_remapping;
    SlotRemapping reverse_slot_remapping;
    bool force_populate;
    size_t entry_max_bytes;
    size_t entry_max_rows;
};
} // namespace query_cache
} // namespace starrocks
