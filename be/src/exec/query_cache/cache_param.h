// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once

#include <string>
#include <unordered_map>

#include "gen_cpp/Types_types.h"
namespace starrocks::query_cache {

using CacheKeyPrefixMap = std::unordered_map<int64_t, std::string>;
using SlotRemapping = std::unordered_map<int32_t, int32_t>;
struct CacheParam {
    int num_lanes;
    int32_t plan_node_id;
    std::string digest;
    CacheKeyPrefixMap cache_key_prefixes;
    SlotRemapping slot_remapping;
    SlotRemapping reverse_slot_remapping;
    bool force_populate;
    size_t entry_max_bytes;
    size_t entry_max_rows;
    bool can_use_multiversion;
    TKeysType::type keys_type;
};
} // namespace starrocks::query_cache
