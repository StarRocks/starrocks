// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once

#include <string>
#include <unordered_map>
<<<<<<< HEAD
namespace starrocks {
namespace query_cache {
=======

#include "gen_cpp/Types_types.h"
namespace starrocks::query_cache {
>>>>>>> 23e80112c ([Enhancement] QueryCache support primary_key and unique_key (#13349))

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
} // namespace query_cache
} // namespace starrocks
