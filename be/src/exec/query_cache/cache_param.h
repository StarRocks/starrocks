// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>

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
    std::unordered_set<int32_t> cached_plan_node_ids;
};
} // namespace starrocks::query_cache
