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

#include "column/column_hash.h"
#include "runtime/global_dict/config.h"
#include "types/logical_type.h"
#include "util/phmap/phmap_fwd_decl.h"
#include "util/phmap/phmap_hash.h"
#include "util/slice.h"

namespace starrocks {

// slice -> global dict code
using GlobalDictMap = phmap::flat_hash_map<Slice, DictId, SliceHashWithSeed<PhmapSeed1>, SliceEqual>;
// global dict code -> slice
using RGlobalDictMap = phmap::flat_hash_map<DictId, Slice>;

using GlobalDictMapEntity = std::pair<GlobalDictMap, RGlobalDictMap>;
// column-id -> GlobalDictMap
using GlobalDictMaps = phmap::flat_hash_map<uint32_t, GlobalDictMapEntity>;

// column-name -> GlobalDictMap
// template to avoid incomplete type problems
template <class GlobalDictType>
struct GlobalDictsWithVersion {
    GlobalDictType dict;
    long version;
};

using GlobalDictByNameMaps = phmap::flat_hash_map<std::string, GlobalDictsWithVersion<GlobalDictMap>>;

using DictColumnsValidMap = phmap::flat_hash_map<std::string, bool, SliceHashWithSeed<PhmapSeed1>, SliceEqual>;

using ColumnIdToGlobalDictMap = phmap::flat_hash_map<uint32_t, GlobalDictMap*>;

} // namespace starrocks
