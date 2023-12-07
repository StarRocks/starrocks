// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column_hash.h"
#include "runtime/global_dict/config.h"
#include "runtime/primitive_type.h"
#include "util/phmap/phmap_fwd_decl.h"
#include "util/phmap/phmap_hash.h"
#include "util/slice.h"

namespace starrocks {
namespace vectorized {

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

} // namespace vectorized
} // namespace starrocks
