#pragma once

#include "column/column_hash.h"
#include "util/slice.h"

namespace starrocks {
namespace vectorized {
// slice -> global dict code
using GlobalDictMap = std::unordered_map<Slice, int, SliceHashWithSeed<PhmapSeed1>, SliceEqual>;
// global dict code -> slice
using RGlobalDictMap = std::unordered_map<int, Slice>;

using GlobalDictMapEntity = std::pair<GlobalDictMap, RGlobalDictMap>;
// column-id -> GlobalDictMap
using GlobalDictMaps = std::unordered_map<uint32_t, GlobalDictMapEntity>;

static inline std::unordered_map<uint32_t, GlobalDictMap*> EMPTY_GLOBAL_DICTMAPS;
} // namespace vectorized
} // namespace starrocks
