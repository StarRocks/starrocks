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

#include <cstring>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>

#include "base/phmap/phmap.h"
#include "column/column_hash.h"
#include "column/global_dict/config.h"
#include "column/global_dict/types_fwd_decl.h"
#include "runtime/mem_pool.h"

namespace starrocks {

// Builds GlobalDictMaps from string literals for tests.
//
// GlobalDictMap compares keys with memequal_padded, which may read up to SLICE_MEMEQUAL_OVERFLOW_PADDING bytes
// past each key, so keys must not point at string literals. Every key is copied into padded memory allocated from
// `pool`, which must outlive the returned maps.
class PaddedGlobalDictBuilder {
public:
    using Entries = std::initializer_list<std::pair<std::string_view, DictId>>;

    static GlobalDictMap build(MemPool* pool, Entries entries) {
        GlobalDictMap dict;
        for (const auto& [value, id] : entries) {
            uint8_t* pos = pool->allocate_with_reserve(value.size(), SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(pos, value.data(), value.size());
            dict.emplace(Slice(pos, value.size()), id);
        }
        return dict;
    }

    // Builds one dict per column name, all with the same version.
    static GlobalDictByNameMaps build_by_name(MemPool* pool,
                                              std::initializer_list<std::pair<std::string, Entries>> dicts,
                                              long version = 1) {
        GlobalDictByNameMaps result;
        for (const auto& [name, entries] : dicts) {
            result[name] = GlobalDictsWithVersion<GlobalDictMap>{build(pool, entries), version};
        }
        return result;
    }
};

} // namespace starrocks
