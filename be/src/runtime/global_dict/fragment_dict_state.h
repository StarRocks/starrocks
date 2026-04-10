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

#include <map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/global_dict/parser.h"
#include "runtime/global_dict/types.h"

namespace starrocks {

class MemPool;
class RuntimeState;

class FragmentDictState {
public:
    using GlobalDictLists = std::vector<TGlobalDict>;

    const GlobalDictMaps& query_global_dicts() const { return _query_global_dicts; }
    GlobalDictMaps* mutable_query_global_dicts() { return &_query_global_dicts; }

    const GlobalDictMaps& load_global_dicts() const { return _load_global_dicts; }
    const phmap::flat_hash_map<uint32_t, int64_t>& load_dict_versions() const { return _load_dict_versions; }

    DictOptimizeParser* mutable_dict_optimize_parser() { return &_dict_optimize_parser; }

    Status init_query_global_dict(RuntimeState* runtime_state, const GlobalDictLists& global_dict_list);
    Status init_query_global_dict_exprs(RuntimeState* runtime_state, const std::map<int, TExpr>& exprs);
    Status init_load_global_dict(RuntimeState* runtime_state, const GlobalDictLists& global_dict_list);

    void close(RuntimeState* runtime_state) noexcept;

private:
    Status _build_global_dict(MemPool* mem_pool, const GlobalDictLists& global_dict_list, GlobalDictMaps* result,
                              phmap::flat_hash_map<uint32_t, int64_t>* column_id_to_version);

    GlobalDictMaps _query_global_dicts;
    GlobalDictMaps _load_global_dicts;
    phmap::flat_hash_map<uint32_t, int64_t> _load_dict_versions;
    DictOptimizeParser _dict_optimize_parser;
};

} // namespace starrocks
