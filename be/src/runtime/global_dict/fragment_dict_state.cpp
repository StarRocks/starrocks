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

#include "runtime/global_dict/fragment_dict_state.h"

#include <cstring>

#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace starrocks {

Status FragmentDictState::init_query_global_dict(RuntimeState* runtime_state, const GlobalDictLists& global_dict_list) {
    RETURN_IF_ERROR(
            _build_global_dict(runtime_state->instance_mem_pool(), global_dict_list, &_query_global_dicts, nullptr));
    _dict_optimize_parser.set_mutable_dict_maps(&_query_global_dicts);
    return Status::OK();
}

Status FragmentDictState::init_query_global_dict_exprs(RuntimeState* runtime_state, const std::map<int, TExpr>& exprs) {
    return _dict_optimize_parser.init_dict_exprs(runtime_state, exprs);
}

Status FragmentDictState::init_load_global_dict(RuntimeState* runtime_state, const GlobalDictLists& global_dict_list) {
    return _build_global_dict(runtime_state->instance_mem_pool(), global_dict_list, &_load_global_dicts,
                              &_load_dict_versions);
}

void FragmentDictState::close(RuntimeState* runtime_state) noexcept {
    _dict_optimize_parser.close(runtime_state);
}

Status FragmentDictState::_build_global_dict(MemPool* mem_pool, const GlobalDictLists& global_dict_list,
                                             GlobalDictMaps* result,
                                             phmap::flat_hash_map<uint32_t, int64_t>* column_id_to_version) {
    for (const auto& global_dict : global_dict_list) {
        DCHECK_EQ(global_dict.ids.size(), global_dict.strings.size());
        GlobalDictMap dict_map;
        RGlobalDictMap rdict_map;
        int dict_sz = global_dict.ids.size();
        for (int i = 0; i < dict_sz; ++i) {
            const std::string& dict_key = global_dict.strings[i];
            auto* data = mem_pool->allocate(dict_key.size());
            RETURN_IF_UNLIKELY_NULL(data, Status::MemoryAllocFailed("alloc mem for global dict failed"));
            memcpy(data, dict_key.data(), dict_key.size());
            Slice slice(data, dict_key.size());
            dict_map.emplace(slice, global_dict.ids[i]);
            rdict_map.emplace(global_dict.ids[i], slice);
        }
        result->emplace(uint32_t(global_dict.columnId), std::make_pair(std::move(dict_map), std::move(rdict_map)));
        if (column_id_to_version != nullptr) {
            column_id_to_version->emplace(uint32_t(global_dict.columnId), global_dict.version);
        }
    }
    return Status::OK();
}

} // namespace starrocks
