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

#include "compute_env/global_dict/fragment_dict_state.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {

TGlobalDict make_global_dict(int32_t column_id, int64_t version, std::vector<int32_t> ids,
                             std::vector<std::string> strings) {
    TGlobalDict dict;
    dict.__set_columnId(column_id);
    dict.__set_version(version);
    dict.__set_ids(std::move(ids));
    dict.__set_strings(std::move(strings));
    return dict;
}

} // namespace

TEST(FragmentDictStateTest, InitQueryAndLoadGlobalDicts) {
    RuntimeState state;
    state._obj_pool = std::make_shared<ObjectPool>();
    state._instance_mem_pool = std::make_unique<MemPool>();

    FragmentDictState fragment_dict_state;

    ASSERT_OK(
            fragment_dict_state.init_query_global_dict(&state, {make_global_dict(3, 11, {0, 1}, {"", "query-value"})}));
    ASSERT_OK(fragment_dict_state.init_load_global_dict(&state, {make_global_dict(5, 17, {0, 2}, {"", "load-value"})}));

    const auto& query_dicts = fragment_dict_state.query_global_dicts();
    ASSERT_EQ(1, query_dicts.size());
    ASSERT_NE(query_dicts.find(3), query_dicts.end());
    EXPECT_EQ("query-value", query_dicts.find(3)->second.second.find(1)->second.to_string());

    const auto& load_dicts = fragment_dict_state.load_global_dicts();
    ASSERT_EQ(1, load_dicts.size());
    ASSERT_NE(load_dicts.find(5), load_dicts.end());
    EXPECT_EQ("load-value", load_dicts.find(5)->second.second.find(2)->second.to_string());

    const auto& versions = fragment_dict_state.load_dict_versions();
    ASSERT_NE(versions.find(5), versions.end());
    EXPECT_EQ(17, versions.find(5)->second);

    fragment_dict_state.close(&state);
}

} // namespace starrocks
