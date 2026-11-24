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

#include <cstring>
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
    EXPECT_GE(state.instance_mem_pool()->total_allocated_bytes(),
              11 + SLICE_MEMEQUAL_OVERFLOW_PADDING + 10 + SLICE_MEMEQUAL_OVERFLOW_PADDING);

    const auto& query_dicts = fragment_dict_state.query_global_dicts();
    ASSERT_EQ(1, query_dicts.size());
    ASSERT_NE(query_dicts.find(3), query_dicts.end());
    Slice query_slice = query_dicts.find(3)->second.second.find(1)->second;
    EXPECT_EQ("query-value", query_slice.to_string());
    std::memset(const_cast<char*>(query_slice.data) + query_slice.size, 0x5A, SLICE_MEMEQUAL_OVERFLOW_PADDING);
    EXPECT_EQ(1, query_dicts.find(3)->second.first.find(query_slice)->second);

    const auto& load_dicts = fragment_dict_state.load_global_dicts();
    ASSERT_EQ(1, load_dicts.size());
    ASSERT_NE(load_dicts.find(5), load_dicts.end());
    Slice load_slice = load_dicts.find(5)->second.second.find(2)->second;
    EXPECT_EQ("load-value", load_slice.to_string());
    std::memset(const_cast<char*>(load_slice.data) + load_slice.size, 0x5A, SLICE_MEMEQUAL_OVERFLOW_PADDING);
    EXPECT_EQ(2, load_dicts.find(5)->second.first.find(load_slice)->second);

    const auto& versions = fragment_dict_state.load_dict_versions();
    ASSERT_NE(versions.find(5), versions.end());
    EXPECT_EQ(17, versions.find(5)->second);

    fragment_dict_state.close(&state);
}

TEST(FragmentDictStateTest, LoadIdentityIsOptionalAndDoesNotReplaceVersion) {
    RuntimeState state;
    state._obj_pool = std::make_shared<ObjectPool>();
    state._instance_mem_pool = std::make_unique<MemPool>();
    FragmentDictState dictionaries;
    auto legacy = make_global_dict(1, 100, {1}, {"A"});
    auto zero = make_global_dict(2, 200, {1}, {"B"});
    auto negative = make_global_dict(3, 300, {1}, {"C"});
    zero.__set_content_identity(0);
    negative.__set_content_identity(-7);
    ASSERT_OK(dictionaries.init_load_global_dict(&state, {legacy, zero, negative}));
    EXPECT_EQ(0, dictionaries.load_dict_content_identities().count(1));
    EXPECT_EQ(0, dictionaries.load_dict_content_identities().at(2));
    EXPECT_EQ(-7, dictionaries.load_dict_content_identities().at(3));
    EXPECT_EQ(100, dictionaries.load_dict_versions().at(1));
    EXPECT_EQ(200, dictionaries.load_dict_versions().at(2));
    EXPECT_EQ(300, dictionaries.load_dict_versions().at(3));
    dictionaries.close(&state);
}

} // namespace starrocks
