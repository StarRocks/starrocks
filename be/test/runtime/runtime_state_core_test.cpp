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

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>

#include "runtime/runtime_state.h"

namespace starrocks {

TEST(RuntimeStateCoreTest, FragmentDictStateDefaultNull) {
    RuntimeState state;
    EXPECT_EQ(nullptr, state.fragment_dict_state());
}

TEST(RuntimeStateCoreTest, FragmentDictStateSetGet) {
    RuntimeState state;
    auto* ptr = reinterpret_cast<FragmentDictState*>(static_cast<uintptr_t>(0x1234));
    state.set_fragment_dict_state(ptr);
    EXPECT_EQ(ptr, state.fragment_dict_state());
    const auto& const_state = state;
    EXPECT_EQ(ptr, const_state.fragment_dict_state());
}

TEST(RuntimeStateCoreTest, SetFragmentCtxKeepsFragmentDictStateExplicit) {
    EXPECT_EXIT(
            [] {
                RuntimeState state;
                auto* fragment_ctx = reinterpret_cast<pipeline::FragmentContext*>(static_cast<uintptr_t>(0x1234));
                auto* fragment_dict_state = reinterpret_cast<FragmentDictState*>(static_cast<uintptr_t>(0x5678));
                state.set_fragment_dict_state(fragment_dict_state);
                state.set_fragment_ctx(fragment_ctx);
                if (state.fragment_ctx() != fragment_ctx || state.fragment_dict_state() != fragment_dict_state) {
                    std::_Exit(1);
                }
                std::_Exit(0);
            }(),
            ::testing::ExitedWithCode(0), "");
}

} // namespace starrocks
