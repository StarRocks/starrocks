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

#include "data_sink/dictionary_cache/dictionary_cache_sink.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(DictionaryCacheSinkTest, PreservesPipelineOnlyLifecycle) {
    DictionaryCacheSink sink;
    TDataSink thrift_sink;
    RuntimeState* state = nullptr;

    ASSERT_TRUE(sink.init(thrift_sink, state).ok());
    ASSERT_TRUE(sink.prepare(state).ok());

    auto open_status = sink.open(state);
    EXPECT_TRUE(open_status.is_not_supported());
    EXPECT_EQ("dictionary cache only support pipeline engine", open_status.message());

    auto send_status = sink.send_chunk(state, nullptr);
    EXPECT_TRUE(send_status.is_not_supported());
    EXPECT_EQ("dictionary cache only support pipeline engine", send_status.message());
    EXPECT_EQ(nullptr, sink.profile());
}

} // namespace starrocks
