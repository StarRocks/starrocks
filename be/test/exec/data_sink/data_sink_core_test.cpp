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

#include "exec/data_sink.h"

namespace starrocks {

class DataSinkCoreTestSink final : public DataSink {
public:
    Status open(RuntimeState* state) override { return Status::OK(); }
    RuntimeProfile* profile() override { return nullptr; }

    RuntimeState* runtime_state() const { return _runtime_state; }
    bool closed() const { return _closed; }
};

TEST(DataSinkCoreTest, DefaultPrepareStoresRuntimeState) {
    DataSinkCoreTestSink sink;
    auto* state = reinterpret_cast<RuntimeState*>(&sink);

    ASSERT_TRUE(sink.prepare(state).ok());
    EXPECT_EQ(state, sink.runtime_state());
}

TEST(DataSinkCoreTest, DefaultSendChunkIsNotSupported) {
    DataSinkCoreTestSink sink;

    EXPECT_TRUE(sink.send_chunk(nullptr, nullptr).is_not_supported());
}

TEST(DataSinkCoreTest, DefaultCloseMarksSinkClosed) {
    DataSinkCoreTestSink sink;

    ASSERT_TRUE(sink.close(nullptr, Status::OK()).ok());
    EXPECT_TRUE(sink.closed());
}

} // namespace starrocks
