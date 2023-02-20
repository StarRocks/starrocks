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

#include "common/tracer.h"

#include <gtest/gtest.h>

#include <memory>

namespace starrocks {

class TracerTest : public testing::Test {};

TEST_F(TracerTest, BasicTest) {
    EXPECT_FALSE(Tracer::Instance().is_enabled());
    auto span = Tracer::Instance().start_trace("test");
    EXPECT_FALSE(span->IsRecording());
    span->End();
    EXPECT_FALSE(span->IsRecording());
}

} // namespace starrocks
