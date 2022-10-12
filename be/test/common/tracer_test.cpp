// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
