// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#include "common/tracer.h"

#include <gtest/gtest.h>

#include <memory>

namespace starrocks {

class TracerTest : public testing::Test {};

TEST_F(TracerTest, BasicTest) {
    std::unique_ptr<Tracer> tracer = std::make_unique<Tracer>();
    auto span = tracer->start_trace("test");
    EXPECT_TRUE(span->IsRecording());
    span->End();
    EXPECT_FALSE(span->IsRecording());
    tracer->shutdown();
}

} // namespace starrocks
