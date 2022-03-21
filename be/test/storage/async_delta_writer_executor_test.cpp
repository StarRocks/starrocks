// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/async_delta_writer_executor.h"

#include <gtest/gtest.h>

#include <vector>

#include "common/config.h"
#include "testutil/parallel_test.h"
#include "util/monotime.h"

namespace starrocks {

PARALLEL_TEST(AsyncDeltaWriterExecutorTest, test_overload) {
    AsyncDeltaWriterExecutor executor;
    ASSERT_TRUE(executor.init(1).ok());

    // submit overloading tasks.
    for (size_t i = 0; i < 4 * config::number_tablet_writer_threads; ++i) {
        ASSERT_EQ(0, executor.submit(
                             [](void*) -> void* {
                                 SleepFor(MonoDelta::FromSeconds(1));
                                 return nullptr;
                             },
                             nullptr));
    }
}

} // namespace starrocks