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

#include "geo/geos_context.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

namespace starrocks {

TEST(GeosContextTest, ReentrantGeometryAndErrorLifecycle) {
    GeosContext context;
    ASSERT_TRUE(context.ready());
    GeosGeometryPtr point(GEOSGeom_createPointFromXY_r(context.get(), 3, 4), GeosGeometryDeleter{context.get()});
    ASSERT_NE(nullptr, point);
    double x = 0;
    double y = 0;
    EXPECT_EQ(1, GEOSGeomGetX_r(context.get(), point.get(), &x));
    EXPECT_EQ(1, GEOSGeomGetY_r(context.get(), point.get(), &y));
    EXPECT_DOUBLE_EQ(3, x);
    EXPECT_DOUBLE_EQ(4, y);

    auto* reader = GEOSWKBReader_create_r(context.get());
    ASSERT_NE(nullptr, reader);
    const unsigned char malformed[] = {1};
    context.clear_error();
    EXPECT_EQ(nullptr, GEOSWKBReader_read_r(context.get(), reader, malformed, sizeof(malformed)));
    EXPECT_FALSE(context.error("GEOS WKB read failed").ok());
    GEOSWKBReader_destroy_r(context.get(), reader);
}

TEST(GeosContextTest, IndependentWorkerContexts) {
    std::atomic<bool> passed = true;
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker] {
            GeosContext context;
            if (!context.ready()) {
                passed = false;
                return;
            }
            for (int iteration = 0; iteration < 20; ++iteration) {
                GeosGeometryPtr point(GEOSGeom_createPointFromXY_r(context.get(), worker, iteration),
                                      GeosGeometryDeleter{context.get()});
                double x = -1;
                if (point == nullptr || GEOSGeomGetX_r(context.get(), point.get(), &x) != 1 || x != worker) {
                    passed = false;
                    return;
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_TRUE(passed);
}

} // namespace starrocks
