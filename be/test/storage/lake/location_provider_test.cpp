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

#ifdef USE_STAROS
#include "storage/lake/location_provider.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "fs/fs_starlet.h"
#include "service/staros_worker.h"
#include "storage/lake/filenames.h"
#include "storage/lake/starlet_location_provider.h"
#include "testutil/assert.h"

namespace starrocks {
extern std::shared_ptr<StarOSWorker> g_worker;
} // namespace starrocks

namespace starrocks::lake {

class StarletLocationProviderTest : public testing::Test {
public:
    StarletLocationProviderTest() = default;
    ~StarletLocationProviderTest() override = default;
    void SetUp() override {
        g_worker = std::make_shared<StarOSWorker>();
        _provider = new lake::StarletLocationProvider();
    }
    void TearDown() override {
        delete _provider;
        g_worker.reset();
    }

    lake::StarletLocationProvider* _provider;
};

TEST_F(StarletLocationProviderTest, test_location) {
    auto location = _provider->root_location(12345);
    EXPECT_EQ(build_starlet_uri(12345, "/"), location);
}

} // namespace starrocks::lake
#endif // USE_STAROS
