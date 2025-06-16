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
#include "storage/lake/starlet_location_provider.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fs/fs_starlet.h"
#include "service/staros_worker.h"
#include "storage/lake/filenames.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/defer_op.h"

namespace starrocks {
extern std::shared_ptr<StarOSWorker> g_worker;
} // namespace starrocks

namespace starrocks::lake {

class MockStarOSWorker : public StarOSWorker {
public:
    MOCK_METHOD((absl::StatusOr<staros::starlet::ShardInfo>), _fetch_shard_info_from_remote,
                (staros::starlet::ShardId id));
};

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

    LocationProvider* base_provider = _provider;
    location = base_provider->tablet_initial_metadata_location(12345);
    std::string_view filename = basename(location);
    EXPECT_TRUE(is_tablet_initial_metadata(filename));
}

TEST_F(StarletLocationProviderTest, test_get_real_location) {
    auto tablet_id = next_id();
    auto shard_info = staros::starlet::ShardInfo{};
    auto root_path = std::string{"/root/path/for/test"};
    shard_info.id = tablet_id;
    shard_info.path_info.set_full_path(root_path);

    // preserve original g_worker value, and reset it to our MockedWorker
    auto backup_worker = g_worker;
    auto defer = DeferOp([backup_worker] { g_worker = backup_worker; });
    g_worker.reset(new MockStarOSWorker());

    // set mock function excepted call
    auto worker = dynamic_cast<MockStarOSWorker*>(g_worker.get());
    EXPECT_CALL(*worker, _fetch_shard_info_from_remote(tablet_id)).WillRepeatedly(::testing::Return(shard_info));

    // fire the testing
    auto root = _provider->root_location(tablet_id);
    ASSIGN_OR_ABORT(auto real_path, _provider->real_location(root));
    EXPECT_EQ("/root/path/for/test/", real_path);

    EXPECT_EQ("/root/path/for/test/abc", _provider->real_location(fmt::format("staros://{}/abc", tablet_id)).value());

    shard_info.path_info.set_full_path("/root/path/for/test/");
    EXPECT_CALL(*worker, _fetch_shard_info_from_remote(tablet_id)).WillRepeatedly(::testing::Return(shard_info));

    EXPECT_EQ("/root/path/for/test/abc", _provider->real_location(fmt::format("staros://{}/abc", tablet_id)).value());
}
} // namespace starrocks::lake
#endif // USE_STAROS
