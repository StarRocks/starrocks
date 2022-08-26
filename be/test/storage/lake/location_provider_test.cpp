// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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

    location = _provider->tablet_metadata_location(12345, 1);
    EXPECT_EQ(build_starlet_uri(12345, tablet_metadata_filename(12345, 1)), location);

    location = _provider->txn_log_location(12345, 45678);
    EXPECT_EQ(build_starlet_uri(12345, txn_log_filename(12345, 45678)), location);

    location = _provider->txn_vlog_location(12345, 10);
    EXPECT_EQ(build_starlet_uri(12345, txn_vlog_filename(12345, 10)), location);

    location = _provider->segment_location(12345, "c805dab9-4048-4909-8239-6d5431989044.dat");
    EXPECT_EQ(build_starlet_uri(12345, "c805dab9-4048-4909-8239-6d5431989044.dat"), location);

    std::set<std::string> roots;
    auto st = _provider->list_root_locations(&roots);
    EXPECT_TRUE(st.ok());
    // TODO: mock g_worker to inject shard info
    EXPECT_TRUE(roots.empty());
}

} // namespace starrocks::lake
#endif // USE_STAROS
