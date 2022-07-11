// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/location_provider.h"

#ifdef USE_STAROS

#include <gtest/gtest.h>

#include "service/staros_worker.h"
#include "storage/lake/starlet_location_provider.h"
#include "testutil/assert.h"

namespace starrocks {

extern std::shared_ptr<StarOSWorker> g_worker;

// TODO: fix broken UTs
class StarletGroupAssignerTest : public testing::Test {
public:
    StarletGroupAssignerTest() = default;
    ~StarletGroupAssignerTest() override = default;
    void SetUp() override {
        g_worker = std::make_shared<StarOSWorker>();
        _assigner = new lake::StarletLocationProvider();
    }
    void TearDown() override {
        delete _assigner;
        g_worker.reset();
    }

    lake::StarletLocationProvider* _assigner;
};

TEST_F(StarletGroupAssignerTest, starlet_group_test) {
    {
        staros::starlet::S3ObjectStoreInfo s3_store_info;
        s3_store_info.uri = "s3://starlet_test/1001";
        staros::starlet::ShardInfo shardInfo = {12345, {ObjectStoreType::S3, s3_store_info}, {}};
        (void)g_worker->add_shard(shardInfo);
    }
    {
        staros::starlet::S3ObjectStoreInfo s3_store_info;
        s3_store_info.uri = "s3://starlet_test/1001/";
        staros::starlet::ShardInfo shardInfo = {23456, {ObjectStoreType::S3, s3_store_info}, {}};
        (void)g_worker->add_shard(shardInfo);
    }

    ASSIGN_OR_ABORT(auto group_path, _assigner->get_group(12345));
    EXPECT_EQ(group_path, "s3://starlet_test/1001");

    ASSIGN_OR_ABORT(group_path, _assigner->get_group(23456));
    EXPECT_EQ(group_path, "s3://starlet_test/1001");

    auto res = _assigner->get_group(34567);
    EXPECT_TRUE(res.status().is_not_found());

    {
        staros::starlet::S3ObjectStoreInfo s3_store_info;
        s3_store_info.uri = "s3://starlet_test/1002/";
        staros::starlet::ShardInfo shardInfo = {34567, {ObjectStoreType::S3, s3_store_info}, {}};
        (void)g_worker->add_shard(shardInfo);
    }

    std::set<std::string> groups;
    EXPECT_OK(_assigner->list_group(&groups));
    EXPECT_EQ(2, groups.size());
    auto iter = std::find(groups.begin(), groups.end(), "s3://starlet_test/1001");
    EXPECT_TRUE(iter != groups.end());
    iter = std::find(groups.begin(), groups.end(), "s3://starlet_test/1002");
    EXPECT_TRUE(iter != groups.end());

    (void)g_worker->remove_shard(34567);
    res = _assigner->get_group(34567);
    EXPECT_TRUE(res.status().is_not_found());

    groups.clear();
    EXPECT_OK(_assigner->list_group(&groups));
    EXPECT_EQ(1, groups.size());
    iter = std::find(groups.begin(), groups.end(), "s3://starlet_test/1001");
    EXPECT_TRUE(iter != groups.end());
}

} // namespace starrocks
#endif // USE_STAROS
