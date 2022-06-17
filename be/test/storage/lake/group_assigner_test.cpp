// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/group_assigner.h"

#ifdef USE_STAROS

#include <gtest/gtest.h>

#include "service/staros_worker.h"
#include "storage/lake/starlet_group_assigner.h"
#include "testutil/assert.h"

namespace starrocks {

extern std::shared_ptr<StarOSWorker> g_worker;

class StarletGroupAssignerTest : public testing::Test {
public:
    StarletGroupAssignerTest() = default;
    ~StarletGroupAssignerTest() override = default;
    void SetUp() override {
        g_worker = std::make_shared<StarOSWorker>();
        _assigner = new lake::StarletGroupAssigner();
    }
    void TearDown() override {
        delete _assigner;
        g_worker.reset();
    }

    lake::StarletGroupAssigner* _assigner;
};

TEST_F(StarletGroupAssignerTest, starlet_group_test) {
    {
        staros::starlet::ShardInfo shardInfo = {12345, {""}};
        shardInfo.properties.emplace("storageGroup", "s3://starlet_test/1001");
        (void)g_worker->add_shard(shardInfo);
    }
    {
        staros::starlet::ShardInfo shardInfo = {23456, {""}};
        shardInfo.properties.emplace("storageGroup", "s3://starlet_test/1001");
        (void)g_worker->add_shard(shardInfo);
    }

    ASSIGN_OR_ABORT(auto group_path, _assigner->get_group(12345));
    EXPECT_EQ(group_path, "staros_s3://starlet_test/1001");

    ASSIGN_OR_ABORT(group_path, _assigner->get_group(23456));
    EXPECT_EQ(group_path, "staros_s3://starlet_test/1001");

    auto res = _assigner->get_group(34567);
    EXPECT_TRUE(res.status().is_not_found());

    {
        staros::starlet::ShardInfo shardInfo = {34567, {""}};
        shardInfo.properties.emplace("storageGroup", "s3://starlet_test/1002");
        (void)g_worker->add_shard(shardInfo);
    }

    std::set<std::string> groups;
    EXPECT_OK(_assigner->list_group(&groups));
    EXPECT_EQ(2, groups.size());
    auto iter = std::find(groups.begin(), groups.end(), "staros_s3://starlet_test/1001");
    EXPECT_TRUE(iter != groups.end());
    iter = std::find(groups.begin(), groups.end(), "staros_s3://starlet_test/1002");
    EXPECT_TRUE(iter != groups.end());

    (void)g_worker->remove_shard(34567);
    res = _assigner->get_group(34567);
    EXPECT_TRUE(res.status().is_not_found());

    groups.clear();
    EXPECT_OK(_assigner->list_group(&groups));
    EXPECT_EQ(1, groups.size());
    iter = std::find(groups.begin(), groups.end(), "staros_s3://starlet_test/1001");
    EXPECT_TRUE(iter != groups.end());
}

} // namespace starrocks
#endif // USE_STAROS
