// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/runtime_profile.h"

#include <gtest/gtest.h>

#include "common/logging.h"
#include "gen_cpp/RuntimeProfile_types.h"

namespace starrocks {

static TCounterStrategy create_strategy(TUnit::type type) {
    return RuntimeProfile::Counter::create_strategy(type);
}

TEST(TestRuntimeProfile, testMergeIsomorphicProfiles1) {
    std::shared_ptr<ObjectPool> obj_pool = std::make_shared<ObjectPool>();
    std::vector<RuntimeProfile*> profiles;

    auto profile1 = std::make_shared<RuntimeProfile>("profile");
    {
        auto* time1 = profile1->add_counter("time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS));
        time1->set(2000000000L);
        auto* time2 = profile1->add_counter("time2", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS));
        time2->set(0L);

        auto* count1 = profile1->add_counter("count1", TUnit::UNIT, create_strategy(TUnit::UNIT));
        count1->set(1L);

        profiles.push_back(profile1.get());
    }

    auto profile2 = std::make_shared<RuntimeProfile>("profile");
    {
        auto* time1 = profile2->add_counter("time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS));
        time1->set(2000000000L);
        auto* time2 = profile2->add_counter("time2", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS));
        time2->set(2000000000L);

        auto* count1 = profile2->add_counter("count1", TUnit::UNIT, create_strategy(TUnit::UNIT));
        count1->set(1L);

        profiles.push_back(profile2.get());
    }

    auto* merged_profile = RuntimeProfile::merge_isomorphic_profiles(obj_pool.get(), profiles);

    auto* merged_time1 = merged_profile->get_counter("time1");
    ASSERT_EQ(2000000000L, merged_time1->value());
    ASSERT_EQ(2000000000L, merged_time1->min_value().value());
    ASSERT_EQ(2000000000L, merged_time1->max_value().value());

    auto* merged_time2 = merged_profile->get_counter("time2");
    ASSERT_EQ(1000000000L, merged_time2->value());
    ASSERT_EQ(0, merged_time2->min_value().value());
    ASSERT_EQ(2000000000L, merged_time2->max_value().value());

    auto* merged_count1 = merged_profile->get_counter("count1");
    ASSERT_EQ(2, merged_count1->value());
    ASSERT_EQ(1, merged_count1->min_value().value());
    ASSERT_EQ(1, merged_count1->min_value().value());
}

TEST(TestRuntimeProfile, testMergeIsomorphicProfiles2) {
    std::shared_ptr<ObjectPool> obj_pool = std::make_shared<ObjectPool>();
    std::vector<RuntimeProfile*> profiles;

    auto profile1 = std::make_shared<RuntimeProfile>("profile");
    {
        auto* time1 = profile1->add_counter("time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS));
        time1->set(2000000000L);
        time1->set_min(1500000000L);
        time1->set_max(5000000000L);

        auto* count1 = profile1->add_counter("count1", TUnit::UNIT, create_strategy(TUnit::UNIT));
        count1->set(6L);
        count1->set_min(1L);
        count1->set_max(3L);

        profiles.push_back(profile1.get());
    }

    auto profile2 = std::make_shared<RuntimeProfile>("profile");
    {
        auto* time1 = profile2->add_counter("time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS));
        time1->set(3000000000L);
        time1->set_min(100000000L);
        time1->set_max(4000000000L);

        auto* count1 = profile2->add_counter("count1", TUnit::UNIT, create_strategy(TUnit::UNIT));
        count1->set(15L);
        count1->set_min(4L);
        count1->set_max(6L);

        profiles.push_back(profile2.get());
    }

    auto* merged_profile = RuntimeProfile::merge_isomorphic_profiles(obj_pool.get(), profiles);
    auto* merged_time1 = merged_profile->get_counter("time1");
    ASSERT_EQ(2500000000L, merged_time1->value());
    ASSERT_EQ(100000000L, merged_time1->min_value().value());
    ASSERT_EQ(5000000000L, merged_time1->max_value().value());

    auto* merged_count1 = merged_profile->get_counter("count1");
    ASSERT_EQ(21, merged_count1->value());
    ASSERT_EQ(1, merged_count1->min_value().value());
    ASSERT_EQ(6, merged_count1->max_value().value());
}

TEST(TestRuntimeProfile, testProfileMergeStrategy) {
    auto do_test = [](TCounterAggregateType::type type1, TCounterAggregateType::type type2) {
        std::shared_ptr<ObjectPool> obj_pool = std::make_shared<ObjectPool>();
        std::vector<RuntimeProfile*> profiles;

        TCounterStrategy strategy1;
        strategy1.aggregate_type = type1;
        strategy1.merge_type = TCounterMergeType::MERGE_ALL;

        TCounterStrategy strategy2;
        strategy2.aggregate_type = type1;
        strategy2.merge_type = TCounterMergeType::SKIP_ALL;

        TCounterStrategy strategy3;
        strategy3.aggregate_type = type2;
        strategy3.merge_type = TCounterMergeType::SKIP_FIRST_MERGE;

        TCounterStrategy strategy4;
        strategy4.aggregate_type = type2;
        strategy4.merge_type = TCounterMergeType::SKIP_SECOND_MERGE;

        auto profile1 = std::make_shared<RuntimeProfile>("profile");
        {
            auto* time1 = profile1->add_counter("time1", TUnit::TIME_NS, strategy1);
            time1->set(1000000000L);

            auto* time2 = profile1->add_counter("time2", TUnit::TIME_NS, strategy2);
            time2->set(2000000000L);

            auto* count1 = profile1->add_counter("count1", TUnit::UNIT, strategy3);
            count1->set(6L);

            auto* count2 = profile1->add_counter("count2", TUnit::UNIT, strategy4);
            count2->set(8L);

            profiles.push_back(profile1.get());
        }

        auto profile2 = std::make_shared<RuntimeProfile>("profile");
        {
            auto* time1 = profile2->add_counter("time1", TUnit::TIME_NS, strategy1);
            time1->set(1000000000L);

            auto* time2 = profile2->add_counter("time2", TUnit::TIME_NS, strategy2);
            time2->set(3000000000L);

            auto* count1 = profile2->add_counter("count1", TUnit::UNIT, strategy3);
            count1->set(6L);

            auto* count2 = profile2->add_counter("count2", TUnit::UNIT, strategy4);
            count2->set(8L);
            profiles.push_back(profile2.get());
        }

        auto* merged_profile = RuntimeProfile::merge_isomorphic_profiles(obj_pool.get(), profiles);

        auto* merged_time1 = merged_profile->get_counter("time1");
        ASSERT_EQ(2000000000L, merged_time1->value());
        ASSERT_EQ(1000000000L, merged_time1->min_value().value());
        ASSERT_EQ(1000000000L, merged_time1->max_value().value());

        auto* merged_time2 = merged_profile->get_counter("time2");
        ASSERT_EQ(2000000000L, merged_time2->value());
        ASSERT_FALSE(merged_time2->min_value().has_value());
        ASSERT_FALSE(merged_time2->max_value().has_value());

        auto* merged_count1 = merged_profile->get_counter("count1");
        ASSERT_EQ(6, merged_count1->value());
        ASSERT_FALSE(merged_count1->min_value().has_value());
        ASSERT_FALSE(merged_count1->max_value().has_value());

        auto* merged_count2 = merged_profile->get_counter("count2");
        ASSERT_EQ(8, merged_count2->value());
        ASSERT_EQ(8, merged_count2->min_value().value());
        ASSERT_EQ(8, merged_count2->max_value().value());
    };

    do_test(TCounterAggregateType::SUM, TCounterAggregateType::AVG);
    do_test(TCounterAggregateType::SUM_AVG, TCounterAggregateType::AVG_SUM);
}

TEST(TestRuntimeProfile, testConflictInfoString) {
    std::shared_ptr<ObjectPool> obj_pool = std::make_shared<ObjectPool>();
    std::vector<RuntimeProfile*> profiles;
    auto profile1 = std::make_shared<RuntimeProfile>("profile");
    {
        profile1->add_info_string("key1", "value1");
        profiles.push_back(profile1.get());
    }
    auto profile2 = std::make_shared<RuntimeProfile>("profile");
    {
        profile2->add_info_string("key1", "value2");
        profiles.push_back(profile2.get());
    }
    auto profile3 = std::make_shared<RuntimeProfile>("profile");
    {
        profile3->add_info_string("key1", "value1");
        profiles.push_back(profile3.get());
    }
    auto profile4 = std::make_shared<RuntimeProfile>("profile");
    {
        profile4->add_info_string("key1__DUP(1)", "value3");
        profile4->add_info_string("key1", "value4");
        profiles.push_back(profile4.get());
    }
    auto profile5 = std::make_shared<RuntimeProfile>("profile");
    {
        profile5->add_info_string("key1", "value5");
        profile5->add_info_string("key1__DUP(1)", "value6");
        profiles.push_back(profile5.get());
    }

    auto* merged_profile = RuntimeProfile::merge_isomorphic_profiles(obj_pool.get(), profiles);
    const std::set<std::string> expected_values{"value1", "value2", "value3", "value4", "value5", "value6"};
    std::set<std::string> actual_values;
    ASSERT_TRUE(merged_profile->get_info_string("key1") != nullptr);
    actual_values.insert(*(merged_profile->get_info_string("key1")));
    ASSERT_TRUE(merged_profile->get_info_string("key1__DUP(0)") != nullptr);
    actual_values.insert(*(merged_profile->get_info_string("key1__DUP(0)")));
    ASSERT_TRUE(merged_profile->get_info_string("key1__DUP(1)") != nullptr);
    actual_values.insert(*(merged_profile->get_info_string("key1__DUP(1)")));
    ASSERT_TRUE(merged_profile->get_info_string("key1__DUP(2)") != nullptr);
    actual_values.insert(*(merged_profile->get_info_string("key1__DUP(2)")));
    ASSERT_TRUE(merged_profile->get_info_string("key1__DUP(3)") != nullptr);
    actual_values.insert(*(merged_profile->get_info_string("key1__DUP(3)")));
    ASSERT_TRUE(merged_profile->get_info_string("key1__DUP(4)") != nullptr);
    actual_values.insert(*(merged_profile->get_info_string("key1__DUP(4)")));

    ASSERT_EQ(expected_values, actual_values);
}

static void test_mass_conflict_info_string(int num) {
    std::shared_ptr<ObjectPool> obj_pool = std::make_shared<ObjectPool>();
    std::vector<std::shared_ptr<RuntimeProfile>> profile_ptrs;
    std::vector<RuntimeProfile*> profiles;
    for (int i = 1; i <= num; ++i) {
        auto profile = std::make_shared<RuntimeProfile>("profile");
        profile->add_info_string("key", std::to_string(i));
        profile_ptrs.push_back(profile);
        profiles.push_back(profile.get());
    }

    auto* merged_profile = RuntimeProfile::merge_isomorphic_profiles(obj_pool.get(), profiles);
    for (int i = 0; i < num - 1; ++i) {
        ASSERT_TRUE(merged_profile->get_info_string("key__DUP(" + std::to_string(i) + ")") != nullptr);
    }
}

TEST(TestRuntimeProfile, testMassConflictInfoString) {
    for (int i = 1; i <= 32; ++i) {
        test_mass_conflict_info_string(i);
    }
    test_mass_conflict_info_string(1024);
    test_mass_conflict_info_string(3267);
}

TEST(TestRuntimeProfile, testCopyCounterWithParent) {
    auto strategy_unit = create_strategy(TUnit::UNIT);
    auto strategy_time = create_strategy(TUnit::TIME_NS);
    auto src1 = std::make_shared<RuntimeProfile>("src profile1");

    auto* time1 = src1->add_counter("time1", TUnit::TIME_NS, strategy_time);
    time1->set(1L);
    auto* time2 = src1->add_child_counter("time2", TUnit::TIME_NS, strategy_time, "time1");
    time2->set(2L);
    auto* time3 = src1->add_child_counter("time3", TUnit::TIME_NS, strategy_time, "time2");
    time3->set(3L);
    auto* time4 = src1->add_counter("time4", TUnit::TIME_NS, strategy_time);
    time4->set(4L);

    auto src2 = std::make_shared<RuntimeProfile>("src profile2");
    auto* count1 = src2->add_counter("count1", TUnit::UNIT, strategy_unit);
    count1->set(11L);
    auto* count2 = src2->add_child_counter("count2", TUnit::UNIT, strategy_unit, "count1");
    count2->set(12L);
    auto* count3 = src2->add_child_counter("count3", TUnit::UNIT, strategy_unit, "count2");
    count3->set(13L);
    auto* count4 = src2->add_counter("count4", TUnit::UNIT, strategy_unit);
    count4->set(14L);

    auto dest = std::make_shared<RuntimeProfile>("destination");
    dest->add_counter("cascade1", TUnit::UNIT, strategy_unit);
    dest->add_child_counter("cascade2", TUnit::UNIT, strategy_unit, "cascade1");

    dest->copy_all_counters_from(src1.get(), "cascade1");
    dest->copy_all_counters_from(src2.get(), "cascade2");

    auto kv = dest->get_counter_pair("time1");
    ASSERT_TRUE(kv.first != nullptr);
    ASSERT_EQ(1L, kv.first->value());
    ASSERT_EQ("cascade1", kv.second);

    kv = dest->get_counter_pair("time2");
    ASSERT_TRUE(kv.first != nullptr);
    ASSERT_EQ(2L, kv.first->value());
    ASSERT_EQ("time1", kv.second);

    kv = dest->get_counter_pair("time3");
    ASSERT_TRUE(kv.first != nullptr);
    ASSERT_EQ(3L, kv.first->value());
    ASSERT_EQ("time2", kv.second);

    kv = dest->get_counter_pair("time4");
    ASSERT_TRUE(kv.first != nullptr);
    ASSERT_EQ(4L, kv.first->value());
    ASSERT_EQ("cascade1", kv.second);

    kv = dest->get_counter_pair("count1");
    ASSERT_TRUE(kv.first != nullptr);
    ASSERT_EQ(11L, kv.first->value());
    ASSERT_EQ("cascade2", kv.second);

    kv = dest->get_counter_pair("count2");
    ASSERT_TRUE(kv.first != nullptr);
    ASSERT_EQ(12L, kv.first->value());
    ASSERT_EQ("count1", kv.second);

    kv = dest->get_counter_pair("count3");
    ASSERT_TRUE(kv.first != nullptr);
    ASSERT_EQ(13L, kv.first->value());
    ASSERT_EQ("count2", kv.second);

    kv = dest->get_counter_pair("count4");
    ASSERT_TRUE(kv.first != nullptr);
    ASSERT_EQ(14L, kv.first->value());
    ASSERT_EQ("cascade2", kv.second);
}

TEST(TestRuntimeProfile, testRemoveCounter) {
    auto create_profile = []() -> std::shared_ptr<RuntimeProfile> {
        auto profile = std::make_shared<RuntimeProfile>("profile1");

        profile->add_counter("counter1", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
        profile->add_counter("counter2", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
        profile->add_counter("counter3", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));

        profile->add_child_counter("counter2-1", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT),
                                   "counter2");
        profile->add_child_counter("counter2-2", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT),
                                   "counter2");

        profile->add_child_counter("counter2-2-1", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT),
                                   "counter2-2");
        profile->add_child_counter("counter2-2-2", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT),
                                   "counter2-2");

        profile->add_child_counter("counter3-1", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT),
                                   "counter3");

        return profile;
    };

    auto check_countains = [](RuntimeProfile* profile, const std::vector<std::string>& names) {
        for (auto& name : names) {
            ASSERT_TRUE(profile->get_counter(name) != nullptr);
        }
    };

    {
        auto profile = create_profile();
        profile->remove_counter("counter1");
        check_countains(profile.get(), {"counter2", "counter3", "counter2-1", "counter2-2", "counter2-2-1",
                                        "counter2-2-2", "counter3-1"});
    }
    {
        auto profile = create_profile();
        profile->remove_counter("counter2");
        check_countains(profile.get(), {"counter1", "counter3", "counter3-1"});
    }
    {
        auto profile = create_profile();
        profile->remove_counter("counter2-1");
        check_countains(profile.get(), {"counter1", "counter2", "counter3", "counter2-2", "counter2-2-1",
                                        "counter2-2-2", "counter3-1"});
    }
    {
        auto profile = create_profile();
        profile->remove_counter("counter2-2");
        check_countains(profile.get(), {"counter1", "counter2", "counter3", "counter2-1", "counter3-1"});
    }
    {
        auto profile = create_profile();
        profile->remove_counter("counter2-2-1");
        check_countains(profile.get(),
                        {"counter1", "counter2", "counter3", "counter2-1", "counter2-2", "counter2-2-2", "counter3-1"});
    }
    {
        auto profile = create_profile();
        profile->remove_counter("counter2-2-2");
        check_countains(profile.get(),
                        {"counter1", "counter2", "counter3", "counter2-1", "counter2-2", "counter2-2-1", "counter3-1"});
    }
    {
        auto profile = create_profile();
        profile->remove_counter("counter3");
        check_countains(profile.get(),
                        {"counter1", "counter2", "counter2-1", "counter2-2", "counter2-2-1", "counter2-2-2"});
    }
    {
        auto profile = create_profile();
        profile->remove_counter("counter3-1");
        check_countains(profile.get(), {"counter1", "counter2", "counter2-1", "counter2-2", "counter2-2-1",
                                        "counter2-2-2", "counter3"});
    }
}

TEST(TestRuntimeProfile, testUpdateWithOldAndNewProfile) {
    auto profile = std::make_shared<RuntimeProfile>("parent-profile");
    auto* counter1 =
            profile->add_counter("counter1", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    auto* child_profile = profile->create_child("child-profile", true);
    auto* counter2 =
            child_profile->add_counter("counter2", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));

    ASSERT_EQ(0, profile->get_version());
    ASSERT_EQ(0, child_profile->get_version());
    COUNTER_UPDATE(counter1, 1);
    COUNTER_UPDATE(counter2, 2);
    ASSERT_EQ(1, counter1->value());
    ASSERT_EQ(2, counter2->value());

    // thrift profile whose parent and child profiles are both 1
    TRuntimeProfileTree tree;
    profile->to_thrift(&tree);
    ASSERT_EQ(2, tree.nodes.size());
    ASSERT_TRUE(tree.nodes[0].__isset.version);
    ASSERT_EQ(0, tree.nodes[0].version);
    ASSERT_TRUE(tree.nodes[1].__isset.version);
    ASSERT_EQ(0, tree.nodes[1].version);

    // update with new versions for both parent and child profile,
    // both should update success
    COUNTER_SET(counter1, int64_t(2));
    COUNTER_SET(counter2, int64_t(3));
    ASSERT_EQ(2, counter1->value());
    ASSERT_EQ(3, counter2->value());
    // make thrift profile versions newer
    tree.nodes[0].version = 1;
    tree.nodes[1].version = 1;
    profile->update(tree);
    ASSERT_EQ(1, counter1->value());
    ASSERT_EQ(2, counter2->value());
    ASSERT_EQ(1, profile->get_version());
    ASSERT_EQ(1, child_profile->get_version());

    // update with an old version for both parent profile, and a new
    // version for child profile, both should skip
    COUNTER_SET(counter1, int64_t(4));
    COUNTER_SET(counter2, int64_t(5));
    ASSERT_EQ(4, counter1->value());
    ASSERT_EQ(5, counter2->value());
    // make thrift parent older, and child newer
    tree.nodes[0].version = 0;
    tree.nodes[1].version = 2;
    // increase parent version to 2
    profile->inc_version();
    ASSERT_EQ(2, profile->get_version());
    ASSERT_EQ(1, child_profile->get_version());
    profile->update(tree);
    ASSERT_EQ(4, counter1->value());
    ASSERT_EQ(5, counter2->value());
    ASSERT_EQ(2, profile->get_version());
    ASSERT_EQ(1, child_profile->get_version());

    // update with a new version for parent profile, and an old
    // version for child profile, the parent should success, and
    // the child skip
    COUNTER_SET(counter1, int64_t(5));
    COUNTER_SET(counter2, int64_t(6));
    ASSERT_EQ(5, counter1->value());
    ASSERT_EQ(6, counter2->value());
    // make thrift parent equal, and child older
    tree.nodes[0].version = 2;
    tree.nodes[1].version = 0;
    // increase child version to 2
    child_profile->inc_version();
    ASSERT_EQ(2, profile->get_version());
    ASSERT_EQ(2, child_profile->get_version());
    profile->update(tree);
    ASSERT_EQ(1, counter1->value());
    ASSERT_EQ(6, counter2->value());
    ASSERT_EQ(2, profile->get_version());
    ASSERT_EQ(2, child_profile->get_version());

    // update with old versions for both parent and child profile,
    // both should skip
    COUNTER_SET(counter1, int64_t(7));
    COUNTER_SET(counter2, int64_t(8));
    ASSERT_EQ(7, counter1->value());
    ASSERT_EQ(8, counter2->value());
    // make thrift both parent and child older
    tree.nodes[0].version = 1;
    tree.nodes[1].version = 1;
    ASSERT_EQ(2, profile->get_version());
    ASSERT_EQ(2, child_profile->get_version());
    profile->update(tree);
    ASSERT_EQ(7, counter1->value());
    ASSERT_EQ(8, counter2->value());
    ASSERT_EQ(2, profile->get_version());
    ASSERT_EQ(2, child_profile->get_version());

    // If thrift not set version, should success
    tree.nodes[0].__isset.version = false;
    tree.nodes[1].__isset.version = false;
    profile->update(tree);
    ASSERT_EQ(1, counter1->value());
    ASSERT_EQ(2, counter2->value());
    ASSERT_EQ(2, profile->get_version());
    ASSERT_EQ(2, child_profile->get_version());
}

} // namespace starrocks
