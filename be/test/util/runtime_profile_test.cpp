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
    auto* merged_min_of_time1 = merged_profile->get_counter("__MIN_OF_time1");
    auto* merged_max_of_time1 = merged_profile->get_counter("__MAX_OF_time1");
    ASSERT_TRUE(merged_min_of_time1);
    ASSERT_TRUE(merged_max_of_time1);
    ASSERT_EQ(2000000000L, merged_min_of_time1->value());
    ASSERT_EQ(2000000000L, merged_max_of_time1->value());

    auto* merged_time2 = merged_profile->get_counter("time2");
    ASSERT_EQ(1000000000L, merged_time2->value());
    auto* merged_min_of_time2 = merged_profile->get_counter("__MIN_OF_time2");
    auto* merged_max_of_time2 = merged_profile->get_counter("__MAX_OF_time2");
    ASSERT_TRUE(merged_min_of_time2);
    ASSERT_TRUE(merged_max_of_time2);
    ASSERT_EQ(0, merged_min_of_time2->value());
    ASSERT_EQ(2000000000L, merged_max_of_time2->value());

    auto* merged_count1 = merged_profile->get_counter("count1");
    ASSERT_EQ(2, merged_count1->value());
    auto* merged_min_of_count1 = merged_profile->get_counter("__MIN_OF_count1");
    auto* merged_max_of_count1 = merged_profile->get_counter("__MAX_OF_count1");
    ASSERT_TRUE(merged_min_of_count1);
    ASSERT_TRUE(merged_max_of_count1);
    ASSERT_EQ(1, merged_min_of_count1->value());
    ASSERT_EQ(1, merged_max_of_count1->value());
}

TEST(TestRuntimeProfile, testMergeIsomorphicProfiles2) {
    std::shared_ptr<ObjectPool> obj_pool = std::make_shared<ObjectPool>();
    std::vector<RuntimeProfile*> profiles;

    auto profile1 = std::make_shared<RuntimeProfile>("profile");
    {
        auto* time1 = profile1->add_counter("time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS));
        time1->set(2000000000L);
        auto* min_of_time1 =
                profile1->add_child_counter("__MIN_OF_time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS), "time1");
        min_of_time1->set(1500000000L);
        auto* max_of_time1 =
                profile1->add_child_counter("__MAX_OF_time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS), "time1");
        max_of_time1->set(5000000000L);

        auto* count1 = profile1->add_counter("count1", TUnit::UNIT, create_strategy(TUnit::UNIT));
        count1->set(6L);
        auto* min_of_count1 =
                profile1->add_child_counter("__MIN_OF_count1", TUnit::UNIT, create_strategy(TUnit::UNIT), "count1");
        min_of_count1->set(1L);
        auto* max_of_count1 =
                profile1->add_child_counter("__MAX_OF_count1", TUnit::UNIT, create_strategy(TUnit::UNIT), "count1");
        max_of_count1->set(3L);

        profiles.push_back(profile1.get());
    }

    auto profile2 = std::make_shared<RuntimeProfile>("profile");
    {
        auto* time1 = profile2->add_counter("time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS));
        time1->set(3000000000L);
        auto* min_of_time1 =
                profile2->add_child_counter("__MIN_OF_time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS), "time1");
        min_of_time1->set(100000000L);
        auto* max_of_time1 =
                profile2->add_child_counter("__MAX_OF_time1", TUnit::TIME_NS, create_strategy(TUnit::TIME_NS), "time1");
        max_of_time1->set(4000000000L);

        auto* count1 = profile2->add_counter("count1", TUnit::UNIT, create_strategy(TUnit::UNIT));
        count1->set(15L);
        auto* min_of_count1 =
                profile2->add_child_counter("__MIN_OF_count1", TUnit::UNIT, create_strategy(TUnit::UNIT), "count1");
        min_of_count1->set(4L);
        auto* max_of_count1 =
                profile2->add_child_counter("__MAX_OF_count1", TUnit::UNIT, create_strategy(TUnit::UNIT), "count1");
        max_of_count1->set(6L);

        profiles.push_back(profile2.get());
    }

    auto* merged_profile = RuntimeProfile::merge_isomorphic_profiles(obj_pool.get(), profiles);
    auto* merged_time1 = merged_profile->get_counter("time1");
    ASSERT_EQ(2500000000L, merged_time1->value());
    auto* merged_min_of_time1 = merged_profile->get_counter("__MIN_OF_time1");
    auto* merged_max_of_time1 = merged_profile->get_counter("__MAX_OF_time1");
    ASSERT_TRUE(merged_min_of_time1);
    ASSERT_TRUE(merged_max_of_time1);
    ASSERT_EQ(100000000L, merged_min_of_time1->value());
    ASSERT_EQ(5000000000L, merged_max_of_time1->value());

    auto* merged_count1 = merged_profile->get_counter("count1");
    ASSERT_EQ(21, merged_count1->value());
    auto* merged_min_of_count1 = merged_profile->get_counter("__MIN_OF_count1");
    auto* merged_max_of_count1 = merged_profile->get_counter("__MAX_OF_count1");
    ASSERT_TRUE(merged_min_of_count1);
    ASSERT_TRUE(merged_max_of_count1);
    ASSERT_EQ(1, merged_min_of_count1->value());
    ASSERT_EQ(6, merged_max_of_count1->value());
}

TEST(TestRuntimeProfile, testProfileMergeStrategy) {
    std::shared_ptr<ObjectPool> obj_pool = std::make_shared<ObjectPool>();
    std::vector<RuntimeProfile*> profiles;

    TCounterStrategy strategy1;
    strategy1.aggregate_type = TCounterAggregateType::SUM;
    strategy1.merge_type = TCounterMergeType::MERGE_ALL;

    TCounterStrategy strategy2;
    strategy2.aggregate_type = TCounterAggregateType::SUM;
    strategy2.merge_type = TCounterMergeType::SKIP_ALL;

    TCounterStrategy strategy3;
    strategy3.aggregate_type = TCounterAggregateType::AVG;
    strategy3.merge_type = TCounterMergeType::SKIP_FIRST_MERGE;

    TCounterStrategy strategy4;
    strategy4.aggregate_type = TCounterAggregateType::AVG;
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
    auto* merged_min_of_time1 = merged_profile->get_counter("__MIN_OF_time1");
    auto* merged_max_of_time1 = merged_profile->get_counter("__MAX_OF_time1");
    ASSERT_TRUE(merged_min_of_time1);
    ASSERT_TRUE(merged_max_of_time1);
    ASSERT_EQ(1000000000L, merged_min_of_time1->value());
    ASSERT_EQ(1000000000L, merged_max_of_time1->value());

    auto* merged_time2 = merged_profile->get_counter("time2");
    ASSERT_EQ(2000000000L, merged_time2->value());
    auto* merged_min_of_time2 = merged_profile->get_counter("__MIN_OF_time2");
    auto* merged_max_of_time2 = merged_profile->get_counter("__MAX_OF_time2");
    ASSERT_FALSE(merged_min_of_time2);
    ASSERT_FALSE(merged_max_of_time2);

    auto* merged_count1 = merged_profile->get_counter("count1");
    ASSERT_EQ(6, merged_count1->value());
    auto* merged_min_of_count1 = merged_profile->get_counter("__MIN_OF_count1");
    auto* merged_max_of_count1 = merged_profile->get_counter("__MAX_OF_count1");
    ASSERT_FALSE(merged_min_of_count1);
    ASSERT_FALSE(merged_max_of_count1);

    auto* merged_count2 = merged_profile->get_counter("count2");
    ASSERT_EQ(8, merged_count2->value());
    auto* merged_min_of_count2 = merged_profile->get_counter("__MIN_OF_count2");
    auto* merged_max_of_count2 = merged_profile->get_counter("__MAX_OF_count2");
    ASSERT_TRUE(merged_min_of_count2);
    ASSERT_TRUE(merged_max_of_count2);
    ASSERT_EQ(8, merged_min_of_count2->value());
    ASSERT_EQ(8, merged_max_of_count2->value());
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

} // namespace starrocks
