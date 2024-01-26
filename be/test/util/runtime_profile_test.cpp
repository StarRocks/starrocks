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
} // namespace starrocks
