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

#include "util/failpoint/fail_point.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(FailPointTest, enable_disable_mode) {
    failpoint::FailPoint fp("test");

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp.setMode(trigger_mode);
    ASSERT_FALSE(fp.shouldFail());

    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    fp.setMode(trigger_mode);
    ASSERT_TRUE(fp.shouldFail());
}

TEST(FailPointTest, n_times_mode) {
    failpoint::FailPoint fp("test");

    int32_t n_times = 10;
    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE_N_TIMES);
    trigger_mode.set_n_times(n_times);
    fp.setMode(trigger_mode);

    for (int i = 0; i < n_times; i++) {
        ASSERT_TRUE(fp.shouldFail());
    }
    ASSERT_FALSE(fp.shouldFail());
}

TEST(FailPointTest, probability_mode) {
    failpoint::FailPoint fp("test");

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::PROBABILITY_ENABLE);
    trigger_mode.set_probability(0);
    fp.setMode(trigger_mode);
    ASSERT_FALSE(fp.shouldFail());

    trigger_mode.set_probability(1);
    fp.setMode(trigger_mode);
    ASSERT_TRUE(fp.shouldFail());
}

TEST(FailPointTest, scoped_fail_point) {
    failpoint::ScopedFailPoint sfp("test");
    failpoint::FailPointRegisterer sfpr(&sfp);

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    sfp.setMode(trigger_mode);

    ASSERT_FALSE(sfp.shouldFail());

    {
        failpoint::ScopedFailPointGuard g("test");
        ASSERT_TRUE(sfp.shouldFail());
    }

    ASSERT_FALSE(sfp.shouldFail());
}

TEST(FailPointTest, fp_demo) {
    DEFINE_FAIL_POINT(fp_test);

    auto test_func = [&]() {
        FAIL_POINT_TRIGGER_RETURN(fp_test, false);
        return true;
    };

    ASSERT_TRUE(test_func());

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    fpfp_test.setMode(trigger_mode);

    ASSERT_FALSE(test_func());
}

TEST(FailPointTest, sfp_demo) {
    DEFINE_SCOPED_FAIL_POINT(sfp_test);

    auto test_func = [&]() {
        FAIL_POINT_TRIGGER_EXECUTE(sfp_test, { return false; });
        return true;
    };

    ASSERT_TRUE(test_func());

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    sfpsfp_test.setMode(trigger_mode);

    ASSERT_TRUE(test_func());

    {
        FAIL_POINT_SCOPE(sfp_test);
        ASSERT_FALSE(test_func());
    }

    ASSERT_TRUE(test_func());
}

} // namespace starrocks
