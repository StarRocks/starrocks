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

#include "common/metrics/hooked_metric.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(HookedMetricTest, DoesNotRegisterHookWhenMetricRegistrationFails) {
    MetricRegistry registry("test");
    IntGauge existing(MetricUnit::NOUNIT);
    IntGauge duplicate(MetricUnit::NOUNIT);

    ASSERT_TRUE(registry.register_metric("metric_test", &existing));
    EXPECT_FALSE(register_hooked_metric(&registry, "metric_test", &duplicate, [] { return 7; }));

    registry.trigger_hook();
    EXPECT_EQ(0, duplicate.value());
    EXPECT_TRUE(registry.register_hook("metric_test", [] {}));
}

TEST(HookedMetricTest, DeregistersMetricWhenHookRegistrationFails) {
    MetricRegistry registry("test");
    IntGauge metric(MetricUnit::NOUNIT);

    ASSERT_TRUE(registry.register_hook("metric_test", [] {}));
    EXPECT_FALSE(register_hooked_metric(&registry, "metric_test", &metric, [] { return 7; }));
    EXPECT_EQ(nullptr, registry.get_metric("metric_test"));
    EXPECT_EQ(nullptr, metric._registry);
}

} // namespace starrocks
