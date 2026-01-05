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

package com.starrocks.statistic;

import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatisticsMetaManagerConfigTest {
    private long originalInterval;

    @BeforeEach
    public void setUp() {
        // Save original config values
        originalInterval = Config.statistics_meta_manager_interval_sec;
    }

    @AfterEach
    public void tearDown() {
        // Restore original config values
        Config.statistics_meta_manager_interval_sec = originalInterval;
    }

    @Test
    public void testConfigDefaults() {
        Assertions.assertEquals(60, Config.statistics_meta_manager_interval_sec);
    }

    @Test
    public void testManagerUsesConfigInterval() {
        Config.statistics_meta_manager_interval_sec = 120;
        StatisticsMetaManager manager = new StatisticsMetaManager();
        Assertions.assertEquals(120 * 1000L, manager.getInterval());
    }
}
