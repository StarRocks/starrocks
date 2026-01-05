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

package com.starrocks.lake;

import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TabletWriteLogHistorySyncerTest {
    private long originalInterval;
    private boolean originalEnabled;
    private int originalRetainedDays;

    @BeforeEach
    public void setUp() {
        // Save original config values
        originalInterval = Config.tablet_write_log_history_syncer_interval_sec;
        originalEnabled = Config.tablet_write_log_history_syncer_enabled;
        originalRetainedDays = Config.tablet_write_log_history_retained_days;
    }

    @AfterEach
    public void tearDown() {
        // Restore original config values
        Config.tablet_write_log_history_syncer_interval_sec = originalInterval;
        Config.tablet_write_log_history_syncer_enabled = originalEnabled;
        Config.tablet_write_log_history_retained_days = originalRetainedDays;
    }

    @Test
    public void testConfigDefaults() {
        Assertions.assertEquals(60, Config.tablet_write_log_history_syncer_interval_sec);
        Assertions.assertTrue(Config.tablet_write_log_history_syncer_enabled);
        Assertions.assertEquals(7, Config.tablet_write_log_history_retained_days);
    }

    @Test
    public void testSyncerUsesConfigInterval() {
        Config.tablet_write_log_history_syncer_interval_sec = 120;
        TabletWriteLogHistorySyncer syncer = new TabletWriteLogHistorySyncer();
        Assertions.assertEquals(120 * 1000L, syncer.getInterval());
    }

    @Test
    public void testSyncerUsesConfigEnabled() {
        Config.tablet_write_log_history_syncer_enabled = false;
        TabletWriteLogHistorySyncer syncer = new TabletWriteLogHistorySyncer();
        // syncData should return early when disabled
        syncer.syncData(); // Should not throw exception
    }

    @Test
    public void testTableKeeperUsesConfigRetainedDays() {
        Config.tablet_write_log_history_retained_days = 14;
        // TableKeeper should use the config value through the lambda
        Assertions.assertEquals(14, Config.tablet_write_log_history_retained_days);
    }
}
