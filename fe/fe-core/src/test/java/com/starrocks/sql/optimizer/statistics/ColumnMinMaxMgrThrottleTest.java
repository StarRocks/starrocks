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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.ColumnId;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Covers the per-column collection throttle (shouldCollect) that bounds how often min/max stats are
// re-collected via a [_META_] MetaScan. The collection itself needs a running executor, so only the
// throttle gate is unit-tested here; it is what decides whether a fresh MetaScan is triggered.
public class ColumnMinMaxMgrThrottleTest {
    private int savedInterval;

    @BeforeEach
    public void setUp() {
        savedInterval = Config.min_max_stats_collect_interval_sec;
    }

    @AfterEach
    public void tearDown() {
        Config.min_max_stats_collect_interval_sec = savedInterval;
    }

    private ColumnIdentifier col(long tableId, String name) {
        return new ColumnIdentifier(tableId, ColumnId.create(name));
    }

    @Test
    public void testThrottleWithinInterval() {
        Config.min_max_stats_collect_interval_sec = 3600;
        ColumnMinMaxMgr mgr = ColumnMinMaxMgr.getInstance();
        ColumnIdentifier id = col(9000001L, "c1");
        assertTrue(mgr.shouldCollect(id), "first trigger should be allowed");
        assertFalse(mgr.shouldCollect(id), "second trigger within the interval must be throttled");
        assertFalse(mgr.shouldCollect(id), "still throttled");
    }

    @Test
    public void testDifferentColumnsAreIndependent() {
        Config.min_max_stats_collect_interval_sec = 3600;
        ColumnMinMaxMgr mgr = ColumnMinMaxMgr.getInstance();
        assertTrue(mgr.shouldCollect(col(9000002L, "a")));
        assertTrue(mgr.shouldCollect(col(9000002L, "b")), "a different column is not throttled by another");
        assertFalse(mgr.shouldCollect(col(9000002L, "a")), "the same column stays throttled");
    }

    @Test
    public void testThrottlingDisabledWhenIntervalZero() {
        Config.min_max_stats_collect_interval_sec = 0;
        ColumnMinMaxMgr mgr = ColumnMinMaxMgr.getInstance();
        ColumnIdentifier id = col(9000003L, "c");
        assertTrue(mgr.shouldCollect(id));
        assertTrue(mgr.shouldCollect(id), "interval <= 0 disables throttling");
    }
}
