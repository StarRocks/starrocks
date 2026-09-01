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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CacheDictManagerThrashGuardTest {

    // A table id unlikely to collide with anything else touching the shared static state.
    private static final long TABLE_ID = 987654321L;

    private boolean savedEnabled;
    private int savedWindow;
    private int savedThreshold;

    @BeforeEach
    public void setUp() {
        savedEnabled = Config.enable_dict_thrash_guard;
        savedWindow = Config.dict_thrash_guard_window_sec;
        savedThreshold = Config.dict_thrash_guard_threshold;
        clearStatics();
    }

    @AfterEach
    public void tearDown() {
        Config.enable_dict_thrash_guard = savedEnabled;
        Config.dict_thrash_guard_window_sec = savedWindow;
        Config.dict_thrash_guard_threshold = savedThreshold;
        clearStatics();
    }

    @SuppressWarnings("unchecked")
    private Set<ColumnIdentifier> noDictColumns() {
        return (Set<ColumnIdentifier>) staticField("NO_DICT_STRING_COLUMNS");
    }

    @SuppressWarnings("unchecked")
    private Map<ColumnIdentifier, ?> invalidationHistory() {
        com.github.benmanes.caffeine.cache.Cache<ColumnIdentifier, ?> cache =
                (com.github.benmanes.caffeine.cache.Cache<ColumnIdentifier, ?>) staticField("DICT_INVALIDATION_HISTORY");
        return cache.asMap();
    }

    private static Object staticField(String name) {
        try {
            java.lang.reflect.Field f = CacheDictManager.class.getDeclaredField(name);
            f.setAccessible(true);
            return f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void clearStatics() {
        noDictColumns().removeIf(c -> c.getTableId() == TABLE_ID);
        invalidationHistory().keySet().removeIf(c -> c.getTableId() == TABLE_ID);
    }

    private ColumnIdentifier col(String name) {
        return new ColumnIdentifier(TABLE_ID, ColumnId.create(name));
    }

    // Drives the guard's counting logic (recordInvalidationAndCheckThreshold) and, on trip, applies the
    // same in-memory forbid the production path does. This avoids constructing an OlapTable just to test
    // the threshold behaviour (the persist-to-table-property side is exercised separately/manually).
    private void invalidate(CacheDictManager mgr, ColumnIdentifier id, int times) {
        for (int i = 0; i < times; i++) {
            boolean tripped = Deencapsulation.invoke(mgr, "recordInvalidationAndCheckThreshold", id);
            if (tripped) {
                noDictColumns().add(id);
                invalidationHistory().remove(id);
            }
        }
    }

    @Test
    public void testForbidsAfterThreshold() {
        Config.enable_dict_thrash_guard = true;
        Config.dict_thrash_guard_window_sec = 3600;
        Config.dict_thrash_guard_threshold = 5;
        CacheDictManager mgr = CacheDictManager.getInstance();
        ColumnIdentifier id = col("rolling_col");

        invalidate(mgr, id, 4);
        assertFalse(noDictColumns().contains(id), "should not be forbidden below threshold");

        invalidate(mgr, id, 1); // reaches 5
        assertTrue(noDictColumns().contains(id), "should be forbidden once threshold is reached");
        // history is dropped once forbidden
        assertFalse(invalidationHistory().containsKey(id));
    }

    @Test
    public void testDisabledGuardNeverForbids() {
        Config.enable_dict_thrash_guard = false;
        Config.dict_thrash_guard_window_sec = 3600;
        Config.dict_thrash_guard_threshold = 2;
        CacheDictManager mgr = CacheDictManager.getInstance();
        ColumnIdentifier id = col("col_guard_off");

        invalidate(mgr, id, 50);
        assertFalse(noDictColumns().contains(id));
    }

    @Test
    public void testThresholdZeroDisablesCountCheck() {
        Config.enable_dict_thrash_guard = true;
        Config.dict_thrash_guard_window_sec = 3600;
        Config.dict_thrash_guard_threshold = 0;
        CacheDictManager mgr = CacheDictManager.getInstance();
        ColumnIdentifier id = col("col_threshold_zero");

        invalidate(mgr, id, 50);
        assertFalse(noDictColumns().contains(id));
    }

    @Test
    public void testEnableGlobalDictClearsForbidden() {
        Config.enable_dict_thrash_guard = true;
        Config.dict_thrash_guard_window_sec = 3600;
        Config.dict_thrash_guard_threshold = 3;
        CacheDictManager mgr = CacheDictManager.getInstance();
        ColumnIdentifier id = col("col_recover");

        invalidate(mgr, id, 3);
        assertTrue(noDictColumns().contains(id));

        mgr.enableGlobalDict(TABLE_ID);
        assertFalse(noDictColumns().contains(id), "enableGlobalDict should clear the forbidden column");
        assertFalse(invalidationHistory().containsKey(id));
    }

    // clearForbiddenColumns backs ALTER TABLE ... ENABLE DICTIONARY (and its replay): it must clear the
    // in-memory forbid only for the named columns, leaving other columns' forbid state intact.
    @Test
    public void testClearForbiddenColumnsClearsOnlyNamedColumns() {
        Config.enable_dict_thrash_guard = true;
        Config.dict_thrash_guard_window_sec = 3600;
        Config.dict_thrash_guard_threshold = 5;
        CacheDictManager mgr = CacheDictManager.getInstance();

        ColumnIdentifier enableMe = col("enable_me");
        ColumnIdentifier keepMe = col("keep_me");
        noDictColumns().add(enableMe);
        noDictColumns().add(keepMe);
        invalidate(mgr, enableMe, 1); // seed invalidation history for enable_me
        assertTrue(invalidationHistory().containsKey(enableMe));

        mgr.clearForbiddenColumns(TABLE_ID, Set.of("enable_me"));

        assertFalse(noDictColumns().contains(enableMe), "named column's forbid should be cleared");
        assertFalse(invalidationHistory().containsKey(enableMe), "named column's history should be cleared");
        assertTrue(noDictColumns().contains(keepMe), "other column's forbid must remain");
    }
}
