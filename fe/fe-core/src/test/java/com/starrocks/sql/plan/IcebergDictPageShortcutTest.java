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

package com.starrocks.sql.plan;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IcebergDictPageShortcutTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableIcebergDictPageShortcut(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableIcebergDictPageShortcut(false);
    }

    @Test
    public void testSelectDistinctHintApplied() throws Exception {
        String sql = "select distinct data from iceberg0.unpartitioned_db.t0";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "dict-page shortcut: hint=true");
    }

    @Test
    public void testGroupByEquivalentToDistinct() throws Exception {
        String sql = "select data from iceberg0.unpartitioned_db.t0 group by data";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "dict-page shortcut: hint=true");
    }

    @Test
    public void testMultiColDistinctNotHinted() throws Exception {
        String sql = "select distinct id, data from iceberg0.unpartitioned_db.t0";
        String plan = getVerboseExplain(sql);
        Assertions.assertFalse(plan.contains("dict-page shortcut: hint=true"),
                "multi-col DISTINCT must not get the hint in PR1");
    }

    @Test
    public void testWithPredicateNotHinted() throws Exception {
        String sql = "select distinct data from iceberg0.unpartitioned_db.t0 where id > 0";
        String plan = getVerboseExplain(sql);
        Assertions.assertFalse(plan.contains("dict-page shortcut: hint=true"),
                "per-row predicate must disable the hint");
    }

    @Test
    public void testAggregateFunctionNotHinted() throws Exception {
        String sql = "select data, count(*) from iceberg0.unpartitioned_db.t0 group by data";
        String plan = getVerboseExplain(sql);
        Assertions.assertFalse(plan.contains("dict-page shortcut: hint=true"),
                "agg with aggregate functions must not get the hint");
    }

    @Test
    public void testInnerLimitNotHinted() throws Exception {
        String sql = "select distinct data from "
                + "(select data from iceberg0.unpartitioned_db.t0 order by data limit 2) s";
        String plan = getVerboseExplain(sql);
        Assertions.assertFalse(plan.contains("dict-page shortcut: hint=true"),
                "inner LIMIT/ORDER BY changes the row set; shortcut would be incorrect");
    }

    @Test
    public void testScanLimitNotHinted() throws Exception {
        String sql = "select distinct data from iceberg0.unpartitioned_db.t0 limit 5";
        String plan = getVerboseExplain(sql);
        Assertions.assertFalse(plan.contains("dict-page shortcut: hint=true"),
                "scan-level LIMIT must disable the hint");
    }

    @Test
    public void testFlagOffNoHint() throws Exception {
        connectContext.getSessionVariable().setEnableIcebergDictPageShortcut(false);
        try {
            String sql = "select distinct data from iceberg0.unpartitioned_db.t0";
            String plan = getVerboseExplain(sql);
            Assertions.assertFalse(plan.contains("dict-page shortcut: hint=true"),
                    "session flag off must produce no hint");
        } finally {
            connectContext.getSessionVariable().setEnableIcebergDictPageShortcut(true);
        }
    }
}
