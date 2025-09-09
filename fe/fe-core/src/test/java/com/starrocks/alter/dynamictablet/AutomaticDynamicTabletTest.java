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

package com.starrocks.alter.dynamictablet;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TabletStatMgr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AutomaticDynamicTabletTest {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String sql = "create table test_table (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1', 'enable_dynamic_tablet' = 'false'); ";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table");
    }

    @Test
    public void testEnableDynamicTablet() throws Exception {
        {
            Assertions.assertFalse(table.getEnableDynamicTablet());
            Assertions.assertFalse(table.isEnableDynamicTablet());
        }

        {
            table.setEnableDynamicTablet(null);
            Assertions.assertNull(table.getEnableDynamicTablet());
            Assertions.assertEquals(Config.enable_dynamic_tablet, table.isEnableDynamicTablet());
        }

        {
            String sql = "alter table test_table set ('enable_dynamic_tablet' = 'true')";
            starRocksAssert.alterTableProperties(sql);
            Assertions.assertTrue(table.getEnableDynamicTablet());
            Assertions.assertTrue(table.isEnableDynamicTablet());
            Assertions.assertTrue(table.getUniqueProperties().containsKey("enable_dynamic_tablet"));

            TabletStatMgr tabletStatMgr = new TabletStatMgr();
            Deencapsulation.invoke(tabletStatMgr, "triggerDynamicTablet", db, table, Config.dynamic_tablet_split_size);
        }

        {
            Assertions.assertThrows(AnalysisException.class,
                    () -> starRocksAssert.alterTable("alter table test_table split tablet temporary partitions (tp1)"));

            Assertions.assertThrows(AnalysisException.class,
                    () -> starRocksAssert.alterTable("alter table test_table split tablet partitions ()"));

            Assertions.assertThrows(AnalysisException.class,
                    () -> starRocksAssert.alterTable("alter table test_table split tablet ()"));

            Assertions.assertThrows(AnalysisException.class, () -> starRocksAssert.alterTable(
                    "alter table test_table split tablet properties ('dynamic_tablet_split_size' = 'true')"));

            Assertions.assertThrows(AnalysisException.class, () -> starRocksAssert.alterTable(
                    "alter table test_table split tablet properties ('dynamic_tablet_split_size_xxx' = 'true')"));

            Assertions.assertThrows(RuntimeException.class,
                    () -> starRocksAssert.alterTable("alter table test_table split tablet"));
        }

        {
            Config.dynamic_tablet_max_split_count = 8;
            Assertions.assertEquals(8, DynamicTabletUtils.calcSplitCount(100, 20));
            Assertions.assertEquals(8, DynamicTabletUtils.calcSplitCount(100, 10));
        }

        {
            Map<String, String> properties = new HashMap<>();
            properties.put("enable_dynamic_tablet", "100");
            properties.put("dynamic_tablet_split_size", "true");

            Assertions.assertThrows(AnalysisException.class,
                    () -> PropertyAnalyzer.analyzeEnableDynamicTablet(properties, false));
            Assertions.assertThrows(AnalysisException.class,
                    () -> PropertyAnalyzer.analyzeDynamicTabletSplitSize(properties, false));
        }

        {
            Map<String, String> properties = new HashMap<>();
            properties.put("enable_dynamic_tablet", "true");
            properties.put("dynamic_tablet_split_size", "100");

            Assertions.assertEquals(true, PropertyAnalyzer.analyzeEnableDynamicTablet(properties, true));
            Assertions.assertEquals(100, PropertyAnalyzer.analyzeDynamicTabletSplitSize(properties, true));
            Assertions.assertTrue(properties.isEmpty());
        }

        {
            Map<String, String> properties = new HashMap<>();
            properties.put("enable_dynamic_tablet", "false");
            properties.put("dynamic_tablet_split_size", "-4");

            Assertions.assertEquals(false, PropertyAnalyzer.analyzeEnableDynamicTablet(properties, true));
            Assertions.assertEquals(-4, PropertyAnalyzer.analyzeDynamicTabletSplitSize(properties, true));
            Assertions.assertTrue(properties.isEmpty());
        }
    }
}
