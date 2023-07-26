// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/DatabaseTest.java

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

package com.starrocks.catalog;

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.system.Backend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class CreateViewTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        Backend be = UtFrameUtils.addMockBackend(10002);
        be.setIsDecommissioned(true);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        Config.enable_strict_storage_medium_check = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
    }

    @Test
    public void testCreateViewNullable() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase("test");
        starRocksAssert.withTable("CREATE TABLE `site_access` (\n" +
                        "  `event_day` date NULL COMMENT \"\",\n" +
                        "  `site_id` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n" +
                        "  `city_code` varchar(100) NULL COMMENT \"\",\n" +
                        "  `user_name` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                        "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`event_day`, `site_id`, `city_code`, `user_name`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(`event_day`)\n" +
                        "(PARTITION p20200321 VALUES [(\"0000-01-01\"), (\"2020-03-22\")),\n" +
                        "PARTITION p20200322 VALUES [(\"2020-03-22\"), (\"2020-03-23\")),\n" +
                        "PARTITION p20200323 VALUES [(\"2020-03-23\"), (\"2020-03-24\")),\n" +
                        "PARTITION p20200324 VALUES [(\"2020-03-24\"), (\"2020-03-25\")))\n" +
                        "DISTRIBUTED BY HASH(`event_day`, `site_id`) BUCKETS 32 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\",\n" +
                        "\"enable_persistent_index\" = \"false\",\n" +
                        "\"compression\" = \"LZ4\"\n" +
                        ");")
                .withView("create view test_null_view as select * from site_access;");

        Table view = starRocksAssert.getCtx().getGlobalStateMgr()
                .getDb("test").getTable("test_null_view");
        Assert.assertTrue(view instanceof View);
        List<Column> columns = view.getColumns();
        for (Column column : columns) {
            Assert.assertTrue(column.isAllowNull());
        }
    }
<<<<<<< HEAD
}
=======

    @Test
    public void createReplace() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase("test");
        starRocksAssert.withTable("CREATE TABLE `test_replace_site_access` (\n" +
                "  `event_day` date NULL COMMENT \"\",\n" +
                "  `site_id` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n" +
                "  `city_code` varchar(100) NULL COMMENT \"\",\n" +
                "  `user_name` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`event_day`, `site_id`, `city_code`, `user_name`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`event_day`, `site_id`) BUCKETS 32 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");

        // create non existed view
        starRocksAssert.withView("create or replace view test_null_view as select event_day " +
                "from test_replace_site_access;");
        Assert.assertNotNull(starRocksAssert.getTable("test", "test_null_view"));

        // replace existed view
        starRocksAssert.withView("create or replace view test_null_view as select site_id " +
                "from test_replace_site_access;");
        View view = (View) starRocksAssert.getTable("test", "test_null_view");
        Assert.assertEquals(
                "SELECT `test`.`test_replace_site_access`.`site_id`\nFROM `test`.`test_replace_site_access`",
                view.getInlineViewDef());
        Assert.assertNotNull(view.getColumn("site_id"));
    }
}
>>>>>>> 6e1d5ec99b ([Feature] support create or replace view (#27768))
