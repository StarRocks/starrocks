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

package com.starrocks.catalog;

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.system.Backend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CreateViewTest extends StarRocksTestBase  {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        Backend be = UtFrameUtils.addMockBackend(10002);
        be.setDecommissioned(true);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);

        Config.enable_strict_storage_medium_check = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());

        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase("test");
    }

    @Test
    public void testCreateViewNullable() throws Exception {
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
                        "\"enable_persistent_index\" = \"true\",\n" +
                        "\"compression\" = \"LZ4\"\n" +
                        ");")
                .withView("create view test_null_view as select * from site_access;");

        Table view = starRocksAssert.getCtx().getGlobalStateMgr()
                .getLocalMetastore().getDb("test").getTable("test_null_view");
        Assertions.assertTrue(view instanceof View);
        List<Column> columns = view.getColumns();
        for (Column column : columns) {
            Assertions.assertTrue(column.isAllowNull());
        }
    }

    @Test
    public void createReplace() throws Exception {
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
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");

        // create non existed view
        starRocksAssert.withView("create or replace view test_null_view as select event_day " +
                "from test_replace_site_access;");
        Assertions.assertNotNull(starRocksAssert.getTable("test", "test_null_view"));

        // replace existed view
        starRocksAssert.withView("create or replace view test_null_view as select site_id " +
                "from test_replace_site_access;");
        View view = (View) starRocksAssert.getTable("test", "test_null_view");
        Assertions.assertEquals(
                "SELECT `test`.`test_replace_site_access`.`site_id`\nFROM `test`.`test_replace_site_access`",
                view.getInlineViewDef());
        Assertions.assertNotNull(view.getColumn("site_id"));
    }

    @Test
    public void testCreateViewWithWindowFunctionIgnoreNulls() throws Exception {
        starRocksAssert.withTable("create table sample_data (\n" +
                        "    timestamp DATETIME not null,\n" +
                        "    username string,\n" +
                        "    price int null\n" +
                        ")PROPERTIES (\n" +
                        "\"replication_num\" = \"1\");")
                .withView("create view test_ignore_nulls as select\n" +
                        "    timestamp,\n" +
                        "    username,\n" +
                        "    last_value(price ignore nulls) over (partition by username) as price\n" +
                        ", lead(price ignore nulls,1,0) over (partition by username) as leadValue\n" +
                        "from sample_data;");

        Table view = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                .getDb("test").getTable("test_ignore_nulls");
        Assertions.assertTrue(view instanceof View);
        String str = ((View) view).getInlineViewDef();
        Assertions.assertEquals(str, "SELECT `test`.`sample_data`.`timestamp`, `test`.`sample_data`.`username`, " +
                "last_value(`test`.`sample_data`.`price` ignore nulls) OVER " +
                "(PARTITION BY `test`.`sample_data`.`username` ) AS `price`, " +
                "lead(`test`.`sample_data`.`price` ignore nulls, 1, 0) OVER " +
                "(PARTITION BY `test`.`sample_data`.`username` ) AS `leadValue`\n" +
                "FROM `test`.`sample_data`");
    }

    @Test
    public void createViewWithComment() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_table1 (\n" +
                "  `event_day` date NULL COMMENT \"\" ,\n" +
                "  `site_id` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n" +
                "  `city_code` varchar(100) NULL COMMENT \"\",\n" +
                "  `user_name` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                ") \n" +
                "DUPLICATE KEY(`event_day`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`event_day`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ");");

        // create non existed view
        starRocksAssert.withView("create or replace view test_view1 as select \n" +
                "`event_day` -- This is a comment from user\n" +
                "from test_table1;");

        List<List<String>> result = starRocksAssert.show("show create view test_view1;");
        Assertions.assertEquals(1, result.size());
        String createViewSql = result.get(0).get(1);
        Assertions.assertTrue(createViewSql.contains("-- This is a comment from user"));
    }
}
