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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class StorageCoolDownTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
    }

    @Test
    public void testForbidCreateTable() throws Exception {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "storage medium property is not found",
                () -> createTable(
                        "CREATE TABLE site_access_with_only_ttl(\n" +
                                "event_day DATE,\n" +
                                "site_id INT DEFAULT '10',\n" +
                                "city_code VARCHAR(100),\n" +
                                "user_name VARCHAR(32) DEFAULT '',\n" +
                                "pv BIGINT DEFAULT '0'\n" +
                                ")\n" +
                                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                                "PARTITION BY RANGE(event_day)(\n" +
                                "\tSTART (\"2021-05-01\") END (\"2021-05-04\") EVERY (INTERVAL 1 DAY)\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                                "PROPERTIES(\n" +
                                "\t\"replication_num\" = \"1\",\n" +
                                "    \"dynamic_partition.enable\" = \"false\",\n" +
                                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                                "    \"dynamic_partition.start\" = \"-3\",\n" +
                                "    \"dynamic_partition.end\" = \"3\",\n" +
                                "    \"dynamic_partition.history_partition_num\" = \"0\",\n" +
                                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                                ");"
                ));

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "e: Can not assign cooldown ttl to table with HDD storage medium",
                () -> createTable(
                        "CREATE TABLE site_access_date_with_1_day_ttl_less_than(\n" +
                                "event_day DATE,\n" +
                                "site_id INT DEFAULT '10',\n" +
                                "city_code VARCHAR(100),\n" +
                                "user_name VARCHAR(32) DEFAULT '',\n" +
                                "pv BIGINT DEFAULT '0'\n" +
                                ")\n" +
                                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                                "PARTITION BY RANGE(event_day)(\n" +
                                "\tSTART (\"2021-05-01\") END (\"2021-05-04\") EVERY (INTERVAL 1 DAY)\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                                "PROPERTIES(\n" +
                                "\t\"replication_num\" = \"1\",\n" +
                                "    \"dynamic_partition.enable\" = \"false\",\n" +
                                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                                "    \"dynamic_partition.start\" = \"-3\",\n" +
                                "    \"dynamic_partition.end\" = \"3\",\n" +
                                "    \"dynamic_partition.history_partition_num\" = \"0\",\n" +
                                "    \"storage_medium\" = \"HDD\",\n" +
                                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                                ");"
                ));
    }

    @Test
    public void testDateWithTTLLessThan() throws Exception {

        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE site_access_datetime_with_1_day_ttl_less_than(\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN (\"2020-03-25\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "\t\"replication_num\" = \"1\",\n" +
                "    \"dynamic_partition.enable\" = \"false\",\n" +
                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "    \"dynamic_partition.start\" = \"-3\",\n" +
                "    \"dynamic_partition.end\" = \"3\",\n" +
                "    \"dynamic_partition.history_partition_num\" = \"0\",\n" +
                "    \"storage_medium\" = \"SSD\",\n" +
                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                ");";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test")
                .getTable("site_access_datetime_with_1_day_ttl_less_than");

        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        DataProperty p20200321 = partitionInfo.idToDataProperty.get(table.getPartition("p20200321").getId());
        DataProperty p20200322 = partitionInfo.idToDataProperty.get(table.getPartition("p20200322").getId());
        DataProperty p20200323 = partitionInfo.idToDataProperty.get(table.getPartition("p20200323").getId());
        DataProperty p20200324 = partitionInfo.idToDataProperty.get(table.getPartition("p20200324").getId());

        Assert.assertEquals("2020-03-23 00:00:00", TimeUtils.longToTimeString(p20200321.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-24 00:00:00", TimeUtils.longToTimeString(p20200322.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-25 00:00:00", TimeUtils.longToTimeString(p20200323.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-26 00:00:00", TimeUtils.longToTimeString(p20200324.getCooldownTimeMs()));


        String dropSQL = "drop table site_access_datetime_with_1_day_ttl_less_than";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

    }

    @Test
    public void testDateWithTTLUpperLower() throws Exception {

        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE `site_access_date_upper_lower_ttl` (\n" +
                "  `event_day` date NULL COMMENT \"\",\n" +
                "  `site_id` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n" +
                "  `city_code` varchar(100) NULL COMMENT \"\",\n" +
                "  `user_name` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`event_day`, `site_id`, `city_code`, `user_name`)\n" +
                "PARTITION BY RANGE(`event_day`)\n" +
                "(PARTITION p20230807 VALUES [(\"2023-08-07\"), (\"2023-08-08\")),\n" +
                "PARTITION p20230808 VALUES [(\"2023-08-08\"), (\"2023-08-09\")),\n" +
                "PARTITION p20230809 VALUES [(\"2023-08-09\"), (\"2023-08-10\")),\n" +
                "PARTITION p20230810 VALUES [(\"2023-08-10\"), (\"2023-08-11\")))\n" +
                "DISTRIBUTED BY HASH(`event_day`, `site_id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"false\",\n" +
                "\"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.history_partition_num\" = \"0\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_ttl\" = \"1 days\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");;";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test")
                .getTable("site_access_date_upper_lower_ttl");

        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        DataProperty p20230807 = partitionInfo.idToDataProperty.get(table.getPartition("p20230807").getId());
        DataProperty p20230808 = partitionInfo.idToDataProperty.get(table.getPartition("p20230808").getId());
        DataProperty p20230809 = partitionInfo.idToDataProperty.get(table.getPartition("p20230809").getId());
        DataProperty p20230810 = partitionInfo.idToDataProperty.get(table.getPartition("p20230810").getId());

        Assert.assertEquals("2023-08-09 00:00:00", TimeUtils.longToTimeString(p20230807.getCooldownTimeMs()));
        Assert.assertEquals("2023-08-10 00:00:00", TimeUtils.longToTimeString(p20230808.getCooldownTimeMs()));
        Assert.assertEquals("2023-08-11 00:00:00", TimeUtils.longToTimeString(p20230809.getCooldownTimeMs()));
        Assert.assertEquals("2023-08-12 00:00:00", TimeUtils.longToTimeString(p20230810.getCooldownTimeMs()));


        String dropSQL = "drop table site_access_date_upper_lower_ttl";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

    }

    @Test
    public void testDateWithTTLStartEnd() throws Exception {

        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE site_access_date_with_1_day_ttl_start_end(\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "\tSTART (\"2023-08-07\") END (\"2023-08-11\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "\t\"replication_num\" = \"1\",\n" +
                "    \"dynamic_partition.enable\" = \"false\",\n" +
                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "    \"dynamic_partition.start\" = \"-3\",\n" +
                "    \"dynamic_partition.end\" = \"3\",\n" +
                "    \"dynamic_partition.history_partition_num\" = \"0\",\n" +
                "    \"storage_medium\" = \"SSD\",\n" +
                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                ");";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test")
                .getTable("site_access_date_with_1_day_ttl_start_end");

        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        DataProperty p20230807 = partitionInfo.idToDataProperty.get(table.getPartition("p20230807").getId());
        DataProperty p20230808 = partitionInfo.idToDataProperty.get(table.getPartition("p20230808").getId());
        DataProperty p20230809 = partitionInfo.idToDataProperty.get(table.getPartition("p20230809").getId());
        DataProperty p20230810 = partitionInfo.idToDataProperty.get(table.getPartition("p20230810").getId());

        Assert.assertEquals("2023-08-09 00:00:00", TimeUtils.longToTimeString(p20230807.getCooldownTimeMs()));
        Assert.assertEquals("2023-08-10 00:00:00", TimeUtils.longToTimeString(p20230808.getCooldownTimeMs()));
        Assert.assertEquals("2023-08-11 00:00:00", TimeUtils.longToTimeString(p20230809.getCooldownTimeMs()));
        Assert.assertEquals("2023-08-12 00:00:00", TimeUtils.longToTimeString(p20230810.getCooldownTimeMs()));


        String dropSQL = "drop table site_access_date_with_1_day_ttl_start_end";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

    }

    @Test
    public void testDateTimeWithTTLLessThan() throws Exception {

        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE site_access_date_with_1_day_ttl_less_than(\n" +
                "event_day DATETIME,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN (\"2020-03-25\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "\t\"replication_num\" = \"1\",\n" +
                "    \"dynamic_partition.enable\" = \"false\",\n" +
                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "    \"dynamic_partition.start\" = \"-3\",\n" +
                "    \"dynamic_partition.end\" = \"3\",\n" +
                "    \"dynamic_partition.history_partition_num\" = \"0\",\n" +
                "    \"storage_medium\" = \"SSD\",\n" +
                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                ");";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test")
                .getTable("site_access_date_with_1_day_ttl_less_than");

        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        DataProperty p20200321 = partitionInfo.idToDataProperty.get(table.getPartition("p20200321").getId());
        DataProperty p20200322 = partitionInfo.idToDataProperty.get(table.getPartition("p20200322").getId());
        DataProperty p20200323 = partitionInfo.idToDataProperty.get(table.getPartition("p20200323").getId());
        DataProperty p20200324 = partitionInfo.idToDataProperty.get(table.getPartition("p20200324").getId());

        Assert.assertEquals("2020-03-23 00:00:00", TimeUtils.longToTimeString(p20200321.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-24 00:00:00", TimeUtils.longToTimeString(p20200322.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-25 00:00:00", TimeUtils.longToTimeString(p20200323.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-26 00:00:00", TimeUtils.longToTimeString(p20200324.getCooldownTimeMs()));


        String dropSQL = "drop table site_access_date_with_1_day_ttl_less_than";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

    }

    @Test
    public void testDateTimeWithMaxPartition() throws Exception {

        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE site_access_with_max_partition (\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN MAXVALUE\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "        \"replication_num\" = \"1\",\n" +
                "    \"dynamic_partition.enable\" = \"false\",\n" +
                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "    \"dynamic_partition.start\" = \"-3\",\n" +
                "    \"dynamic_partition.end\" = \"3\",\n" +
                "    \"dynamic_partition.history_partition_num\" = \"0\",\n" +
                "    \"storage_medium\" = \"SSD\",\n" +
                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                ");";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test")
                .getTable("site_access_with_max_partition");

        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        DataProperty p20200321 = partitionInfo.idToDataProperty.get(table.getPartition("p20200321").getId());
        DataProperty p20200322 = partitionInfo.idToDataProperty.get(table.getPartition("p20200322").getId());
        DataProperty p20200323 = partitionInfo.idToDataProperty.get(table.getPartition("p20200323").getId());
        DataProperty p20200324 = partitionInfo.idToDataProperty.get(table.getPartition("p20200324").getId());

        Assert.assertEquals("2020-03-23 00:00:00", TimeUtils.longToTimeString(p20200321.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-24 00:00:00", TimeUtils.longToTimeString(p20200322.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-25 00:00:00", TimeUtils.longToTimeString(p20200323.getCooldownTimeMs()));
        Assert.assertEquals("9999-12-31 23:59:59", TimeUtils.longToTimeString(p20200324.getCooldownTimeMs()));


        String dropSQL = "drop table site_access_with_max_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

    }
}
