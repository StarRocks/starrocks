// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UseMaterializedViewTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {

        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k2)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('10'),\n" +
                        "    PARTITION p2 values less than('20')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view mv1 " +
                        "partition by ss " +
                        "distributed by hash(k2) " +
                        "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ") " +
                        "as select tbl1.k1 ss, k2 from tbl1;");
    }

    @Test
    public void testSelect() {
        String sql = "select * from mv1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            assertTrue(statementBase instanceof QueryStatement);
            QueryRelation queryRelation = ((QueryStatement) statementBase).getQueryRelation();
            TableRelation tableRelation = ((TableRelation) ((SelectRelation) queryRelation).getRelation());
            assertTrue(tableRelation.getTable() instanceof MaterializedView);
            assertEquals(tableRelation.getResolveTableName().getTbl(), "mv1");
            Map<Field, Column> columns = tableRelation.getColumns();
            assertEquals(columns.size(),2);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testInsert() {
        String sql = "insert into mv1 values(date('2020-04-21'),1)";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),"data cannot be inserted into table with empty partition.Use `SHOW PARTITIONS FROM mv1` to see the currently partitions of this table. ");
        }

    }

    @Test
    public void testUpdate() {
        String sql = "update mv1 set k2 = 2";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),"only support updating primary key table");
        }
    }

    @Test
    public void testDelete() {
        String sql = "delete from mv1 where k2 = 2";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }



}

