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

package com.starrocks.qe;

import com.google.gson.Gson;
import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class QueryDetailTest {
    @Test
    public void testQueryDetail() {
        QueryDetail queryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", true, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                "testDb", "select * from table1 limit 1",
                "root", "", "default_catalog");
        queryDetail.setProfile("bbbbb");
        queryDetail.setErrorMessage("cancelled");

        QueryDetail copyOfQueryDetail = queryDetail.copy();
        Gson gson = new Gson();
        Assert.assertEquals(gson.toJson(queryDetail), gson.toJson(copyOfQueryDetail));

        queryDetail.setLatency(10);
        Assert.assertEquals(-1, copyOfQueryDetail.getLatency());
    }

    @Test
    public void testAddScanPartitions() throws Exception {
        //1.init env
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        PseudoCluster cluster = PseudoCluster.getInstance();

        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);

        //2.create table and insert data
        starRocksAssert.withDatabase("db1")
                .useDatabase("db1")
                .withTable("CREATE TABLE db1.test_add_scan_partitions_1 ( " +
                        "  c1 string, " +
                        "  c2 string, " +
                        "  c3 string, " +
                        "  ds date " +
                        ") ENGINE=OLAP " +
                        "DUPLICATE KEY(c1) " +
                        "PARTITION BY RANGE(ds) " +
                        "( " +
                        "PARTITION p20241001 VALUES [('2024-10-01'), ('2024-10-02')), " +
                        "PARTITION p20241002 VALUES [('2024-10-02'), ('2024-10-03'))) " +
                        "DISTRIBUTED BY HASH(c1) " +
                        "PROPERTIES ( " +
                        "'replication_num' = '1'" +
                        ");")
                .withTable("CREATE TABLE db1.test_add_scan_partitions_2 ( " +
                        "    c1 string, " +
                        "    c2 string, " +
                        "    c3 string, " +
                        "    ds varchar(20) " +
                        ") " +
                        "DUPLICATE KEY(c1) " +
                        "PARTITION BY (ds,c3) " +
                        "DISTRIBUTED BY HASH(c1) " +
                        "PROPERTIES ( " +
                        "'replication_num' = '1'" +
                        ");")
                .withTable("CREATE TABLE db1.test_add_scan_partitions_3 ( " +
                        "  c1 string, " +
                        "  c2 string, " +
                        "  c3 string, " +
                        "  ds string " +
                        ") ENGINE=OLAP " +
                        "DUPLICATE KEY(c1) " +
                        "DISTRIBUTED BY HASH(c1) " +
                        "PROPERTIES ( " +
                        "'replication_num' = '1'" +
                        ");");
        String sql1 = "insert into db1.test_add_scan_partitions_1(c1,c2,c3,ds) values('1','1','1','2024-10-01');" +
                "insert into db1.test_add_scan_partitions_1(c1,c2,c3,ds) values('2','2','2','2024-10-02');" +
                "insert into db1.test_add_scan_partitions_2(c1,c2,c3,ds) values('1','1','1','2024-10-01');" +
                "insert into db1.test_add_scan_partitions_2(c1,c2,c3,ds) values('2','2','2','2024-10-02');" +
                "insert into db1.test_add_scan_partitions_3(c1,c2,c3,ds) values('1','1','1','2024-10-01');" +
                "insert into db1.test_add_scan_partitions_3(c1,c2,c3,ds) values('2','2','2','2024-10-02');";
        cluster.runSql("db1", sql1);
        System.out.println("test_add_scan_partitions_1 RowCount: "
                + cluster.getTableRowCount("db1", "test_add_scan_partitions_1"));
        System.out.println("test_add_scan_partitions_2 RowCount: "
                + cluster.getTableRowCount("db1", "test_add_scan_partitions_2"));
        System.out.println("test_add_scan_partitions_3 RowCount: "
                + cluster.getTableRowCount("db1", "test_add_scan_partitions_3"));
        Assert.assertEquals(2, cluster.getTableRowCount("db1", "test_add_scan_partitions_1"));
        Assert.assertEquals(2, cluster.getTableRowCount("db1", "test_add_scan_partitions_2"));
        Assert.assertEquals(2, cluster.getTableRowCount("db1", "test_add_scan_partitions_3"));


        //3.enable scan partitions audit
        cluster.runSql("db1", "set enable_scan_partitions_audit=true");

        //4.query
        String sql2 = "select count(*) as row_num from db1.test_add_scan_partitions_1" +
                " union all " +
                " select count(*) as row_num from db1.test_add_scan_partitions_2" +
                " union all " +
                " select count(*) as row_num from db1.test_add_scan_partitions_3";
        QueryStatement parsedStmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, parsedStmt);
        Config.enable_collect_query_detail_info = true;
        long startTime = System.currentTimeMillis();
        executor.addRunningQueryDetail(parsedStmt);
        executor.execute();
        executor.addFinishedQueryDetail();

        //5.diff actual and expect info
        List<QueryDetail> queryDetails = QueryDetailQueue.getQueryDetailsAfterTime(startTime);
        QueryDetail finishedDetail = queryDetails.get(1);

        String expectInfo = "\"[{catalogName:default_catalog,databaseName:db1,tableName:test_add_scan_partitions_1," +
                "partitionIds:[p20241001, p20241002]}, " +
                "{catalogName:default_catalog,databaseName:db1,tableName:test_add_scan_partitions_2," +
                "partitionIds:[p20241002_2, p20241001_1]}, " +
                "{catalogName:default_catalog,databaseName:db1,tableName:test_add_scan_partitions_3," +
                "partitionIds:[test_add_scan_partitions_3]}]\"";
        //System.out.println("expect info=" + expectInfo);
        //System.out.println("actual info=" + finishedDetail.getScanPartitions());
        Assert.assertEquals(expectInfo, finishedDetail.getScanPartitions());

        Config.enable_collect_query_detail_info = false;
    }
}
