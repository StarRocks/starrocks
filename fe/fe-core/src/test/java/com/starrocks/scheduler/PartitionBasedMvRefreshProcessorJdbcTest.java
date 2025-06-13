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

package com.starrocks.scheduler;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionBasedMvRefreshProcessorJdbcTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME);
        starRocksAssert
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`jdbc_parttbl_mv0`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`d`)\n" +
                        "DISTRIBUTED BY HASH(`a`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `a`, `b`, `c`, `d`  FROM `jdbc0`.`partitioned_db0`.`tbl0`;")
                .withMaterializedView("create materialized view jdbc_parttbl_mv1 " +
                        "partition by ss " +
                        "distributed by hash(a) buckets 10 " +
                        "REFRESH DEFERRED MANUAL " +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ") " +
                        "as select str2date(d,'%Y%m%d') ss, a, b, c from jdbc0.partitioned_db0.tbl1;")
                .withMaterializedView("create materialized view jdbc_parttbl_mv2 " +
                        "partition by ss " +
                        "distributed by hash(a) buckets 10 " +
                        "REFRESH DEFERRED MANUAL " +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ") " +
                        "as select str2date(d,'%Y%m%d') ss, a, b, c from jdbc0.partitioned_db0.tbl2;")
                .withMaterializedView("create materialized view jdbc_parttbl_mv3 " +
                        "partition by str2date(d,'%Y%m%d') " +
                        "distributed by hash(a) buckets 10 " +
                        "REFRESH DEFERRED MANUAL " +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ") " +
                        "as select a, b, c, d from jdbc0.partitioned_db0.tbl1;")
                .withMaterializedView("create materialized view jdbc_parttbl_mv5 " +
                        "partition by str2date(d,'%Y%m%d') " +
                        "distributed by hash(a) buckets 10 " +
                        "REFRESH DEFERRED MANUAL " +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ") " +
                        "as select a, b, c, d from jdbc0.partitioned_db0.tbl3;")
                .withMaterializedView("create materialized view jdbc_parttbl_mv6 " +
                        "partition by ss " +
                        "distributed by hash(a) buckets 10 " +
                        "REFRESH DEFERRED MANUAL " +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"partition_refresh_number\" = \"1\"" +
                        ") " +
                        "as select str2date(d,'%Y%m%d') ss, a, b, c from jdbc0.partitioned_db0.tbl1;");
    }

    @Test
    public void testJDBCProtocolType() {
        JDBCTable table = new JDBCTable();
        table.getConnectInfo().put(JDBCResource.URI, "jdbc:postgres:aaa");
        Assert.assertEquals(JDBCTable.ProtocolType.POSTGRES, table.getProtocolType());
        table.getConnectInfo().put(JDBCResource.URI, "jdbc:mysql:aaa");
        Assert.assertEquals(JDBCTable.ProtocolType.MYSQL, table.getProtocolType());
        table.getConnectInfo().put(JDBCResource.URI, "jdbc:h2:aaa");
        Assert.assertEquals(JDBCTable.ProtocolType.UNKNOWN, table.getProtocolType());
    }

    @Test
    public void testPartitionJDBCSupported() throws Exception {
        // not supported
        Assert.assertThrows(AnalysisException.class, () ->
                starRocksAssert.withMaterializedView("create materialized view mv_jdbc_postgres " +
                        "partition by d " +
                        "refresh deferred manual " +
                        "AS SELECT `a`, `b`, `c`, `d`  FROM `jdbc_postgres`.`partitioned_db0`.`tbl0`;")
        );

        // supported
        starRocksAssert.withMaterializedView("create materialized view mv_jdbc_mysql " +
                "partition by d " +
                "refresh deferred manual " +
                "AS SELECT `a`, `b`, `c`, `d`  FROM `jdbc0`.`partitioned_db0`.`tbl0`;");
    }

    @Test
    public void testRangePartitionChangeWithJDBCTable() throws Exception {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
        mockedJDBCMetadata.initPartitions();

        MaterializedView materializedView = refreshMaterializedView("jdbc_parttbl_mv0", "20230801", "20230805");
        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(3, partitions.size());

        mockedJDBCMetadata.addPartitions();
        refreshMVRange(materializedView.getName(), "20230801", "20230805", false);
        Collection<Partition> incrementalPartitions = materializedView.getPartitions();
        Assert.assertEquals(4, incrementalPartitions.size());
    }

    @NotNull
    private MaterializedView refreshMaterializedView(String materializedViewName, String start, String end) throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), materializedViewName));
        refreshMVRange(materializedView.getName(), start, end, false);
        return materializedView;
    }

    @Test
    public void testRangePartitionWithJDBCTableUseStr2Date() throws Exception {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
        mockedJDBCMetadata.initPartitions();

        MaterializedView materializedView = refreshMaterializedView("jdbc_parttbl_mv1", "20230731", "20230805");
        List<String> partitions = materializedView.getPartitions().stream()
                .map(Partition::getName).sorted().collect(Collectors.toList());
        Assert.assertEquals(ImmutableList.of("p00010101_20230801", "p20230801_20230802",
                        "p20230802_20230803", "p20230803_99991231"),
                partitions);
    }

    @Test
    public void testRangePartitionWithJDBCTableUseStr2DateForError() {
        try {
            refreshMaterializedView("jdbc_parttbl_mv2", "20230731", "20230805");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Text '1234567' could not be parsed"));
        }
    }

    @Test
    public void testRangePartitionWithJDBCTableUseStr2Date2() throws Exception {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
        mockedJDBCMetadata.initPartitions();

        MaterializedView materializedView = refreshMaterializedView("jdbc_parttbl_mv3", "20230731", "20230805");
        List<String> partitions = materializedView.getPartitions().stream()
                .map(Partition::getName).sorted().collect(Collectors.toList());
        Assert.assertEquals(ImmutableList.of("p00010101_20230801", "p20230801_20230802",
                        "p20230802_20230803", "p20230803_99991231"),
                partitions);
    }

    @Test
    public void testStr2Date_DateTrunc() throws Exception {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
        mockedJDBCMetadata.initPartitions();

        String mvName = "jdbc_parttbl_str2date";
        starRocksAssert.withMaterializedView("create materialized view jdbc_parttbl_str2date " +
                "partition by date_trunc('month', ss) " +
                "distributed by hash(a) buckets 10 " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"partition_refresh_number\" = \"1\"" +
                ") " +
                "as select str2date(d,'%Y%m%d') ss, a, b, c from jdbc0.partitioned_db0.tbl5;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), mvName));

        // full refresh
        {
            starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " force with sync mode");
            List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                            .collect(Collectors.toList());
            Assert.assertEquals(Arrays.asList("p000101_202308", "p202308_202309"), partitions);
        }

        // partial range refresh 1
        {
            Map<String, Long> partitionVersionMap = new HashMap<>();
            for (Partition p : materializedView.getPartitions()) {
                partitionVersionMap.put(p.getName(), p.getDefaultPhysicalPartition().getVisibleVersion());
            }
            starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName +
                    " partition start('2023-08-02') end('2023-09-01')" +
                    "force with sync mode");
            List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                            .collect(Collectors.toList());
            Assert.assertEquals(Arrays.asList("p000101_202308", "p202308_202309"), partitions);
            // mv partition p202308_202309 is force refreshed and visible version is increased
            Assert.assertEquals(partitionVersionMap.get("p202308_202309") + 1,
                    materializedView.getPartition("p202308_202309")
                            .getDefaultPhysicalPartition().getVisibleVersion());
        }

        // partial range refresh 2
        {
            Map<String, Long> partitionVersionMap = new HashMap<>();
            for (Partition p : materializedView.getPartitions()) {
                partitionVersionMap.put(p.getName(), p.getDefaultPhysicalPartition().getVisibleVersion());
            }
            starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName +
                    " partition start('2023-07-01') end('2023-08-01')" +
                    "force with sync mode");
            List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                            .collect(Collectors.toList());
            Assert.assertEquals(Arrays.asList("p000101_202308", "p202308_202309"), partitions);
            Assert.assertEquals(partitionVersionMap.get("p202308_202309").longValue(),
                    materializedView.getPartition("p202308_202309").getDefaultPhysicalPartition()
                            .getVisibleVersion());
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2Date_TTL() throws Exception {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
        mockedJDBCMetadata.initPartitions();

        String mvName = "jdbc_parttbl_str2date";
        starRocksAssert.withMaterializedView("create materialized view jdbc_parttbl_str2date " +
                "partition by ss " +
                "distributed by hash(a) buckets 10 " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"partition_refresh_number\" = \"1\"," +
                "'partition_ttl_number'='2'" +
                ") " +
                "as select str2date(d,'%Y%m%d') ss, a, b, c from jdbc0.partitioned_db0.tbl1;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " force with sync mode");
        List<String> partitions =
                materializedView.getPartitions().stream().map(Partition::getName).sorted()
                        .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("p20230802_20230803", "p20230803_99991231"), partitions);

        // modify TTL
        {
            starRocksAssert.getCtx().executeSql(
                    String.format("alter materialized view %s set ('partition_ttl_number'='1')", mvName));
            starRocksAssert.getCtx().executeSql(String.format("refresh materialized view %s with sync mode", mvName));
            GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();

            partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                            .collect(Collectors.toList());
            Assert.assertEquals(Arrays.asList("p20230803_99991231"), partitions);
        }
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testRangePartitionWithJDBCTableUseStr2Date3() throws Exception {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
        mockedJDBCMetadata.initPartitions();

        MaterializedView materializedView = refreshMaterializedView("jdbc_parttbl_mv5", "20230731", "20230805");
        List<String> partitions = materializedView.getPartitions().stream()
                .map(Partition::getName).sorted().collect(Collectors.toList());
        Assert.assertEquals(ImmutableList.of("p00010101_20230801", "p20230801_20230802",
                        "p20230802_20230803", "p20230803_20230804"),
                partitions);
    }

    @Test
    public void testRefreshByParCreateOnlyNecessaryPar() throws Exception {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
        mockedJDBCMetadata.initPartitions();

        // get base table partitions
        List<String> baseParNames =
                mockedJDBCMetadata.listPartitionNames("partitioned_db0", "tbl1", ConnectorMetadatRequestContext.DEFAULT);
        Assert.assertEquals(4, baseParNames.size());

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "jdbc_parttbl_mv6"));
        HashMap<String, String> taskRunProperties = new HashMap<>();
        // check corner case: the first partition of base table is 0000 to 20230801
        // p20230801 of mv should not be created
        refreshMVRange(materializedView.getName(), "20230801", "20230802", false);
        List<String> partitions = materializedView.getPartitions().stream()
                .map(Partition::getName).sorted().collect(Collectors.toList());
        Assert.assertEquals(ImmutableList.of("p20230801_20230802"), partitions);
    }

    @Test
    public void testStr2DateMVRefresh_Rewrite() throws Exception {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
        mockedJDBCMetadata.initPartitions();

        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                "partition by str2date(d,'%Y%m%d') " +
                "distributed by hash(a) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "'replication_num' = '1'" +
                ") " +
                "as select  t1.a, t2.b, t3.c, t1.d " +
                " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " force with sync mode");
        List<String> partitions =
                materializedView.getPartitions().stream().map(Partition::getName).sorted()
                        .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("p00010101_20230801", "p20230801_20230802", "p20230802_20230803",
                "p20230803_99991231"), partitions);

        starRocksAssert.dropMaterializedView(mvName);
    }
}