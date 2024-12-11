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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
=======
import com.starrocks.sql.plan.ConnectorPlanTestBase;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MvRefreshAndRewriteJDBCTest extends MvRewriteTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
<<<<<<< HEAD
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
=======
        ConnectorPlanTestBase.mockCatalog(connectContext, MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME);
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr();
        MockedJDBCMetadata mockedJDBCMetadata =
                    (MockedJDBCMetadata) metadataMgr.getOptionalMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME).get();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        mockedJDBCMetadata.initPartitions();
    }

    @Test
    public void testStr2DateMVRefreshRewrite_InnerJoin_FullRefresh() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
<<<<<<< HEAD
                "partition by str2date(d,'%Y%m%d') " +
                "distributed by hash(a) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\",\n" +
                "'replication_num' = '1'" +
                ") " +
                "as select  t1.a, t2.b, t3.c, t1.d " +
                " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
=======
                    "partition by str2date(d,'%Y%m%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'," +
                    "'query_rewrite_consistency' = 'loose'" +
                    ") " +
                    "as select  t1.a, t2.b, t3.c, t1.d " +
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " force with sync mode");
        List<String> partitions =
<<<<<<< HEAD
                materializedView.getPartitions().stream().map(Partition::getName).sorted()
                        .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("p20230801_20230802", "p20230802_20230803", "p20230803_20230804"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                    " where t1.d='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                    "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 16: d = '20230801'\n" +
                    "     partitions=1/3");
=======
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("p00010101_20230801", "p20230801_20230802",
                    "p20230802_20230803", "p20230803_99991231"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                        " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d = '20230801'\n" +
                        "     partitions=1/4");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
<<<<<<< HEAD
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                    " where t1.d>='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                    "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 16: d >= '20230801'\n" +
                    "     partitions=3/3\n" +
                    "     rollup: test_mv1");
=======
                        " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                        " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d >= '20230801'\n" +
                        "     partitions=3/4\n" +
                        "     rollup: test_mv1");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
<<<<<<< HEAD
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                    " where t1.d < '20230802';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                    "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/3\n" +
                    "     rollup: test_mv1");
=======
                        " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                        " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d < '20230802';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=2/4\n" +
                        "     rollup: test_mv1");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_InnerJoin_PartialRefresh() throws Exception {
<<<<<<< HEAD
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                "partition by str2date(d,'%Y%m%d') " +
                "distributed by hash(a) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"query_rewrite_consistency\" = \"disable\",\n" +
                "'replication_num' = '1'" +
                ") " +
                "as select  t1.a, t2.b, t3.c, t1.d " +
                " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('20230801') " +
                "end ('20230802') force with sync mode");
        List<String> partitions =
                materializedView.getPartitions().stream().map(Partition::getName).sorted()
                        .collect(Collectors.toList());
=======
        String mvName = "test_mv2";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y%m%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"query_rewrite_consistency\" = \"disable\",\n" +
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
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('20230801') " +
                    "end ('20230802') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        Assert.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
<<<<<<< HEAD
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                    " where t1.d='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_mv1");
=======
                        " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                        " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_mv2");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

<<<<<<< HEAD

    @Test
    public void testStr2DateMVRefreshRewrite_LeftJoin_FullRefresh() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                "partition by str2date(d,'%Y%m%d') " +
                "distributed by hash(a) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\",\n" +
                "'replication_num' = '1'" +
                ") " +
                "as select  t1.a, t2.b, t3.c, t1.d " +
                " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
=======
    @Test
    public void testStr2DateMVRefreshRewrite_LeftJoin_FullRefresh() throws Exception {
        String mvName = "test_mv3";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y%m%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'," +
                    "'query_rewrite_consistency' = 'loose'" +
                    ") " +
                    "as select  t1.a, t2.b, t3.c, t1.d " +
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " force with sync mode");
        List<String> partitions =
<<<<<<< HEAD
                materializedView.getPartitions().stream().map(Partition::getName).sorted()
                        .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("p20230801_20230802", "p20230802_20230803", "p20230803_20230804"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                    " where t1.d='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                    "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 16: d = '20230801'\n" +
                    "     partitions=1/3");
=======
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("p00010101_20230801", "p20230801_20230802", "p20230802_20230803",
                    "p20230803_99991231"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                        " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv3\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d = '20230801'\n" +
                        "     partitions=1/4");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
<<<<<<< HEAD
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                    " where t1.d>='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                    "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 16: d >= '20230801'\n" +
                    "     partitions=3/3\n" +
                    "     rollup: test_mv1");
=======
                        " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                        " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv3\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d >= '20230801'\n" +
                        "     partitions=3/4\n" +
                        "     rollup: test_mv3");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
<<<<<<< HEAD
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                    " where t1.d < '20230802';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                    "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/3\n" +
                    "     rollup: test_mv1");
=======
                        " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                        " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d < '20230802';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv3\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=2/4\n" +
                        "     rollup: test_mv3");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_LeftJoin_PartialRefresh() throws Exception {
<<<<<<< HEAD
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                "partition by str2date(d,'%Y%m%d') " +
                "distributed by hash(a) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"query_rewrite_consistency\" = \"disable\",\n" +
                "'replication_num' = '1'" +
                ") " +
                "as select  t1.a, t2.b, t3.c, t1.d " +
                " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('20230801') " +
                "end ('20230802') force with sync mode");
        List<String> partitions =
                materializedView.getPartitions().stream().map(Partition::getName).sorted()
                        .collect(Collectors.toList());
=======
        String mvName = "test_mv4";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y%m%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"query_rewrite_consistency\" = \"disable\",\n" +
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
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('20230801') " +
                    "end ('20230802') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        Assert.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
<<<<<<< HEAD
                    " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                    " where t1.d='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_mv1");
=======
                        " from  jdbc0.partitioned_db0.part_tbl1 as t1 " +
                        " inner join jdbc0.partitioned_db0.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join jdbc0.partitioned_db0.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_mv4");
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testViewBasedMvRewriteOnJdbcTable() throws Exception {
        String view = "create view jdbc_table_view " +
                    " as select t1.a, t2.b, t1.d, count(t1.c) as cnt" +
                    " from  jdbc0.partitioned_db.part_tbl1 as t1 " +
                    " inner join jdbc0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join jdbc0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.a, t2.b, t1.d;";
        starRocksAssert.withView(view);
        String mvName = "jdbc_view_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y%m%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"query_rewrite_consistency\" = \"loose\",\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    " as select a, b, d, cnt" +
                    " from jdbc_table_view");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " with sync mode");

        {
            String query = "select a, b, d, cnt from jdbc_table_view";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, mvName);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        starRocksAssert.dropMaterializedView(mvName);
    }
}
