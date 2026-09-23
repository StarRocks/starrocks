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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LanceTable;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.lance.LanceApiConverter;
import com.starrocks.connector.lance.LanceMetadata;
import com.starrocks.planner.LanceScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCloudType;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.type.IntegerType.INT;

public class LancePlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        // 1. Create a mocked catalog
        String catalogName = "lance_catalog";
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("aws.s3.access_key", "test-access");
        properties.put("aws.s3.secret_key", "test-secret");
        properties.put("aws.s3.session_token", "test-session");
        properties.put("aws.s3.region", "us-east-1");
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("lance", catalogName, "", properties);

        // 2. Wrap it with mocked metadata manager
        GlobalStateMgr gsmMgr = connectContext.getGlobalStateMgr();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);

        // 3. Setup a mock Lance metadata with db & tables
        LanceMetadata metadata = new LanceMetadata(catalogName, properties);
        Database db = new Database(10001, "db1");
        metadata.addDatabase(db);

        List<Column> columns = ImmutableList.of(
                new Column("id", INT),
                new Column("embedding", LanceApiConverter.parseType("fixed_size_list<float32, 128>"))
        );
        LanceTable table = new LanceTable(20001, "vectors_table", columns, "s3://bucket/vectors", catalogName);
        metadata.addTable("db1", table);
        metadata.addTable("db1", new LanceTable(20002, "events", columns, "s3://bucket/events", catalogName));

        metadataMgr.registerMockedMetadata(catalogName, metadata);
    }

    @Test
    public void testSelectAll() throws Exception {
        String sql = "SELECT * FROM lance_catalog.db1.vectors_table";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "LanceScanNode");
        assertContains(plan, "TABLE: vectors_table");

        // A single JNI range reads every fragment; the dataset URI is in the table descriptor.
        com.starrocks.common.Pair<String, com.starrocks.qe.DefaultCoordinator> pair =
                com.starrocks.utframe.UtFrameUtils.getPlanAndStartScheduling(connectContext, sql);
        java.util.List<com.starrocks.thrift.TScanRangeParams> tScanRangeLocationsList =
                collectAllScanRangeParams(pair.second);
        org.junit.jupiter.api.Assertions.assertFalse(tScanRangeLocationsList.isEmpty());
        com.starrocks.thrift.THdfsScanRange hdfsScanRange = tScanRangeLocationsList.get(0).scan_range.hdfs_scan_range;
        org.junit.jupiter.api.Assertions.assertFalse(hdfsScanRange.isSetUse_lance_jni_reader());
        org.junit.jupiter.api.Assertions.assertFalse(hdfsScanRange.isSetLance_split_info());
    }

    @Test
    public void testSelectWithPredicate() throws Exception {
        String sql = "SELECT id, embedding FROM lance_catalog.db1.vectors_table WHERE id > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "LanceScanNode");
        assertContains(plan, "TABLE: vectors_table");
        assertContains(plan, "PREDICATES: 1: id > 10");
    }
    @Test
    public void testProjectionPredicateAndCatalogCredentialsReachThrift() throws Exception {
        ExecPlan plan = getExecPlan("SELECT id FROM lance_catalog.db1.vectors_table WHERE id > 10 LIMIT 3");
        LanceScanNode scan = (LanceScanNode) plan.getScanNodes().get(0);
        Assertions.assertEquals(1, scan.getDesc().getSlots().size());
        Assertions.assertEquals("id", scan.getDesc().getSlots().get(0).getColumn().getName());
        Assertions.assertTrue(scan.getDesc().getSlots().get(0).getIsNullable());
        TPlanNode thrift = scan.treeToThrift().getNodes().get(0);
        Assertions.assertEquals(1, thrift.getConjunctsSize());
        Assertions.assertEquals(3, thrift.getLimit());
        Assertions.assertEquals(TCloudType.AWS, thrift.getHdfs_scan_node().getCloud_configuration().getCloud_type());
        Map<String, String> cloud = thrift.getHdfs_scan_node().getCloud_configuration().getCloud_properties();
        Assertions.assertEquals("test-access", cloud.get("aws.s3.access_key"));
        Assertions.assertEquals("test-secret", cloud.get("aws.s3.secret_key"));
        Assertions.assertEquals("test-session", cloud.get("aws.s3.session_token"));
        Assertions.assertFalse(getFragmentPlan("SELECT id FROM lance_catalog.db1.vectors_table").contains("test-secret"));
    }

    @Test
    public void testJoinAndAggregationRetainBothScans() throws Exception {
        ExecPlan plan = getExecPlan("SELECT a.id, count(*) FROM lance_catalog.db1.vectors_table a "
                + "JOIN lance_catalog.db1.events b ON a.id = b.id WHERE a.id > 10 GROUP BY a.id");
        Assertions.assertEquals(2, plan.getScanNodes().size());
        for (com.starrocks.planner.ScanNode scan : plan.getScanNodes()) {
            Assertions.assertInstanceOf(LanceScanNode.class, scan);
            Assertions.assertEquals(1, scan.getDesc().getSlots().size());
            Assertions.assertFalse(scan.treeToThrift().getNodes().get(0).getConjuncts().isEmpty());
        }
    }

}
