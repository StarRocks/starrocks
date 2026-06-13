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
import com.starrocks.server.GlobalStateMgr;
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
        LanceTable table = new LanceTable(20001, "vectors_table", columns, "s3://bucket/vectors");
        metadata.addTable("db1", table);

        metadataMgr.registerMockedMetadata(catalogName, metadata);
    }

    @Test
    public void testSelectAll() throws Exception {
        String sql = "SELECT * FROM lance_catalog.db1.vectors_table";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "LanceScanNode");
        assertContains(plan, "TABLE: vectors_table");

        // Verify Thrift scan range params (use_lance_jni_reader and lance_dataset_uri)
        com.starrocks.common.Pair<String, com.starrocks.qe.DefaultCoordinator> pair =
                com.starrocks.utframe.UtFrameUtils.getPlanAndStartScheduling(connectContext, sql);
        java.util.List<com.starrocks.thrift.TScanRangeParams> tScanRangeLocationsList =
                collectAllScanRangeParams(pair.second);
        org.junit.jupiter.api.Assertions.assertFalse(tScanRangeLocationsList.isEmpty());
        com.starrocks.thrift.THdfsScanRange hdfsScanRange = tScanRangeLocationsList.get(0).scan_range.hdfs_scan_range;
        org.junit.jupiter.api.Assertions.assertTrue(hdfsScanRange.isUse_lance_jni_reader());
        org.junit.jupiter.api.Assertions.assertEquals("s3://bucket/vectors", hdfsScanRange.getLance_dataset_uri());
    }

    @Test
    public void testSelectWithPredicate() throws Exception {
        String sql = "SELECT id, embedding FROM lance_catalog.db1.vectors_table WHERE id > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "LanceScanNode");
        assertContains(plan, "TABLE: vectors_table");
        assertContains(plan, "PREDICATES: 1: id > 10");
    }
}
