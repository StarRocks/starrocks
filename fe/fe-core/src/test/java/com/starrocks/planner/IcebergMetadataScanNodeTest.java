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

package com.starrocks.planner;

import com.starrocks.catalog.Database;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class IcebergMetadataScanNodeTest extends TableTestBase {
    StarRocksAssert starRocksAssert = new StarRocksAssert();

    public IcebergMetadataScanNodeTest() throws IOException {
    }

    @Before
    public void before() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\"type\"=\"iceberg\", " +
                "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createCatalog);
    }

    @After
    public void after() throws Exception {
        starRocksAssert.dropCatalog("iceberg_catalog");
    }

    @Test
    public void testIcebergMetadataScanNode() throws Exception {
        mockedNativeTableC.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableC.refresh();

        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(String dbName) {
                return new Database(1, "db");
            }
        };

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
                return mockedNativeTableC;
            }
        };

        long snapshotId = mockedNativeTableC.currentSnapshot().snapshotId();
        String sql = "explain select file_path from iceberg_catalog.db.tc$logical_iceberg_metadata " +
                "for version as of " + snapshotId + ";";
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second;
        Assert.assertEquals(1, execPlan.getScanNodes().size());
        Assert.assertTrue(execPlan.getScanNodes().get(0) instanceof IcebergMetadataScanNode);
        IcebergMetadataScanNode scanNode = (IcebergMetadataScanNode) execPlan.getScanNodes().get(0);
        List<TScanRangeLocations> result = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(1, result.size());

        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        Assert.assertTrue(planNode.isSetHdfs_scan_node());
        THdfsScanNode tHdfsScanNode = planNode.getHdfs_scan_node();
        Assert.assertTrue(tHdfsScanNode.isSetSerialized_table());
        Assert.assertTrue(tHdfsScanNode.isSetSerialized_predicate());
    }

    @Test(expected = StarRocksPlannerException.class)
    public void testIcebergMetadataScanNodeWithNonSnapshot() throws Exception {
        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(String dbName) {
                return new Database(1, "db");
            }
        };

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
                return mockedNativeTableC;
            }
        };

        String sql = "explain select file_path from iceberg_catalog.db.tc$logical_iceberg_metadata " +
                "for version as of " + "123456777" + ";";
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second;
    }
}
