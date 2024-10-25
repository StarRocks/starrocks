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
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.Pair;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

            @Mock
            boolean tableExists(String dbName, String tableName) {
                return true;
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

            @Mock
            boolean tableExists(String dbName, String tableName) {
                return true;
            }
        };

        String sql = "explain select file_path from iceberg_catalog.db.tc$logical_iceberg_metadata " +
                "for version as of " + "123456777" + ";";
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second;
    }
    
    public void testIcebergMetadataScanNodeScheduler() throws Exception {
        mockedNativeTableC.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableC.newAppend().appendFile(FILE_B_2).commit();
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

            @Mock
            boolean tableExists(String dbName, String tableName) {
                return true;
            }
        };

        String sql = "explain scheduler select file_path from iceberg_catalog.db.tc$logical_iceberg_metadata";
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql);
        List<TScanRangeLocations> scanRangeLocations = pair.second.getFragments().get(1).collectScanNodes()
                .get(new PlanNodeId(0)).getScanRangeLocations(100);
        Assert.assertEquals(2, scanRangeLocations.size());
    }

    public void testIcebergDistributedPlanJobError() {
        mockedNativeTableC.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableC.newAppend().appendFile(FILE_B_2).commit();
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

            @Mock
            boolean tableExists(String dbName, String tableName) {
                return true;
            }
        };

        new MockUp<DefaultCoordinator>() {
            @Mock
            public RowBatch getNext() throws Exception {
                throw new RuntimeException("run failed");
            }

            @Mock
            public void exec() throws Exception {

            }
        };


        String sql = "trace values select * from iceberg_catalog.db.tc";
        starRocksAssert.getCtx().getSessionVariable().setPlanMode("distributed");
        ExceptionChecker.expectThrowsWithMsg(StarRocksPlannerException.class,
                "Failed to execute metadata collection job. run failed",
                () -> UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql));
        starRocksAssert.getCtx().getSessionVariable().setPlanMode("local");
    }

    public void testIcebergDistributedPlanJobBeforeExecError() throws Exception {
        mockedNativeTableC.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableC.newAppend().appendFile(FILE_B_2).commit();
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

        new MockUp<SqlParser>() {
            @Mock
            public StatementBase parseOneWithStarRocksDialect(String originSql, SessionVariable sessionVariable) {
                throw new SemanticException("parse sql failed");
            }
        };


        String sql = "trace values select * from iceberg_catalog.db.tc";
        starRocksAssert.getCtx().getSessionVariable().setPlanMode("distributed");
        ExceptionChecker.expectThrowsWithMsg(StarRocksPlannerException.class,
                "Failed to execute metadata collection job. Getting analyzing error. Detail message: parse sql failed.",
                () -> UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql));
        starRocksAssert.getCtx().getSessionVariable().setPlanMode("local");
    }

    public void testIcebergDistributedPlanParserError() {
        mockedNativeTableC.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableC.newAppend().appendFile(FILE_B_2).commit();
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

        List<java.nio.ByteBuffer> rows = new ArrayList<>();
        rows.add(ByteBuffer.wrap("aaaa".getBytes()));
        TResultBatch resultBatch = new TResultBatch();
        resultBatch.setRows(rows);
        RowBatch rowBatch = new RowBatch();
        rowBatch.setBatch(resultBatch);
        rowBatch.setEos(true);

        new MockUp<DefaultCoordinator>() {
            @Mock
            public RowBatch getNext() throws Exception {
                return rowBatch;
            }

            @Mock
            public void exec() throws Exception {

            }
        };


        String sql = "trace values select * from iceberg_catalog.db.tc";

        starRocksAssert.getCtx().getSessionVariable().setPlanMode("distributed");
        ExceptionChecker.expectThrowsWithMsg(StarRocksPlannerException.class,
                "Failed to parse iceberg file scan task",
                () -> UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql));
        starRocksAssert.getCtx().getSessionVariable().setPlanMode("local");
    }

}
