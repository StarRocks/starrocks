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
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class IcebergMetadataPartitionsPushDownTest extends TableTestBase {
    private final StarRocksAssert starRocksAssert = new StarRocksAssert();

    public IcebergMetadataPartitionsPushDownTest() throws IOException {
    }

    @BeforeEach
    public void before() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES("
                + "\"type\"=\"iceberg\", "
                + "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", "
                + "\"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createCatalog);

        // Ensure mockedNativeTableB has a current snapshot so planner can resolve it.
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableB;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, dbName);
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return true;
            }
        };
    }

    @AfterEach
    public void after() throws Exception {
        starRocksAssert.dropCatalog("iceberg_catalog");
    }

    private String pushedPredicate(String sql) throws Exception {
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second;
        IcebergMetadataScanNode scanNode = (IcebergMetadataScanNode) plan.getScanNodes().get(0);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        THdfsScanNode tHdfsScanNode = planNode.getHdfs_scan_node();
        return tHdfsScanNode.getSerialized_predicate();
    }

    private static Expression decode(String serialized) {
        return SerializationUtil.deserializeFromBase64(serialized);
    }

    @Test
    public void partitionValueEq_pushedDown() throws Exception {
        String predicate = pushedPredicate(
                "select * from iceberg_catalog.db.tb$partitions where partition_value.k2 = 4");
        Assertions.assertFalse(predicate.isEmpty(), "partition_value.k2 = 4 must be pushed down");
        Expression expr = decode(predicate);
        String s = expr.toString();
        Assertions.assertTrue(s.contains("k2") && s.contains("4"),
                "iceberg expression should reference k2 and value 4, got: " + s);
    }

    @Test
    public void partitionValueIn_pushedDown() throws Exception {
        String predicate = pushedPredicate(
                "select * from iceberg_catalog.db.tb$partitions where partition_value.k2 in (1, 2, 3)");
        Assertions.assertFalse(predicate.isEmpty(), "IN over partition_value must be pushed down");
    }

    @Test
    public void partitionValueOr_allLeavesPartition_pushedDown() throws Exception {
        String predicate = pushedPredicate(
                "select * from iceberg_catalog.db.tb$partitions "
                        + "where partition_value.k2 = 4 or partition_value.k2 = 5");
        Assertions.assertFalse(predicate.isEmpty(),
                "OR over only partition_value leaves must be pushed down");
    }

    @Test
    public void nonPartitionPredicate_notPushed() throws Exception {
        String predicate = pushedPredicate(
                "select * from iceberg_catalog.db.tb$partitions where record_count > 0");
        Assertions.assertTrue(predicate.isEmpty(),
                "predicate over a scalar metadata column must not be pushed down");
    }

    @Test
    public void mixedAnd_onlyPartitionLeafPushed() throws Exception {
        // extractConjuncts splits on top-level AND; the partition leaf reaches iceberg,
        // the scalar leaf stays in SR conjuncts.
        String predicate = pushedPredicate(
                "select * from iceberg_catalog.db.tb$partitions "
                        + "where partition_value.k2 = 4 and record_count > 10");
        Assertions.assertFalse(predicate.isEmpty(), "partition leaf must reach iceberg");
        String s = decode(predicate).toString();
        Assertions.assertTrue(s.contains("k2"), "partition leaf must reference k2");
        Assertions.assertFalse(s.contains("record_count"),
                "scalar leaf must not appear in iceberg predicate, got: " + s);
    }

    @Test
    public void mixedOrInsideSingleConjunct_notPushed() throws Exception {
        // OR keeps the whole conjunct as one. With a non-partition leaf inside,
        // allColumnsArePartitionValueSubfields rejects it.
        String predicate = pushedPredicate(
                "select * from iceberg_catalog.db.tb$partitions "
                        + "where partition_value.k2 = 4 or record_count > 10");
        Assertions.assertTrue(predicate.isEmpty(),
                "OR mixing partition_value with a scalar column must not be pushed down");
    }
}
