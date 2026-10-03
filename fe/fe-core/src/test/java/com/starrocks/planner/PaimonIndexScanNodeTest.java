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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.ConnectorIndexShard;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.connector.paimon.PaimonGlobalIndexRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PaimonIndexScanNodeTest {
    @Test
    public void testExtractRequestAcceptsOnlyStringEquality() {
        ColumnRefOperator args = new ColumnRefOperator(
                1, VarcharType.VARCHAR, IndexTable.ARGS_COLUMN_NAME, true);

        Assertions.assertEquals("request", PaimonIndexScanNode.extractRequest(
                new BinaryPredicateOperator(BinaryType.EQ, args, ConstantOperator.createVarchar("request"))));
        Assertions.assertEquals("request", PaimonIndexScanNode.extractRequest(
                new BinaryPredicateOperator(BinaryType.EQ, ConstantOperator.createVarchar("request"), args)));

        Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonIndexScanNode.extractRequest(
                new BinaryPredicateOperator(BinaryType.NE, args, ConstantOperator.createVarchar("request"))));
        Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonIndexScanNode.extractRequest(
                new BinaryPredicateOperator(BinaryType.EQ, args, ConstantOperator.createInt(1))));
    }

    @Test
    public void testBuildsSnapshotBoundDistributedScanRanges(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked MetadataMgr metadataMgr,
            @Mocked CatalogConnector connector,
            @Mocked ConnectorMetadata connectorMetadata,
            @Mocked PaimonTable table,
            @Mocked FileStoreTable nativeTable) {
        String catalogName = "paimon_catalog";
        long snapshotId = 42L;
        Map<String, ConnectorIndexType> requiredIndexes = Map.of("embedding", ConnectorIndexType.VECTOR);
        new Expectations() {
            {
                table.getId();
                result = 7L;
                table.getCatalogName();
                result = catalogName;
                table.getName();
                result = "orders";
                table.getNativeTable();
                result = nativeTable;
                nativeTable.location();
                result = new Path("file:///tmp/paimon/orders");

                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
                result = connector;
                connector.getMetadata();
                result = connectorMetadata;
                connectorMetadata.getCloudConfiguration();
                result = null;
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                metadataMgr.getOptionalMetadata(catalogName);
                result = Optional.of(connectorMetadata);
                connectorMetadata.getIndexShards(table, snapshotId, requiredIndexes);
                result = List.of(new ConnectorIndexShard(0L, 99L),
                        new ConnectorIndexShard(100L, Long.MAX_VALUE));
            }
        };

        IndexTable indexTable = new IndexTable(table);
        TupleDescriptor descriptor = new TupleDescriptor(new TupleId(3));
        descriptor.setTable(indexTable);
        PaimonIndexScanNode scanNode = new PaimonIndexScanNode(new PlanNodeId(5), descriptor, indexTable);

        ColumnRefOperator indexedColumn = new ColumnRefOperator(
                1, ArrayType.ARRAY_FLOAT, "embedding", false);
        ScalarOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        ScalarOperator score = new CallOperator(FunctionSet.APPROX_COSINE_SIMILARITY, FloatType.FLOAT,
                List.of(indexedColumn, queryVector));
        TopNIndexCondition condition = new TopNIndexCondition(
                null, score, requiredIndexes, 10, 2, false);
        String transport = PaimonGlobalIndexRequest.create(snapshotId, condition).toTransportString();
        ColumnRefOperator argsColumn = new ColumnRefOperator(
                2, VarcharType.VARCHAR, IndexTable.ARGS_COLUMN_NAME, true);
        scanNode.setupScanRangeLocations(new BinaryPredicateOperator(
                BinaryType.EQ, argsColumn, ConstantOperator.createVarchar(transport)));

        Assertions.assertTrue(scanNode.getTablePath().endsWith("/tmp/paimon/orders"));
        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0L);
        Assertions.assertEquals(2, locations.size());
        THdfsScanRange first = locations.get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(0L, first.getPaimon_global_index_scan_range().getRange_from());
        Assertions.assertEquals(99L, first.getPaimon_global_index_scan_range().getRange_to());
        Assertions.assertEquals(100L, first.getLength());
        Assertions.assertEquals(snapshotId, first.getPaimon_global_index_scan_range().getSnapshot_id());
        Assertions.assertEquals(PaimonGlobalIndexRequest.TOP_N_VERSION,
                first.getPaimon_global_index_scan_range().getProtocol_version());
        PaimonGlobalIndexRequest forwarded = PaimonGlobalIndexRequest.parse(
                first.getPaimon_global_index_scan_range().getQuery_json());
        Assertions.assertTrue(forwarded.isTopN());
        Assertions.assertEquals(12, forwarded.getLocalLimit());
        THdfsScanRange second = locations.get(1).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(Long.MAX_VALUE, second.getLength());
        Assertions.assertEquals(1, second.getPaimon_global_index_scan_range().getShard_id());

        TPlanNode thriftNode = new TPlanNode();
        scanNode.toThrift(thriftNode);
        Assertions.assertEquals(TPlanNodeType.HDFS_SCAN_NODE, thriftNode.getNode_type());
        Assertions.assertEquals(descriptor.getId().asInt(), thriftNode.getHdfs_scan_node().getTuple_id());
        String explain = scanNode.getNodeExplainString("  ", TExplainLevel.NORMAL);
        Assertions.assertTrue(explain.contains("TABLE: orders$global_index"));
        Assertions.assertTrue(explain.contains("SNAPSHOT: 42"));
        Assertions.assertTrue(explain.contains("SHARDS: 2"));
    }
}
