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

import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexShard;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.connector.paimon.PaimonGlobalIndexRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPaimonGlobalIndexScanRange;
import com.starrocks.thrift.TParquetEncryptionInfo;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PaimonIndexScanNodeTest {
    @Test
    public void testIndexRequestDoesNotReuseEncryptionFieldId() throws TException {
        Assertions.assertEquals(46, THdfsScanRange._Fields.PARQUET_ENCRYPTION_INFO.getThriftFieldId());
        Assertions.assertEquals(47, THdfsScanRange._Fields.PAIMON_GLOBAL_INDEX_SCAN_RANGE.getThriftFieldId());

        THdfsScanRange range = new THdfsScanRange();
        range.setParquet_encryption_info(new TParquetEncryptionInfo().setEncryption_algorithm("AES_GCM_V1"));
        range.setPaimon_global_index_scan_range(new TPaimonGlobalIndexScanRange()
                .setProtocol_version(PaimonGlobalIndexRequest.CURRENT_VERSION).setSnapshot_id(42L));
        for (TProtocolFactory protocol : List.of(new TBinaryProtocol.Factory(), new TCompactProtocol.Factory())) {
            THdfsScanRange decoded = new THdfsScanRange();
            new TDeserializer(protocol).deserialize(decoded, new TSerializer(protocol).serialize(range));
            Assertions.assertEquals(range, decoded);
            Assertions.assertEquals("AES_GCM_V1", decoded.getParquet_encryption_info().getEncryption_algorithm());
            Assertions.assertEquals(42L, decoded.getPaimon_global_index_scan_range().getSnapshot_id());
        }
    }

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
    public void testExtractRequestRejectsAmbiguousOrResidualArgsPredicates() {
        ColumnRefOperator args = new ColumnRefOperator(1, VarcharType.VARCHAR, IndexTable.ARGS_COLUMN_NAME, true);
        ColumnRefOperator result = new ColumnRefOperator(
                2, VarbinaryType.VARBINARY, IndexTable.INDEX_RESULT_COLUMN_NAME, true);
        ScalarOperator request = new BinaryPredicateOperator(
                BinaryType.EQ, args, ConstantOperator.createVarchar("request"));
        ScalarOperator residual = new IsNullPredicateOperator(result);
        Assertions.assertEquals("request", PaimonIndexScanNode.extractRequest(Utils.compoundAnd(residual, request)));

        List<ScalarOperator> invalidPredicates = List.of(
                new BinaryPredicateOperator(BinaryType.EQ, args, ConstantOperator.createVarchar("")),
                new BinaryPredicateOperator(BinaryType.EQ, args, ConstantOperator.createNull(VarcharType.VARCHAR)),
                new BinaryPredicateOperator(BinaryType.EQ, args, args),
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, args, ConstantOperator.createVarchar("request")),
                Utils.compoundAnd(request, request),
                Utils.compoundAnd(request, new BinaryPredicateOperator(
                        BinaryType.EQ, args, ConstantOperator.createVarchar("another request"))),
                Utils.compoundAnd(request, new BinaryPredicateOperator(
                        BinaryType.NE, args, ConstantOperator.createVarchar("another request"))),
                Utils.compoundAnd(request, new IsNullPredicateOperator(true, args)),
                Utils.compoundAnd(request, new BinaryPredicateOperator(BinaryType.GT,
                        new CallOperator("length", IntegerType.INT, List.of(args)), ConstantOperator.createInt(0))),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, request, residual),
                residual);
        for (ScalarOperator predicate : invalidPredicates) {
            IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonIndexScanNode.extractRequest(predicate), predicate.toString());
            Assertions.assertTrue(exception.getMessage().contains("exactly one non-empty string"));
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> PaimonIndexScanNode.extractRequest(null));
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
        Map<Integer, IndexType> requiredIndexes = Map.of(7, IndexType.RANGE);
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
                nativeTable.schema().id();
                result = 3L;
                nativeTable.schema().timeMillis();
                result = 1000L;

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

        ColumnRefOperator indexedColumn = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        IndexCondition condition = new IndexCondition(
                new BinaryPredicateOperator(BinaryType.GE, indexedColumn, ConstantOperator.createInt(10)),
                Map.of(1, 7));
        String transport = PaimonGlobalIndexRequest.create(
                new Path("file:///tmp/paimon/orders").toString(), "main", snapshotId,
                3L, 1000L, condition).toTransportString();
        ColumnRefOperator argsColumn = new ColumnRefOperator(
                2, VarcharType.VARCHAR, IndexTable.ARGS_COLUMN_NAME, true);
        ScalarOperator requestPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, argsColumn, ConstantOperator.createVarchar(transport));
        Assertions.assertTrue(scanNode.setupScanRangeLocations(requestPredicate).isEmpty());

        Assertions.assertTrue(scanNode.getTablePath().endsWith("/tmp/paimon/orders"));
        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0L);
        Assertions.assertEquals(2, locations.size());
        THdfsScanRange first = locations.get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(0L, first.getPaimon_global_index_scan_range().getRange_from());
        Assertions.assertEquals(99L, first.getPaimon_global_index_scan_range().getRange_to());
        Assertions.assertEquals(100L, first.getLength());
        Assertions.assertEquals(snapshotId, first.getPaimon_global_index_scan_range().getSnapshot_id());
        Assertions.assertEquals(PaimonGlobalIndexRequest.CURRENT_VERSION,
                first.getPaimon_global_index_scan_range().getProtocol_version());
        THdfsScanRange second = locations.get(1).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(Long.MAX_VALUE, second.getLength());
        Assertions.assertEquals(1, second.getPaimon_global_index_scan_range().getShard_id());
        String staleSchema = PaimonGlobalIndexRequest.create(scanNode.getTablePath(), "main", snapshotId,
                2L, 1000L, condition).toTransportString();
        Assertions.assertThrows(IllegalArgumentException.class, () -> scanNode.setupScanRangeLocations(
                new BinaryPredicateOperator(BinaryType.EQ, argsColumn, ConstantOperator.createVarchar(staleSchema))));

        TPlanNode thriftNode = new TPlanNode();
        scanNode.toThrift(thriftNode);
        Assertions.assertEquals(TPlanNodeType.HDFS_SCAN_NODE, thriftNode.getNode_type());
        Assertions.assertEquals(descriptor.getId().asInt(), thriftNode.getHdfs_scan_node().getTuple_id());
        String explain = scanNode.getNodeExplainString("  ", TExplainLevel.NORMAL);
        Assertions.assertTrue(explain.contains("TABLE: orders$global_index"));
        Assertions.assertTrue(explain.contains("SNAPSHOT: 42"));
        Assertions.assertTrue(explain.contains("SHARDS: 2"));

        PaimonIndexScanNode residualScan = new PaimonIndexScanNode(new PlanNodeId(6), descriptor, indexTable);
        ScalarOperator residual = new IsNullPredicateOperator(new ColumnRefOperator(
                3, VarbinaryType.VARBINARY, IndexTable.INDEX_RESULT_COLUMN_NAME, true));
        Assertions.assertEquals(List.of(residual),
                residualScan.setupScanRangeLocations(Utils.compoundAnd(requestPredicate, residual)));
        Assertions.assertEquals(locations, residualScan.getScanRangeLocations(0L));
    }
}
