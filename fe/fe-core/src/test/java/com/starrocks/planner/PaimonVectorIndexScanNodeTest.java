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
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.ConnectorIndexDescriptor;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Coverage;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Operation;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexMetadata.TableType;
import com.starrocks.connector.index.ConnectorIndexShard;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.connector.index.VectorIndexMetric;
import com.starrocks.connector.paimon.PaimonGlobalIndexRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FloatType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PaimonVectorIndexScanNodeTest {
    @Test
    public void testV4PreservesSchemaIdentityAndValidatesCanonicalVectorBinding(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked MetadataMgr metadataMgr,
            @Mocked CatalogConnector connector,
            @Mocked ConnectorMetadata connectorMetadata,
            @Mocked PaimonTable table,
            @Mocked FileStoreTable nativeTable) {
        String tablePath = new Path("file:///tmp/paimon/vectors").toString();
        DataField vectorField = new DataField(7, "embedding", new ArrayType(false, new FloatType(false)));
        ConnectorIndexDescriptor vectorIndex = new ConnectorIndexDescriptor(
                IndexType.VECTOR, "lumina", 7, "embedding", false, false,
                Map.of(ConnectorIndexDescriptor.OPTION_METRIC, "L2", ConnectorIndexDescriptor.OPTION_DIMENSION, "2"),
                Set.of(Operation.VECTOR_TOP_N), Coverage.FULL);
        new Expectations() {
            {
                table.getId();
                result = 7L;
                table.getCatalogName();
                result = "paimon";
                table.getNativeTable();
                result = nativeTable;
                nativeTable.location();
                result = new Path(tablePath);
                nativeTable.schema().id();
                result = 3L;
                nativeTable.schema().timeMillis();
                result = 1000L;
                nativeTable.schema().idToFieldMap();
                result = Map.of(7, vectorField);
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector("paimon");
                result = connector;
                connector.getMetadata();
                result = connectorMetadata;
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                metadataMgr.getOptionalMetadata("paimon");
                result = java.util.Optional.of(connectorMetadata);
                connectorMetadata.getIndexMetadata(table, TvrTableSnapshot.of(42L));
                result = ConnectorIndexMetadata.of(42L, TableType.DATA_EVOLUTION, List.of(vectorIndex));
                connectorMetadata.getIndexShards(table, 42L, Map.of(7, IndexType.VECTOR));
                result = List.of(new ConnectorIndexShard(0, 99));
            }
        };
        IndexTable indexTable = new IndexTable(table);
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(3));
        tuple.setTable(indexTable);
        PaimonIndexScanNode scan = new PaimonIndexScanNode(new PlanNodeId(5), tuple, indexTable);
        scan.setupScanRangeLocations(request(tablePath, 7, "embedding", VectorIndexMetric.L2, 2, 3L));
        var range = scan.getScanRangeLocations(0).get(0).getScan_range().getHdfs_scan_range()
                .getPaimon_global_index_scan_range();
        Assertions.assertEquals(4, range.getProtocol_version());
        Assertions.assertEquals(42L, range.getSnapshot_id());
        Assertions.assertTrue(range.getQuery_json().contains("\"schemaTimeMillis\":1000"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> scan.setupScanRangeLocations(
                request(tablePath, 8, "embedding", VectorIndexMetric.L2, 2, 3L)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> scan.setupScanRangeLocations(
                request(tablePath, 7, "renamed", VectorIndexMetric.L2, 2, 3L)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> scan.setupScanRangeLocations(
                request(tablePath, 7, "embedding", VectorIndexMetric.COSINE, 2, 3L)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> scan.setupScanRangeLocations(
                request(tablePath, 7, "embedding", VectorIndexMetric.L2, 3, 3L)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> scan.setupScanRangeLocations(
                request(tablePath, 7, "embedding", VectorIndexMetric.L2, 2, 2L)));
    }

    private static ScalarOperator request(String path, int fieldId, String name,
                                          VectorIndexMetric metric, int dimension, long schemaId) {
        TopNIndexCondition condition = new TopNIndexCondition(null, null, ConstantOperator.createFloat(1),
                fieldId, name, new float[dimension], metric, 12, 10, 2, metric == VectorIndexMetric.L2, false);
        String encoded = PaimonGlobalIndexRequest.create(path, "main", 42L, schemaId, 1000L, condition)
                .toTransportString();
        return new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, VarcharType.VARCHAR, IndexTable.ARGS_COLUMN_NAME, false),
                ConstantOperator.createVarchar(encoded));
    }
}
