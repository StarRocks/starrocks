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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.IndexAnalyzer;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PaimonFilteredAnnServiceTest {
    @TempDir
    java.nio.file.Path warehouse;

    @Test
    public void testSnapshotBoundPartitionBitmapAndGlobalLimitAssembly() throws Exception {
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse.toUri())))) {
            catalog.createDatabase("db", true);
            Identifier identifier = Identifier.create("db", "vectors");
            catalog.createTable(identifier, Schema.newBuilder().column("id", DataTypes.INT())
                    .column("pt", DataTypes.STRING())
                    .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT().notNull()).notNull())
                    .partitionKeys("pt").option("bucket", "-1")
                    .option("row-tracking.enabled", "true").option("data-evolution.enabled", "true").build(), false);
            org.apache.paimon.table.Table nativeTable = catalog.getTable(identifier);
            List<Long> snapshots = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                BatchWriteBuilder builder = nativeTable.newBatchWriteBuilder();
                try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
                    write.write(GenericRow.of(i, BinaryString.fromString(i == 1 ? "b" : "a"),
                            new GenericArray(new Float[] {1.0f, 2.0f})));
                    commit.commit(write.prepareCommit());
                }
                snapshots.add(nativeTable.latestSnapshot().orElseThrow().id());
            }
            PaimonTable table = new PaimonTable("paimon", "db", "vectors", List.of(), nativeTable);
            TopNIndexCondition condition = condition("a");
            for (int i : List.of(0, 2)) {
                List<PaimonGlobalIndexRequest> requests = new ArrayList<>();
                PaimonGlobalIndexResult result = new PaimonGlobalIndexService(table, condition, snapshots.get(i),
                        (sql, timeout) -> {
                            assertTrue(sql.endsWith("order by `score` asc limit 3"));
                            String transport = sql.substring(sql.indexOf("where `args`='") + 14);
                            requests.add(PaimonGlobalIndexRequest.parseTransport(
                                    transport.substring(0, transport.indexOf('\''))));
                            TResultBatch batch = new TResultBatch();
                            batch.setRows(List.of(ByteBuffer.wrap("{\"data\":[0,0.1]}".getBytes(StandardCharsets.UTF_8))));
                            return List.of(batch);
                        }).evaluate();
                assertTrue(result.getResult().results().contains(0L));
                assertEquals(3, requests.get(0).getVersion());
                assertEquals(condition.getRequiredIndexes(), requests.get(0).getRequiredIndexes());
                assertEquals(i == 0 ? List.of(List.of(0L, 0L)) : List.of(List.of(0L, 0L), List.of(2L, 2L)),
                        requests.get(0).getPartitionRanges());
            }
            PaimonGlobalIndexResult empty = new PaimonGlobalIndexService(table, condition("missing"), snapshots.get(2),
                    (sql, timeout) -> {
                        throw new AssertionError("Empty partitions must not run ANN");
                    }).evaluate();
            assertTrue(empty.getResult().results().isEmpty());

            catalog.alterTable(identifier, List.of(SchemaChange.addColumn("added", DataTypes.INT())), false);
            PaimonTable evolved = new PaimonTable("paimon", "db", "vectors", List.of(), catalog.getTable(identifier));
            TopNIndexCondition scalarOnly = new TopNIndexCondition(condition.getPredicate(), condition.getScoreExpression(),
                    condition.getRequiredIndexes(), 2, 1, true);
            assertThrows(IllegalStateException.class, () -> new PaimonGlobalIndexService(evolved, scalarOnly, snapshots.get(2),
                    (sql, timeout) -> {
                        throw new AssertionError("Schema mismatch must not run ANN");
                    }).evaluate());
        }
    }

    @Test
    public void testZeroArgumentResidualCallIsRejectedSafely() {
        PaimonTable table = mock(PaimonTable.class);
        when(table.getPartitionColumnNames()).thenReturn(List.of());
        assertNull(PaimonTopNFilter.analyze(table, new CallOperator("unsupported_filter", FloatType.FLOAT, List.of()),
                new IndexAnalyzer(ConnectorIndexMetadata.empty())));
    }

    @Test
    public void testRejectsPrimaryKeyAndNonDataEvolutionTables() {
        FileStoreTable nativeTable = mock(FileStoreTable.class);
        when(nativeTable.rowType()).thenReturn(org.apache.paimon.types.RowType.of(
                new org.apache.paimon.types.DataField(8, "embedding", DataTypes.ARRAY(DataTypes.FLOAT().notNull()).notNull())));
        when(nativeTable.partitionKeys()).thenReturn(List.of());
        PaimonTable table = new PaimonTable("paimon", "db", "vectors", List.of(), nativeTable);
        when(nativeTable.primaryKeys()).thenReturn(List.of());
        when(nativeTable.options()).thenReturn(Map.of("row-tracking.enabled", "true"));
        assertFalse(PaimonTopNFilter.supportsFilteredVector(table, "embedding"));
        when(nativeTable.options()).thenReturn(Map.of("row-tracking.enabled", "true", "data-evolution.enabled", "true"));
        assertTrue(PaimonTopNFilter.supportsFilteredVector(table, "embedding"));
        when(nativeTable.primaryKeys()).thenReturn(List.of("id"));
        assertFalse(PaimonTopNFilter.supportsFilteredVector(table, "embedding"));
    }

    private static TopNIndexCondition condition(String partition) {
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new ColumnRefOperator(1, IntegerType.INT, "id", true), ConstantOperator.createInt(0));
        ScalarOperator partitionPredicate = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(2, VarcharType.VARCHAR, "pt", true), ConstantOperator.createVarchar(partition));
        ScalarOperator score = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(
                new ColumnRefOperator(3, ArrayType.ARRAY_FLOAT, "embedding", true),
                new ArrayOperator(ArrayType.ARRAY_FLOAT, false, List.of(ConstantOperator.createFloat(1),
                        ConstantOperator.createFloat(2)))));
        return new TopNIndexCondition(predicate, score, Map.of("id", ConnectorIndexType.RANGE,
                "embedding", ConnectorIndexType.VECTOR), 2, 1, true, partitionPredicate);
    }
}
