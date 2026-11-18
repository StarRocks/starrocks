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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonGlobalIndexPlannerTest {
    private static final ColumnRefOperator KEY = new ColumnRefOperator(101, IntegerType.INT, "key", true);
    private static final ColumnRefOperator PART = new ColumnRefOperator(102, IntegerType.INT, "part", true);
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(103, VarcharType.VARCHAR, "text", true);
    private static final ColumnRefOperator NUMBER = new ColumnRefOperator(104, FloatType.DOUBLE, "number", true);
    private static final Map<Integer, String> COLUMNS = Map.of(101, "key", 102, "part", 103, "text", 104, "number");

    private static FileStoreTable table(String keyName, int keyId) {
        TableSchema schema = new TableSchema(3, List.of(DataTypes.FIELD(keyId, keyName, DataTypes.INT()),
                DataTypes.FIELD(8, "part", DataTypes.INT()), DataTypes.FIELD(9, "text", DataTypes.STRING()),
                DataTypes.FIELD(10, "number", DataTypes.DOUBLE())), 20, List.of("part"), List.of(),
                Map.of("data-evolution.enabled", "true"), "");
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.schema()).thenReturn(schema);
        when(table.options()).thenReturn(schema.options());
        when(table.location()).thenReturn(new Path("file:///warehouse/db.db/table"));
        return table;
    }

    private static FileStoreTable table() {
        return table("key", 7);
    }

    private static ScalarOperator equal(ScalarOperator column) {
        return new BinaryPredicateOperator(BinaryType.EQ, column, ConstantOperator.createInt(5));
    }

    private static ScalarOperator or(ScalarOperator left, ScalarOperator right) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, left, right);
    }

    private static IndexManifestEntry index(int fieldId, String provider) {
        GlobalIndexMeta global = new GlobalIndexMeta(0, 99, fieldId, new int[0], new byte[0]);
        IndexFileMeta file = new IndexFileMeta(provider, "index-" + fieldId, 100L, 100L, global, null);
        return new IndexManifestEntry(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, file);
    }

    private static void discover(FileStoreTable table, List<IndexManifestEntry> indexes) {
        Snapshot snapshot = mock(Snapshot.class);
        SnapshotManager snapshots = mock(SnapshotManager.class);
        FileStore<?> store = mock(FileStore.class);
        IndexFileHandler handler = mock(IndexFileHandler.class);
        when(table.snapshotManager()).thenReturn(snapshots);
        when(snapshots.snapshot(1L)).thenReturn(snapshot);
        doReturn(store).when(table).store();
        when(store.newIndexFileHandler()).thenReturn(handler);
        when(handler.scan(eq(snapshot), org.mockito.ArgumentMatchers.<Filter<IndexManifestEntry>>any()))
                .thenReturn(indexes);
    }

    private static void assertNoDiscovery(FileStoreTable table) {
        verify(table, never()).snapshotManager();
        verify(table, never()).store();
    }

    private static JsonObject json(PaimonGlobalIndexRequest request) {
        assertNotNull(request);
        return JsonParser.parseString(request.toJson()).getAsJsonObject();
    }

    @Test
    void testPartitionOnlySkipsDiscovery() {
        FileStoreTable table = table();
        assertNull(PaimonGlobalIndexPlanner.request(table, 1, equal(PART), COLUMNS));
        assertNoDiscovery(table);
    }

    @Test
    void testUnsupportedExpressionsSkipDiscovery() {
        List<ScalarOperator> unsupported = List.of(
                new LikePredicateOperator(TEXT, ConstantOperator.createVarchar("prefix%")),
                equal(new CallOperator("abs", IntegerType.INT, List.of(KEY))),
                equal(new CastOperator(IntegerType.BIGINT, KEY)),
                new BinaryPredicateOperator(BinaryType.EQ, KEY, PART),
                new BinaryPredicateOperator(BinaryType.EQ, KEY, ConstantOperator.createNull(IntegerType.INT)),
                new BinaryPredicateOperator(BinaryType.EQ, KEY, ConstantOperator.createVarchar("5")),
                new BinaryPredicateOperator(BinaryType.EQ, NUMBER, ConstantOperator.createDouble(5)),
                new InPredicateOperator(false, KEY, ConstantOperator.createNull(IntegerType.INT)),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, equal(KEY)));
        for (ScalarOperator predicate : unsupported) {
            FileStoreTable table = table();
            assertNull(PaimonGlobalIndexPlanner.request(table, 1, predicate, COLUMNS), predicate.toString());
            assertNoDiscovery(table);
        }
    }

    @Test
    void testUnusableOrSkipsDiscoveryAsAWhole() {
        for (ScalarOperator residual : List.of(equal(PART),
                new LikePredicateOperator(TEXT, ConstantOperator.createVarchar("a%")))) {
            FileStoreTable table = table();
            assertNull(PaimonGlobalIndexPlanner.request(table, 1, or(equal(KEY), residual), COLUMNS));
            assertNoDiscovery(table);
        }
    }

    @Test
    void testAndKeepsUsableConjunctAndOriginalPredicate() {
        FileStoreTable table = table();
        discover(table, List.of(index(7, "btree")));
        ScalarOperator predicate = Utils.compoundAnd(equal(KEY), equal(PART),
                new LikePredicateOperator(TEXT, ConstantOperator.createVarchar("a%")));
        String original = predicate.toString();
        var request = PaimonGlobalIndexPlanner.request(table, 1, predicate, COLUMNS);
        assertEquals(Set.of(7), request.getRequiredFieldIds());
        assertEquals(json(PaimonGlobalIndexRequest.create(table.location().toString(), "main", 1,
                table.schema().id(), table.schema().timeMillis(), equal(KEY), Map.of(101, 7))).get("predicate"),
                json(request).get("predicate"));
        assertEquals(original, predicate.toString());
        assertEquals(3, Utils.extractConjuncts(predicate).size());
        verify(table.snapshotManager()).snapshot(1L);
        verify(table.store().newIndexFileHandler()).scan(any(Snapshot.class),
                org.mockito.ArgumentMatchers.<Filter<IndexManifestEntry>>any());
    }

    @Test
    void testSupportedPredicateShapesStillDiscoverIndexes() {
        List<ScalarOperator> supported = List.of(equal(KEY),
                new BinaryPredicateOperator(BinaryType.LT, ConstantOperator.createInt(1), KEY),
                new InPredicateOperator(false, KEY, ConstantOperator.createInt(1), ConstantOperator.createInt(2)),
                new InPredicateOperator(true, KEY, ConstantOperator.createInt(1)),
                new IsNullPredicateOperator(false, KEY), new IsNullPredicateOperator(true, KEY),
                new CallOperator("starts_with", BooleanType.BOOLEAN, List.of(TEXT, ConstantOperator.createVarchar("a"))),
                or(equal(KEY), new IsNullPredicateOperator(false, TEXT)));
        for (ScalarOperator predicate : supported) {
            FileStoreTable table = table();
            discover(table, List.of(index(7, "btree"), index(9, "btree")));
            var request = PaimonGlobalIndexPlanner.request(table, 1, predicate, COLUMNS);
            assertNotNull(request, predicate.toString());
            request.validatePredicate(table.schema());
        }
    }

    @Test
    void testOrRequiresEveryReferencedFieldToBeIndexed() {
        FileStoreTable table = table();
        discover(table, List.of(index(7, "btree")));
        ScalarOperator partiallyIndexed = or(equal(KEY), new IsNullPredicateOperator(false, TEXT));
        assertNull(PaimonGlobalIndexPlanner.request(table, 1, partiallyIndexed, COLUMNS));
        var request = PaimonGlobalIndexPlanner.request(table, 1,
                Utils.compoundAnd(equal(KEY), partiallyIndexed), COLUMNS);
        assertEquals(Set.of(7), request.getRequiredFieldIds());
        assertEquals("b", json(request).getAsJsonObject("predicate").get("o").getAsString());
    }

    @Test
    void testMissingOrMixedProviderIndexesAreNotUsed() {
        for (List<IndexManifestEntry> indexes : List.of(List.<IndexManifestEntry>of(),
                List.of(index(7, "btree"), index(7, "bitmap")))) {
            FileStoreTable table = table();
            discover(table, indexes);
            assertNull(PaimonGlobalIndexPlanner.request(table, 1, equal(KEY), COLUMNS));
        }
    }

    @Test
    void testAliasUsesScanColumnAndDoesNotRenameOriginalPredicate() {
        FileStoreTable table = table();
        discover(table, List.of(index(7, "btree")));
        // An alias that happens to match the partition column must still bind to the actual data column.
        ColumnRefOperator alias = new ColumnRefOperator(101, IntegerType.INT, "part", true);
        ScalarOperator predicate = equal(alias);
        var request = PaimonGlobalIndexPlanner.request(table, 1, predicate, COLUMNS);
        assertEquals(Set.of(7), request.getRequiredFieldIds());
        assertEquals(Set.of("key"), json(request).getAsJsonObject("indexes").keySet());
        assertEquals("key", json(request).getAsJsonObject("predicate").getAsJsonArray("c")
                .get(0).getAsJsonObject().get("n").getAsString());
        assertEquals("part", ((ColumnRefOperator) predicate.getChild(0)).getName());
        assertEquals("part", alias.getName());
    }

    @Test
    void testSelfJoinColumnRequiresItsOwnScanBinding() {
        FileStoreTable table = table();
        ColumnRefOperator otherScan = new ColumnRefOperator(201, IntegerType.INT, "key", true);
        assertNull(PaimonGlobalIndexPlanner.request(table, 1, equal(otherScan), COLUMNS));
        assertNull(PaimonGlobalIndexPlanner.request(table, 1, or(equal(KEY), equal(otherScan)), COLUMNS));
        assertNoDiscovery(table);
        discover(table, List.of(index(7, "btree")));
        assertNotNull(PaimonGlobalIndexPlanner.request(table, 1, equal(otherScan), Map.of(201, "key")));
    }

    @Test
    void testRenameUsesCurrentSchemaNameAndStableFieldId() {
        FileStoreTable table = table("renamed", 7);
        assertNull(PaimonGlobalIndexPlanner.request(table, 1, equal(KEY), COLUMNS));
        assertNoDiscovery(table);
        discover(table, List.of(index(7, "btree")));
        var request = PaimonGlobalIndexPlanner.request(table, 1, equal(KEY), Map.of(101, "renamed"));
        assertEquals(Set.of(7), request.getRequiredFieldIds());
        assertEquals(Set.of("renamed"), json(request).getAsJsonObject("indexes").keySet());
    }

    @Test
    void testRecreatedColumnCannotUseOldFieldIndex() {
        FileStoreTable table = table("key", 11);
        discover(table, List.of(index(7, "btree")));
        assertNull(PaimonGlobalIndexPlanner.request(table, 1, equal(KEY), COLUMNS));
    }
}
