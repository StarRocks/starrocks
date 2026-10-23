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

package com.starrocks.connector.index;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Coverage;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Operation;
import com.starrocks.connector.index.ConnectorIndexMetadata.TableType;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexAnalyzerTest {
    private final ColumnRefOperator bitmapColumn =
            new ColumnRefOperator(1, IntegerType.INT, "bitmap_col", true);
    private final ColumnRefOperator rangeColumn =
            new ColumnRefOperator(2, IntegerType.INT, "range_col", true);

    private final ConnectorIndexMetadata metadata = ConnectorIndexMetadata.of(
            42, TableType.DATA_EVOLUTION, List.of(
                    descriptor(IndexType.BITMAP, bitmapColumn, Set.of(
                            Operation.EQUAL, Operation.NOT_EQUAL,
                            Operation.IN, Operation.NOT_IN,
                            Operation.IS_NULL)),
                    descriptor(IndexType.RANGE, rangeColumn, Set.of(
                            Operation.EQUAL, Operation.LESS_THAN,
                            Operation.LESS_THAN_OR_EQUAL, Operation.GREATER_THAN,
                            Operation.GREATER_THAN_OR_EQUAL, Operation.IN,
                            Operation.NOT_IN, Operation.STARTS_WITH))));

    @Test
    public void testConnectorMetadataContract() {
        ConnectorMetadata defaultProvider = new ConnectorMetadata() {
        };
        ConnectorIndexMetadata empty = defaultProvider.getIndexMetadata(new PaimonTable(), null);
        Assertions.assertTrue(empty.isEmpty());
        Assertions.assertSame(empty, ConnectorIndexMetadata.of(ConnectorIndexMetadata.UNKNOWN_SNAPSHOT_ID,
                TableType.UNKNOWN, List.of()));
        Assertions.assertTrue(empty.getDescriptors().isEmpty());
        Assertions.assertEquals(ConnectorIndexMetadata.UNKNOWN_SNAPSHOT_ID, empty.getSnapshotId());
        Assertions.assertEquals(TableType.UNKNOWN, empty.getTableType());

        ConnectorMetadata fakeProvider = new ConnectorMetadata() {
            @Override
            public ConnectorIndexMetadata getIndexMetadata(
                    com.starrocks.catalog.Table table, com.starrocks.common.tvr.TvrVersionRange versionRange) {
                return metadata;
            }
        };
        ConnectorIndexMetadata actual = fakeProvider.getIndexMetadata(new PaimonTable(), null);
        Assertions.assertEquals(42, actual.getSnapshotId());
        Assertions.assertEquals(1, actual.getDescriptors("range_col").size());
        Assertions.assertTrue(actual.findDescriptor("range_col", IndexType.RANGE).isPresent());
    }

    @Test
    public void testScalarPredicateRecognitionPreservesResidual() {
        BinaryPredicateOperator bitmapEquality =
                new BinaryPredicateOperator(BinaryType.EQ, bitmapColumn, ConstantOperator.createInt(7));
        BinaryPredicateOperator unsupportedBitmapRange =
                new BinaryPredicateOperator(BinaryType.GT, bitmapColumn, ConstantOperator.createInt(1));
        BinaryPredicateOperator rangePredicate =
                new BinaryPredicateOperator(BinaryType.LE, rangeColumn, ConstantOperator.createInt(9));

        IndexAnalyzer analyzer = new IndexAnalyzer(metadata);
        Assertions.assertEquals(bitmapEquality, analyzer.getIndexPredicate(bitmapEquality));
        Assertions.assertNull(analyzer.getIndexPredicate(unsupportedBitmapRange));
        Assertions.assertEquals(rangePredicate, analyzer.getIndexPredicate(rangePredicate));

        ScalarOperator original = Utils.compoundAnd(bitmapEquality, unsupportedBitmapRange);
        Assertions.assertEquals(bitmapEquality, analyzer.getIndexPredicate(original));
        Assertions.assertEquals(unsupportedBitmapRange, analyzer.analyzePredicate(original).getResidualPredicate());
        Assertions.assertTrue(analyzer.analyzePredicate(original).hasResidual());
        Assertions.assertEquals(Utils.compoundAnd(bitmapEquality, unsupportedBitmapRange), original);
    }

    @Test
    public void testScalarPredicateFamiliesAndEdgeCases() {
        IndexAnalyzer analyzer = new IndexAnalyzer(metadata);
        BinaryPredicateOperator bitmapEquality =
                new BinaryPredicateOperator(BinaryType.EQ, bitmapColumn, ConstantOperator.createInt(7));

        Assertions.assertNull(analyzer.getIndexPredicate(null));
        Assertions.assertNull(new IndexAnalyzer(ConnectorIndexMetadata.empty()).getIndexPredicate(bitmapEquality));
        Assertions.assertTrue(IndexAnalyzer.hasIndexablePredicateShape(bitmapEquality));
        Assertions.assertFalse(IndexAnalyzer.hasIndexablePredicateShape(ConstantOperator.createBoolean(true)));

        InPredicateOperator inPredicate = new InPredicateOperator(false, bitmapColumn,
                ConstantOperator.createInt(1),
                new CastOperator(IntegerType.BIGINT, ConstantOperator.createInt(2)));
        Assertions.assertEquals(inPredicate, analyzer.getIndexPredicate(inPredicate));
        InPredicateOperator bitmapNotIn = new InPredicateOperator(true, bitmapColumn,
                ConstantOperator.createInt(1), ConstantOperator.createInt(2));
        Assertions.assertEquals(bitmapNotIn, analyzer.getIndexPredicate(bitmapNotIn));
        InPredicateOperator rangeNotIn = new InPredicateOperator(true, rangeColumn,
                ConstantOperator.createInt(1), ConstantOperator.createInt(2));
        Assertions.assertEquals(rangeNotIn, analyzer.getIndexPredicate(rangeNotIn));

        InPredicateOperator inWithNull = new InPredicateOperator(false, bitmapColumn,
                ConstantOperator.createInt(1), ConstantOperator.createNull(IntegerType.INT));
        Assertions.assertNull(analyzer.getIndexPredicate(inWithNull));

        IsNullPredicateOperator isNullPredicate = new IsNullPredicateOperator(false, bitmapColumn);
        Assertions.assertEquals(isNullPredicate, analyzer.getIndexPredicate(isNullPredicate));

        CallOperator startsWith = new CallOperator(FunctionSet.STARTS_WITH, BooleanType.BOOLEAN,
                List.of(rangeColumn, ConstantOperator.createVarchar("prefix")));
        Assertions.assertEquals(startsWith, analyzer.getIndexPredicate(startsWith));

        BinaryPredicateOperator reversedEquality =
                new BinaryPredicateOperator(BinaryType.EQ, ConstantOperator.createInt(7), bitmapColumn);
        Assertions.assertEquals(reversedEquality, analyzer.getIndexPredicate(reversedEquality));

        BinaryPredicateOperator twoColumns =
                new BinaryPredicateOperator(BinaryType.EQ, bitmapColumn, rangeColumn);
        Assertions.assertNull(analyzer.getIndexPredicate(twoColumns));
        Assertions.assertNull(analyzer.getIndexPredicate(ConstantOperator.createBoolean(true)));
    }

    @Test
    public void testReversedComparisonsRequireTheNormalizedCapability() {
        Map<BinaryType, Operation> reversedOperations = Map.of(
                BinaryType.EQ, Operation.EQUAL, BinaryType.NE, Operation.NOT_EQUAL,
                BinaryType.LT, Operation.GREATER_THAN, BinaryType.LE, Operation.GREATER_THAN_OR_EQUAL,
                BinaryType.GT, Operation.LESS_THAN, BinaryType.GE, Operation.LESS_THAN_OR_EQUAL);
        reversedOperations.forEach((type, operation) -> {
            IndexAnalyzer analyzer = new IndexAnalyzer(ConnectorIndexMetadata.of(
                    42, TableType.DATA_EVOLUTION,
                    List.of(descriptor(IndexType.RANGE, rangeColumn, Set.of(operation)))));
            ConstantOperator literal = ConstantOperator.createInt(7);
            BinaryPredicateOperator predicate = new BinaryPredicateOperator(type, literal, rangeColumn);
            Assertions.assertSame(predicate, analyzer.getIndexPredicate(predicate), type.name());
            Assertions.assertEquals(type, predicate.getBinaryType());
            Assertions.assertSame(literal, predicate.getChild(0));
            Assertions.assertSame(rangeColumn, predicate.getChild(1));
            if (type != BinaryType.EQ && type != BinaryType.NE) {
                Assertions.assertNull(analyzer.getIndexPredicate(
                        new BinaryPredicateOperator(type, rangeColumn, literal)), type.name());
            }
        });
        Assertions.assertNull(new IndexAnalyzer(metadata).getIndexPredicate(
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, ConstantOperator.createInt(7), rangeColumn)));
    }

    @Test
    public void testScanBindingUsesCurrentFieldIdInsteadOfColumnRefName() {
        ConnectorIndexDescriptor historicalDescriptor = new ConnectorIndexDescriptor(
                IndexType.RANGE, "btree", 7, "old_id", Map.of(), Set.of(Operation.EQUAL), Coverage.FULL);
        ConnectorIndexMetadata renamedMetadata = ConnectorIndexMetadata.of(
                42, TableType.DATA_EVOLUTION, List.of(historicalDescriptor),
                Map.of("renamed_id", 7, "old_id", 8), Set.of());

        // The scan's actual Column binds the stale ColumnRef name to the current stable field id.
        ColumnRefOperator renamedRef = new ColumnRefOperator(10, IntegerType.INT, "old_id", true);
        Column renamedColumn = new Column("renamed_id", IntegerType.INT);
        LogicalPaimonScanOperator renamedScan = new LogicalPaimonScanOperator(new PaimonTable(),
                Map.of(renamedRef, renamedColumn), Map.of(renamedColumn, renamedRef), -1, null);
        ScalarOperator renamedPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, renamedRef, ConstantOperator.createInt(7));
        IndexCondition condition = IndexAnalyzer.forScan(renamedMetadata, renamedScan)
                .getIndexCondition(renamedPredicate);
        Assertions.assertNotNull(condition);
        Assertions.assertSame(renamedPredicate, condition.getPredicate());
        Assertions.assertEquals(Map.of(renamedRef.getId(), 7), condition.getFieldIds());

        // Reusing the indexed name for a different current-schema field must not cross-bind.
        ColumnRefOperator reusedNameRef = new ColumnRefOperator(11, IntegerType.INT, "renamed_id", true);
        Column reusedNameColumn = new Column("old_id", IntegerType.INT);
        LogicalPaimonScanOperator reusedNameScan = new LogicalPaimonScanOperator(new PaimonTable(),
                Map.of(reusedNameRef, reusedNameColumn), Map.of(reusedNameColumn, reusedNameRef), -1, null);
        ScalarOperator reusedNamePredicate = new BinaryPredicateOperator(
                BinaryType.EQ, reusedNameRef, ConstantOperator.createInt(7));
        Assertions.assertNull(IndexAnalyzer.forScan(renamedMetadata, reusedNameScan)
                .getIndexCondition(reusedNamePredicate));
    }

    @Test
    public void testPartitionPredicateIsNotResidual() {
        ColumnRefOperator partitionColumn = new ColumnRefOperator(4, IntegerType.INT, "pt", true);
        ConnectorIndexMetadata partitionedMetadata = ConnectorIndexMetadata.of(
                42, TableType.DATA_EVOLUTION, metadata.getDescriptors(),
                Map.of("bitmap_col", bitmapColumn.getId(), "range_col", rangeColumn.getId(),
                        "pt", partitionColumn.getId()),
                Set.of(partitionColumn.getId()));
        BinaryPredicateOperator partitionPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, partitionColumn, ConstantOperator.createInt(20260928));
        BinaryPredicateOperator indexedPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, bitmapColumn, ConstantOperator.createInt(7));
        BinaryPredicateOperator residualPredicate = new BinaryPredicateOperator(
                BinaryType.GT, bitmapColumn, ConstantOperator.createInt(1));

        IndexAnalyzer.PredicateAnalysis analysis = new IndexAnalyzer(partitionedMetadata).analyzePredicate(
                Utils.compoundAnd(List.of(partitionPredicate, indexedPredicate, residualPredicate)));
        Assertions.assertEquals(partitionPredicate, analysis.getPartitionPredicate());
        Assertions.assertEquals(indexedPredicate, analysis.getIndexPredicate());
        Assertions.assertEquals(residualPredicate, analysis.getResidualPredicate());
        Assertions.assertTrue(analysis.hasResidual());
    }

    @Test
    public void testFunctionOverPartitionColumnRemainsResidual() {
        ColumnRefOperator partitionColumn = new ColumnRefOperator(4, VarcharType.VARCHAR, "dt", true);
        ConnectorIndexMetadata partitionedMetadata = ConnectorIndexMetadata.of(
                42, TableType.DATA_EVOLUTION, metadata.getDescriptors(),
                Map.of("dt", partitionColumn.getId()), Set.of(partitionColumn.getId()));
        CallOperator year = new CallOperator("substr", VarcharType.VARCHAR, List.of(
                partitionColumn, ConstantOperator.createInt(1), ConstantOperator.createInt(4)));
        ScalarOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ, year, ConstantOperator.createVarchar("2024"));

        IndexAnalyzer.PredicateAnalysis analysis =
                new IndexAnalyzer(partitionedMetadata).analyzePredicate(predicate);
        Assertions.assertNull(analysis.getPartitionPredicate());
        Assertions.assertEquals(predicate, analysis.getResidualPredicate());
        Assertions.assertTrue(analysis.hasResidual());
    }

    @Test
    public void testIndexConditionPropagatesWithoutChangingScanSemantics() {
        Column column = new Column(bitmapColumn.getName(), IntegerType.INT);
        BinaryPredicateOperator predicate =
                new BinaryPredicateOperator(BinaryType.EQ, bitmapColumn, ConstantOperator.createInt(7));
        LogicalPaimonScanOperator scan = new LogicalPaimonScanOperator(new PaimonTable(),
                Map.of(bitmapColumn, column), Map.of(column, bitmapColumn), -1, predicate);
        IndexCondition condition = new IndexCondition(predicate);

        LogicalPaimonScanOperator annotated = new LogicalPaimonScanOperator.Builder()
                .withOperator(scan)
                .setIndexCondition(condition)
                .build();
        PhysicalPaimonScanOperator physical = new PhysicalPaimonScanOperator(annotated);
        LogicalPaimonScanOperator logicalCopy = new LogicalPaimonScanOperator.Builder()
                .withOperator(annotated)
                .build();
        PhysicalPaimonScanOperator physicalCopy = new PhysicalPaimonScanOperator(logicalCopy);

        Assertions.assertSame(predicate, annotated.getPredicate());
        Assertions.assertEquals(condition, physical.getIndexCondition());
        Assertions.assertTrue(LogicalPlanPrinter.print(OptExpression.create(annotated)).contains("index["));
        Assertions.assertTrue(LogicalPlanPrinter.print(OptExpression.create(annotated), false, true)
                .contains("index["));
        Assertions.assertTrue(LogicalPlanPrinter.print(OptExpression.create(physical)).contains("index["));
        Assertions.assertTrue(physical.getUsedColumns().contains(bitmapColumn));
        Assertions.assertTrue(new IndexCondition(null).getUsedColumns().isEmpty());
        Assertions.assertEquals(annotated, logicalCopy);
        Assertions.assertEquals(annotated.hashCode(), logicalCopy.hashCode());
        Assertions.assertEquals(physical, physicalCopy);
        Assertions.assertEquals(physical.hashCode(), physicalCopy.hashCode());
    }

    private ConnectorIndexDescriptor descriptor(IndexType type, ColumnRefOperator column,
                                                Set<Operation> operations) {
        return new ConnectorIndexDescriptor(type, "test", column.getId(), column.getName(), Map.of(), operations,
                Coverage.FULL);
    }
}
