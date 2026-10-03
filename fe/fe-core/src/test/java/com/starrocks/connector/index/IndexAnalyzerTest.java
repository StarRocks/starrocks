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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.FloatType;
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
    private final ColumnRefOperator vectorColumn =
            new ColumnRefOperator(3, ArrayType.ARRAY_FLOAT, "embedding", true);

    private final ConnectorIndexMetadata metadata = ConnectorIndexMetadata.of(
            42, ConnectorIndexTableType.DATA_EVOLUTION, List.of(
                    descriptor(ConnectorIndexType.BITMAP, bitmapColumn, Set.of(
                            ConnectorIndexOperation.EQUAL, ConnectorIndexOperation.NOT_EQUAL,
                            ConnectorIndexOperation.IN, ConnectorIndexOperation.NOT_IN,
                            ConnectorIndexOperation.IS_NULL)),
                    descriptor(ConnectorIndexType.RANGE, rangeColumn, Set.of(
                            ConnectorIndexOperation.EQUAL, ConnectorIndexOperation.LESS_THAN,
                            ConnectorIndexOperation.LESS_THAN_OR_EQUAL, ConnectorIndexOperation.GREATER_THAN,
                            ConnectorIndexOperation.GREATER_THAN_OR_EQUAL, ConnectorIndexOperation.IN,
                            ConnectorIndexOperation.NOT_IN, ConnectorIndexOperation.STARTS_WITH)),
                    vectorDescriptor(VectorIndexMetric.L2), vectorDescriptor(VectorIndexMetric.COSINE),
                    vectorDescriptor(VectorIndexMetric.INNER_PRODUCT)));

    @Test
    public void testConnectorMetadataContract() {
        ConnectorMetadata defaultProvider = new ConnectorMetadata() {
        };
        ConnectorIndexMetadata empty = defaultProvider.getIndexMetadata(new PaimonTable(), null);
        Assertions.assertTrue(empty.isEmpty());
        Assertions.assertSame(empty, ConnectorIndexMetadata.of(ConnectorIndexMetadata.UNKNOWN_SNAPSHOT_ID,
                ConnectorIndexTableType.UNKNOWN, List.of()));
        Assertions.assertTrue(empty.getDescriptors().isEmpty());
        Assertions.assertEquals(ConnectorIndexMetadata.UNKNOWN_SNAPSHOT_ID, empty.getSnapshotId());
        Assertions.assertEquals(ConnectorIndexTableType.UNKNOWN, empty.getTableType());

        ConnectorMetadata fakeProvider = new ConnectorMetadata() {
            @Override
            public ConnectorIndexMetadata getIndexMetadata(
                    com.starrocks.catalog.Table table, com.starrocks.common.tvr.TvrVersionRange versionRange) {
                return metadata;
            }
        };
        ConnectorIndexMetadata actual = fakeProvider.getIndexMetadata(new PaimonTable(), null);
        Assertions.assertEquals(42, actual.getSnapshotId());
        Assertions.assertEquals(3, actual.getDescriptors("embedding").size());
        Assertions.assertTrue(actual.findDescriptor("embedding", ConnectorIndexType.VECTOR).isPresent());
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
    public void testVectorTopNRecognitionRequiresCompatibleDirection() {
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator l2 = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        CallOperator cosine = new CallOperator(FunctionSet.APPROX_COSINE_SIMILARITY, FloatType.FLOAT,
                List.of(queryVector, vectorColumn));

        IndexAnalyzer analyzer = new IndexAnalyzer(metadata);
        Assertions.assertFalse(analyzer.supportsVectorTopN(ConstantOperator.createFloat(1), true));
        Assertions.assertTrue(analyzer.supportsVectorTopN(l2, true));
        Assertions.assertFalse(analyzer.supportsVectorTopN(l2, false));
        Assertions.assertTrue(analyzer.supportsVectorTopN(cosine, false));
        Assertions.assertFalse(analyzer.supportsVectorTopN(cosine, true));

        CallOperator innerProduct = new CallOperator(FunctionSet.APPROX_INNER_PRODUCT, FloatType.FLOAT,
                List.of(vectorColumn, new CastOperator(ArrayType.ARRAY_FLOAT, queryVector)));
        Assertions.assertTrue(analyzer.supportsVectorTopN(innerProduct, false));
        Assertions.assertTrue(analyzer.supportsVectorTopN(new CastOperator(FloatType.DOUBLE, l2), true));

        CallOperator unsupported = new CallOperator("unsupported_distance", FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        Assertions.assertFalse(analyzer.supportsVectorTopN(unsupported, true));

        ColumnRefOperator unindexed = new ColumnRefOperator(4, ArrayType.ARRAY_FLOAT, "other", true);
        CallOperator unindexedL2 = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(unindexed, queryVector));
        Assertions.assertFalse(analyzer.supportsVectorTopN(unindexedL2, true));

        ConnectorIndexMetadata cosineOnly = ConnectorIndexMetadata.of(
                42, ConnectorIndexTableType.DATA_EVOLUTION, List.of(vectorDescriptor(VectorIndexMetric.COSINE)));
        Assertions.assertFalse(new IndexAnalyzer(cosineOnly).supportsVectorTopN(l2, true));

        IndexAnalyzer.VectorTopNShape shape = IndexAnalyzer.analyzeVectorTopNShape(l2, true).orElseThrow();
        float[] vector = shape.getQueryVector();
        vector[0] = 99;
        Assertions.assertArrayEquals(new float[] {1, 2}, shape.getQueryVector());

        ArrayOperator emptyVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false, List.of());
        Assertions.assertTrue(IndexAnalyzer.analyzeVectorTopNShape(new CallOperator(
                FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(vectorColumn, emptyVector)), true).isEmpty());
        ArrayOperator nullVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createNull(FloatType.FLOAT)));
        Assertions.assertTrue(IndexAnalyzer.analyzeVectorTopNShape(new CallOperator(
                FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(vectorColumn, nullVector)), true).isEmpty());
        ArrayOperator nanVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createObject(Double.NaN, FloatType.FLOAT)));
        Assertions.assertTrue(IndexAnalyzer.analyzeVectorTopNShape(new CallOperator(
                FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(vectorColumn, nanVector)), true).isEmpty());
    }

    @Test
    public void testVectorTopNRequiresMatchingDimensionAndNullSemantics() {
        ArrayOperator threeDimensions = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2),
                        ConstantOperator.createFloat(3)));
        CallOperator wrongDimension = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, threeDimensions));
        Assertions.assertFalse(new IndexAnalyzer(metadata).supportsVectorTopN(wrongDimension, true));

        ConnectorIndexDescriptor nullableDescriptor = new ConnectorIndexDescriptor(
                ConnectorIndexType.VECTOR, "test", vectorColumn.getId(), vectorColumn.getName(), true, false,
                Map.of(ConnectorIndexDescriptor.OPTION_METRIC, VectorIndexMetric.L2.name(),
                        ConnectorIndexDescriptor.OPTION_DIMENSION, "2"),
                Set.of(ConnectorIndexOperation.VECTOR_TOP_N), ConnectorIndexCoverage.FULL);
        IndexAnalyzer nullableAnalyzer = new IndexAnalyzer(ConnectorIndexMetadata.of(
                42, ConnectorIndexTableType.DATA_EVOLUTION, List.of(nullableDescriptor)));
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator score = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        IndexAnalyzer.VectorTopNShape shape = IndexAnalyzer.analyzeVectorTopNShape(score, true).orElseThrow();

        // ANN candidates omit NULL vectors. Until execution can replenish those rows, neither
        // ANN cannot represent leading nulls. NULLS LAST is still a valid request, but the
        // execution layer must replenish trailing null rows or fall back when candidates run short.
        Assertions.assertTrue(nullableAnalyzer.findVectorIndex(shape, true).isEmpty());
        Assertions.assertTrue(nullableAnalyzer.findVectorIndex(shape, false).isPresent());
        Assertions.assertTrue(new IndexAnalyzer(metadata).findVectorIndex(shape, true).isPresent());
    }

    @Test
    public void testScanBindingUsesCurrentFieldIdInsteadOfColumnRefName() {
        ConnectorIndexDescriptor historicalDescriptor = new ConnectorIndexDescriptor(
                ConnectorIndexType.VECTOR, "test", 7, "old_embedding", false, false,
                Map.of(ConnectorIndexDescriptor.OPTION_METRIC, VectorIndexMetric.L2.name(),
                        ConnectorIndexDescriptor.OPTION_DIMENSION, "2"),
                Set.of(ConnectorIndexOperation.VECTOR_TOP_N), ConnectorIndexCoverage.FULL);
        ConnectorIndexMetadata renamedMetadata = ConnectorIndexMetadata.of(
                42, ConnectorIndexTableType.DATA_EVOLUTION, List.of(historicalDescriptor),
                Map.of("renamed_embedding", 7, "old_embedding", 8), Set.of());
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));

        // The ColumnRef deliberately keeps the old name. The scan's actual Column is the source
        // of truth and binds it to field id 7 in the current schema.
        ColumnRefOperator renamedRef = new ColumnRefOperator(10, ArrayType.ARRAY_FLOAT, "old_embedding", true);
        Column renamedColumn = new Column("renamed_embedding", ArrayType.ARRAY_FLOAT);
        LogicalPaimonScanOperator renamedScan = new LogicalPaimonScanOperator(new PaimonTable(),
                Map.of(renamedRef, renamedColumn), Map.of(renamedColumn, renamedRef), -1, null);
        CallOperator renamedScore = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(renamedRef, queryVector));
        Assertions.assertTrue(IndexAnalyzer.forScan(renamedMetadata, renamedScan)
                .supportsVectorTopN(renamedScore, true));

        // Conversely, a ColumnRef name that happens to match the renamed indexed column must not
        // cross-bind when its actual current-schema Column owns a different field id.
        ColumnRefOperator reusedNameRef = new ColumnRefOperator(
                11, ArrayType.ARRAY_FLOAT, "renamed_embedding", true);
        Column reusedNameColumn = new Column("old_embedding", ArrayType.ARRAY_FLOAT);
        LogicalPaimonScanOperator reusedNameScan = new LogicalPaimonScanOperator(new PaimonTable(),
                Map.of(reusedNameRef, reusedNameColumn), Map.of(reusedNameColumn, reusedNameRef), -1, null);
        CallOperator reusedNameScore = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(reusedNameRef, queryVector));
        Assertions.assertFalse(IndexAnalyzer.forScan(renamedMetadata, reusedNameScan)
                .supportsVectorTopN(reusedNameScore, true));
    }

    @Test
    public void testPartitionPredicateIsNotResidual() {
        ColumnRefOperator partitionColumn = new ColumnRefOperator(4, IntegerType.INT, "pt", true);
        ConnectorIndexMetadata partitionedMetadata = ConnectorIndexMetadata.of(
                42, ConnectorIndexTableType.DATA_EVOLUTION, metadata.getDescriptors(),
                Map.of("bitmap_col", bitmapColumn.getId(), "range_col", rangeColumn.getId(),
                        "embedding", vectorColumn.getId(), "pt", partitionColumn.getId()),
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
                42, ConnectorIndexTableType.DATA_EVOLUTION, metadata.getDescriptors(),
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

    private ConnectorIndexDescriptor descriptor(ConnectorIndexType type, ColumnRefOperator column,
                                                Set<ConnectorIndexOperation> operations) {
        return new ConnectorIndexDescriptor(type, "test", column.getId(), column.getName(), Map.of(), operations,
                ConnectorIndexCoverage.FULL);
    }

    private ConnectorIndexDescriptor vectorDescriptor(VectorIndexMetric metric) {
        return new ConnectorIndexDescriptor(ConnectorIndexType.VECTOR, "test", vectorColumn.getId(),
                vectorColumn.getName(), false, false, Map.of(
                ConnectorIndexDescriptor.OPTION_METRIC, metric.name(),
                ConnectorIndexDescriptor.OPTION_DIMENSION, "2"),
                Set.of(ConnectorIndexOperation.VECTOR_TOP_N), ConnectorIndexCoverage.FULL);
    }
}
