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
    private final ColumnRefOperator stringRangeColumn =
            new ColumnRefOperator(4, VarcharType.VARCHAR, "string_range_col", true);
    private final ColumnRefOperator floatRangeColumn =
            new ColumnRefOperator(5, FloatType.FLOAT, "float_range_col", true);

    private final ConnectorIndexMetadata metadata = ConnectorIndexMetadata.of(Map.of(
            bitmapColumn.getName(), Set.of(ConnectorIndexType.BITMAP),
            rangeColumn.getName(), Set.of(ConnectorIndexType.RANGE),
            vectorColumn.getName(), Set.of(ConnectorIndexType.VECTOR),
            stringRangeColumn.getName(), Set.of(ConnectorIndexType.RANGE),
            floatRangeColumn.getName(), Set.of(ConnectorIndexType.RANGE)));

    @Test
    public void testConnectorMetadataContract() {
        ConnectorMetadata defaultProvider = new ConnectorMetadata() {
        };
        ConnectorIndexMetadata empty = defaultProvider.getIndexMetadata(new PaimonTable());
        Assertions.assertTrue(empty.isEmpty());
        Assertions.assertSame(empty, ConnectorIndexMetadata.of(Map.of()));
        Assertions.assertTrue(empty.getColumnIndexes().isEmpty());
        Assertions.assertEquals("{}", empty.toString());

        ConnectorMetadata fakeProvider = new ConnectorMetadata() {
            @Override
            public ConnectorIndexMetadata getIndexMetadata(com.starrocks.catalog.Table table) {
                return metadata;
            }
        };
        Assertions.assertTrue(fakeProvider.getIndexMetadata(new PaimonTable())
                .supports("embedding", ConnectorIndexType.VECTOR));
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

        IndexCondition condition = analyzer.getIndexCondition(Utils.compoundAnd(bitmapEquality, rangePredicate));
        Assertions.assertNotNull(condition);
        Assertions.assertEquals(Map.of(
                bitmapColumn.getName(), ConnectorIndexType.BITMAP,
                rangeColumn.getName(), ConnectorIndexType.RANGE), condition.getRequiredIndexes());

        ScalarOperator original = Utils.compoundAnd(bitmapEquality, unsupportedBitmapRange);
        Assertions.assertEquals(bitmapEquality, analyzer.getIndexPredicate(original));
        Assertions.assertEquals(Utils.compoundAnd(bitmapEquality, unsupportedBitmapRange), original);
    }

    @Test
    public void testScalarPredicateFamiliesAndEdgeCases() {
        IndexAnalyzer analyzer = new IndexAnalyzer(metadata);
        BinaryPredicateOperator bitmapEquality =
                new BinaryPredicateOperator(BinaryType.EQ, bitmapColumn, ConstantOperator.createInt(7));

        Assertions.assertNull(analyzer.getIndexPredicate(null));
        Assertions.assertNull(new IndexAnalyzer(ConnectorIndexMetadata.empty()).getIndexPredicate(bitmapEquality));

        InPredicateOperator inPredicate = new InPredicateOperator(false, bitmapColumn,
                ConstantOperator.createInt(1),
                new CastOperator(IntegerType.BIGINT, ConstantOperator.createInt(2)));
        Assertions.assertEquals(inPredicate, analyzer.getIndexPredicate(inPredicate));

        InPredicateOperator inWithNull = new InPredicateOperator(false, bitmapColumn,
                ConstantOperator.createInt(1), ConstantOperator.createNull(IntegerType.INT));
        Assertions.assertNull(analyzer.getIndexPredicate(inWithNull));

        IsNullPredicateOperator isNullPredicate = new IsNullPredicateOperator(false, bitmapColumn);
        Assertions.assertEquals(isNullPredicate, analyzer.getIndexPredicate(isNullPredicate));

        CallOperator startsWith = new CallOperator(FunctionSet.STARTS_WITH, BooleanType.BOOLEAN,
                List.of(stringRangeColumn, ConstantOperator.createVarchar("prefix")));
        Assertions.assertEquals(startsWith, analyzer.getIndexPredicate(startsWith));

        CallOperator invalidStartsWith = new CallOperator(FunctionSet.STARTS_WITH, BooleanType.BOOLEAN,
                List.of(rangeColumn, ConstantOperator.createVarchar("prefix")));
        Assertions.assertNull(analyzer.getIndexPredicate(invalidStartsWith));

        BinaryPredicateOperator reversedEquality =
                new BinaryPredicateOperator(BinaryType.EQ, ConstantOperator.createInt(7), bitmapColumn);
        Assertions.assertEquals(reversedEquality, analyzer.getIndexPredicate(reversedEquality));

        BinaryPredicateOperator twoColumns =
                new BinaryPredicateOperator(BinaryType.EQ, bitmapColumn, rangeColumn);
        Assertions.assertNull(analyzer.getIndexPredicate(twoColumns));
        Assertions.assertNull(analyzer.getIndexPredicate(new BinaryPredicateOperator(
                BinaryType.EQ, rangeColumn, ConstantOperator.createNull(IntegerType.INT))));
        Assertions.assertNull(analyzer.getIndexPredicate(new BinaryPredicateOperator(
                BinaryType.EQ, floatRangeColumn, ConstantOperator.createFloat(0.0))));
        Assertions.assertNull(analyzer.getIndexPredicate(ConstantOperator.createBoolean(true)));
    }

    @Test
    public void testScalarPredicateRejectsCastsThatChangeSerializedLiteralValue() {
        IndexAnalyzer analyzer = new IndexAnalyzer(metadata);

        BinaryPredicateOperator sameType = new BinaryPredicateOperator(BinaryType.EQ, rangeColumn,
                new CastOperator(IntegerType.INT, ConstantOperator.createInt(7)));
        BinaryPredicateOperator widening = new BinaryPredicateOperator(BinaryType.EQ, rangeColumn,
                new CastOperator(IntegerType.BIGINT, ConstantOperator.createInt(7)));
        BinaryPredicateOperator narrowing = new BinaryPredicateOperator(BinaryType.EQ, rangeColumn,
                new CastOperator(IntegerType.TINYINT, ConstantOperator.createInt(257)));
        BinaryPredicateOperator stringToInteger = new BinaryPredicateOperator(BinaryType.EQ, rangeColumn,
                new CastOperator(IntegerType.INT, ConstantOperator.createVarchar("7")));

        Assertions.assertEquals(sameType, analyzer.getIndexPredicate(sameType));
        Assertions.assertEquals(widening, analyzer.getIndexPredicate(widening));
        Assertions.assertNull(analyzer.getIndexPredicate(narrowing));
        Assertions.assertNull(analyzer.getIndexPredicate(stringToInteger));
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

        ColumnRefOperator unindexed = new ColumnRefOperator(6, ArrayType.ARRAY_FLOAT, "other", true);
        CallOperator unindexedL2 = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(unindexed, queryVector));
        Assertions.assertFalse(analyzer.supportsVectorTopN(unindexedL2, true));
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
}
