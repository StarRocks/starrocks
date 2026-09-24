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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ApplyConnectorIndexRuleTest {
    private final ColumnRefOperator idColumn =
            new ColumnRefOperator(1, IntegerType.INT, "id", true);
    private final ColumnRefOperator vectorColumn =
            new ColumnRefOperator(2, ArrayType.ARRAY_FLOAT, "embedding", false);

    @Test
    public void testPredicateRuleAnnotatesScanWithoutRemovingPredicate() {
        BinaryPredicateOperator predicate =
                new BinaryPredicateOperator(BinaryType.EQ, idColumn, ConstantOperator.createInt(7));
        LogicalPaimonScanOperator scan = createScan(predicate, null);
        ConnectorIndexMetadata metadata = ConnectorIndexMetadata.of(
                Map.of(idColumn.getName(), Set.of(ConnectorIndexType.BITMAP)));
        ApplyPredicateIndexRule rule = new ApplyPredicateIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> metadata);
        OptExpression input = OptExpression.create(scan);

        Assertions.assertTrue(rule.check(input, null));
        List<OptExpression> outputs = rule.transform(input, null);

        Assertions.assertEquals(1, outputs.size());
        LogicalScanOperator annotated = (LogicalScanOperator) outputs.get(0).getOp();
        Assertions.assertSame(predicate, annotated.getPredicate());
        Assertions.assertEquals(
                new IndexCondition(predicate, Map.of("id", ConnectorIndexType.BITMAP)),
                annotated.getIndexCondition());
    }

    @Test
    public void testTopNRulePreservesTopNScoreAndScanSemantics() {
        ColumnRefOperator scoreColumn = new ColumnRefOperator(3, FloatType.FLOAT, "score", true);
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator scoreExpression = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        Projection projection = new Projection(Map.of(
                idColumn, idColumn,
                vectorColumn, vectorColumn,
                scoreColumn, scoreExpression));
        LogicalPaimonScanOperator scan = createScan(null, projection);
        LogicalTopNOperator topN = new LogicalTopNOperator(
                List.of(new Ordering(scoreColumn, true, false)), 10, 2);
        ConnectorIndexMetadata metadata = ConnectorIndexMetadata.of(Map.of(
                idColumn.getName(), Set.of(ConnectorIndexType.BITMAP),
                vectorColumn.getName(), Set.of(ConnectorIndexType.VECTOR)));
        ApplyTopNIndexRule rule = new ApplyTopNIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> metadata);
        OptExpression input = OptExpression.create(topN, OptExpression.create(scan));

        Assertions.assertTrue(rule.check(input, null));
        List<OptExpression> outputs = rule.transform(input, null);

        Assertions.assertEquals(1, outputs.size());
        Assertions.assertSame(topN, outputs.get(0).getOp());
        LogicalScanOperator annotated = (LogicalScanOperator) outputs.get(0).inputAt(0).getOp();
        Assertions.assertNull(annotated.getPredicate());
        Assertions.assertSame(projection, annotated.getProjection());
        TopNIndexCondition condition = (TopNIndexCondition) annotated.getIndexCondition();
        Assertions.assertNull(condition.getPredicate());
        Assertions.assertSame(scoreExpression, condition.getScoreExpression());
        Assertions.assertEquals(10, condition.getLimit());
        Assertions.assertEquals(2, condition.getOffset());
        Assertions.assertEquals(12, condition.getCandidateLimit());
        Assertions.assertTrue(condition.isAscending());
        Assertions.assertTrue(condition.getUsedColumns().contains(vectorColumn));
        Assertions.assertEquals(Map.of("embedding", ConnectorIndexType.VECTOR), condition.getRequiredIndexes());

        TopNIndexCondition equivalent = new TopNIndexCondition(null, scoreExpression,
                Map.of(vectorColumn.getName(), ConnectorIndexType.VECTOR), 10, 2, true);
        Assertions.assertEquals(condition, equivalent);
        Assertions.assertEquals(condition.hashCode(), equivalent.hashCode());
        Assertions.assertNotEquals(condition, new IndexCondition(null));
        Assertions.assertNotEquals(condition,
                new TopNIndexCondition(null, scoreExpression,
                        Map.of(vectorColumn.getName(), ConnectorIndexType.VECTOR), 10, 3, true));
        Assertions.assertTrue(condition.toString().contains("score="));

        TopNIndexCondition fullTextCondition = new TopNIndexCondition(null, scoreExpression,
                Map.of("body", ConnectorIndexType.FULL_TEXT), 10, 2, false);
        Assertions.assertEquals(Map.of("body", ConnectorIndexType.FULL_TEXT),
                fullTextCondition.getRequiredIndexes());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new TopNIndexCondition(null, scoreExpression, Map.of(), 10, 2, false));

        ColumnRefOperator doubleScoreColumn = new ColumnRefOperator(5, FloatType.DOUBLE, "double_score", true);
        CastOperator castedScore = new CastOperator(FloatType.DOUBLE, scoreExpression);
        Projection castedProjection = new Projection(Map.of(doubleScoreColumn, castedScore));
        LogicalPaimonScanOperator castedScan = createScan(null, castedProjection);
        LogicalTopNOperator castedTopN = new LogicalTopNOperator(
                List.of(new Ordering(doubleScoreColumn, true, false)), 10, 2);
        List<OptExpression> castedOutputs = rule.transform(
                OptExpression.create(castedTopN, OptExpression.create(castedScan)), null);
        Assertions.assertEquals(1, castedOutputs.size());
        Assertions.assertSame(castedTopN, castedOutputs.get(0).getOp());
        LogicalScanOperator castedAnnotated = (LogicalScanOperator) castedOutputs.get(0).inputAt(0).getOp();
        Assertions.assertSame(castedProjection, castedAnnotated.getProjection());
        Assertions.assertSame(scoreExpression,
                ((TopNIndexCondition) castedAnnotated.getIndexCondition()).getScoreExpression());
    }

    @Test
    public void testTopNRuleDefersFilteredAndNullableVectorQueries() {
        BinaryPredicateOperator predicate =
                new BinaryPredicateOperator(BinaryType.EQ, idColumn, ConstantOperator.createInt(7));
        ColumnRefOperator nullableVector =
                new ColumnRefOperator(4, ArrayType.ARRAY_FLOAT, "nullable_embedding", true);
        ColumnRefOperator scoreColumn = new ColumnRefOperator(3, FloatType.FLOAT, "score", true);
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator scoreExpression = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        Projection projection = new Projection(Map.of(scoreColumn, scoreExpression));
        ConnectorIndexMetadata metadata = ConnectorIndexMetadata.of(
                Map.of(vectorColumn.getName(), Set.of(ConnectorIndexType.VECTOR)));
        ApplyTopNIndexRule rule = new ApplyTopNIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> metadata);

        LogicalTopNOperator nullsLast = new LogicalTopNOperator(
                List.of(new Ordering(scoreColumn, true, false)), 10, 0);
        Assertions.assertFalse(rule.check(
                OptExpression.create(nullsLast, OptExpression.create(createScan(predicate, projection))), null));

        CallOperator nullableScore = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(nullableVector, queryVector));
        Projection nullableProjection = new Projection(Map.of(scoreColumn, nullableScore));
        ConnectorIndexMetadata nullableMetadata = ConnectorIndexMetadata.of(
                Map.of(nullableVector.getName(), Set.of(ConnectorIndexType.VECTOR)));
        ApplyTopNIndexRule nullableRule = new ApplyTopNIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> nullableMetadata);
        LogicalTopNOperator nullsFirst = new LogicalTopNOperator(
                List.of(new Ordering(scoreColumn, true, true)), 10, 0);
        OptExpression nullableNullsFirst = OptExpression.create(
                nullsFirst, OptExpression.create(createScan(null, nullableProjection, nullableVector)));
        Assertions.assertTrue(nullableRule.check(nullableNullsFirst, null));
        Assertions.assertTrue(nullableRule.transform(nullableNullsFirst, null).isEmpty());

        OptExpression nullableNullsLast = OptExpression.create(
                nullsLast, OptExpression.create(createScan(null, nullableProjection, nullableVector)));
        Assertions.assertTrue(nullableRule.check(nullableNullsLast, null));
        Assertions.assertTrue(nullableRule.transform(nullableNullsLast, null).isEmpty());
    }

    private LogicalPaimonScanOperator createScan(ScalarOperator predicate, Projection projection) {
        return createScan(predicate, projection, vectorColumn);
    }

    private LogicalPaimonScanOperator createScan(
            ScalarOperator predicate, Projection projection, ColumnRefOperator scanVectorColumn) {
        Column id = new Column(idColumn.getName(), IntegerType.INT);
        Column vector = new Column(scanVectorColumn.getName(), ArrayType.ARRAY_FLOAT);
        LogicalPaimonScanOperator scan = new LogicalPaimonScanOperator(new PaimonTable(),
                Map.of(idColumn, id, scanVectorColumn, vector),
                Map.of(id, idColumn, vector, scanVectorColumn), -1, predicate);
        if (projection == null) {
            return scan;
        }
        return new LogicalPaimonScanOperator.Builder()
                .withOperator(scan)
                .setProjection(projection)
                .build();
    }
}
