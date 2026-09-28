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
import com.starrocks.connector.index.ConnectorIndexCoverage;
import com.starrocks.connector.index.ConnectorIndexDescriptor;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexOperation;
import com.starrocks.connector.index.ConnectorIndexTableType;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.connector.index.VectorIndexMetric;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
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
import java.util.concurrent.atomic.AtomicInteger;

public class ApplyConnectorIndexRuleTest {
    private final ColumnRefOperator idColumn =
            new ColumnRefOperator(1, IntegerType.INT, "id", true);
    private final ColumnRefOperator vectorColumn =
            new ColumnRefOperator(2, ArrayType.ARRAY_FLOAT, "embedding", true);

    @Test
    public void testPredicateRuleAnnotatesScanWithoutRemovingPredicate() {
        BinaryPredicateOperator predicate =
                new BinaryPredicateOperator(BinaryType.EQ, idColumn, ConstantOperator.createInt(7));
        LogicalPaimonScanOperator scan = createScan(predicate, null);
        ConnectorIndexMetadata metadata = metadata(List.of(scalarDescriptor()));
        ApplyPredicateIndexRule rule = new ApplyPredicateIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> metadata);
        OptExpression input = OptExpression.create(scan);

        Assertions.assertTrue(rule.check(input, null));
        List<OptExpression> outputs = rule.transform(input, null);

        Assertions.assertEquals(1, outputs.size());
        LogicalScanOperator annotated = (LogicalScanOperator) outputs.get(0).getOp();
        Assertions.assertSame(predicate, annotated.getPredicate());
        Assertions.assertEquals(new IndexCondition(predicate), annotated.getIndexCondition());
    }

    @Test
    public void testTopNRulePreservesTopNScoreAndScanSemantics() {
        BinaryPredicateOperator predicate =
                new BinaryPredicateOperator(BinaryType.EQ, idColumn, ConstantOperator.createInt(7));
        ColumnRefOperator scoreColumn = new ColumnRefOperator(3, FloatType.FLOAT, "score", true);
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator scoreExpression = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        Projection projection = new Projection(Map.of(
                idColumn, idColumn,
                vectorColumn, vectorColumn,
                scoreColumn, scoreExpression));
        LogicalPaimonScanOperator scan = createScan(predicate, projection);
        LogicalTopNOperator topN = new LogicalTopNOperator(
                List.of(new Ordering(scoreColumn, true, false)), 10, 2);
        ConnectorIndexMetadata metadata = metadata(List.of(scalarDescriptor(), vectorDescriptor()));
        ApplyTopNIndexRule rule = new ApplyTopNIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> metadata);
        OptExpression input = OptExpression.create(topN, OptExpression.create(scan));

        Assertions.assertTrue(rule.check(input, null));
        List<OptExpression> outputs = rule.transform(input, null);

        Assertions.assertEquals(1, outputs.size());
        Assertions.assertSame(topN, outputs.get(0).getOp());
        LogicalScanOperator annotated = (LogicalScanOperator) outputs.get(0).inputAt(0).getOp();
        Assertions.assertSame(predicate, annotated.getPredicate());
        Assertions.assertSame(projection, annotated.getProjection());
        TopNIndexCondition condition = (TopNIndexCondition) annotated.getIndexCondition();
        Assertions.assertEquals(predicate, condition.getPredicate());
        Assertions.assertSame(scoreExpression, condition.getScoreExpression());
        Assertions.assertEquals(10, condition.getLimit());
        Assertions.assertEquals(2, condition.getOffset());
        Assertions.assertEquals(12, condition.getK());
        Assertions.assertEquals(vectorColumn.getId(), condition.getFieldId());
        Assertions.assertEquals(vectorColumn.getName(), condition.getColumnName());
        Assertions.assertEquals(VectorIndexMetric.L2, condition.getMetric());
        Assertions.assertArrayEquals(new float[] {1, 2}, condition.getQueryVector());
        Assertions.assertTrue(condition.isAscending());
        Assertions.assertFalse(condition.isNullsFirst());
        Assertions.assertFalse(condition.hasResidual());
        Assertions.assertTrue(condition.getUsedColumns().contains(idColumn));
        Assertions.assertTrue(condition.getUsedColumns().contains(vectorColumn));

        TopNIndexCondition equivalent = new TopNIndexCondition(predicate, null, scoreExpression,
                vectorColumn.getId(), vectorColumn.getName(), new float[] {1, 2}, VectorIndexMetric.L2,
                12, 10, 2, true, false);
        Assertions.assertEquals(condition, equivalent);
        Assertions.assertEquals(condition.hashCode(), equivalent.hashCode());
        Assertions.assertNotEquals(condition, new IndexCondition(predicate));
        Assertions.assertTrue(condition.toString().contains("score="));

        float[] mutable = condition.getQueryVector();
        mutable[0] = 99;
        Assertions.assertArrayEquals(new float[] {1, 2}, condition.getQueryVector());
    }

    @Test
    public void testTopNRuleRejectsUnsafeShapesBeforeMetadataLoad() {
        ColumnRefOperator scoreColumn = new ColumnRefOperator(3, FloatType.FLOAT, "score", true);
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator scoreExpression = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        Projection projection = new Projection(Map.of(
                idColumn, idColumn, vectorColumn, vectorColumn, scoreColumn, scoreExpression));
        LogicalPaimonScanOperator scan = createScan(null, projection);

        LogicalTopNOperator base = new LogicalTopNOperator(
                List.of(new Ordering(scoreColumn, true, false)), 10, 0);
        assertRejectedBeforeMetadata(scan, LogicalTopNOperator.builder().withOperator(base)
                .setTopNType(TopNType.RANK).build());
        assertRejectedBeforeMetadata(scan, LogicalTopNOperator.builder().withOperator(base)
                .setPartitionByColumns(List.of(idColumn)).build());
        assertRejectedBeforeMetadata(scan, LogicalTopNOperator.builder().withOperator(base)
                .setSortPhase(SortPhase.PARTIAL).build());
        assertRejectedBeforeMetadata(scan, LogicalTopNOperator.builder().withOperator(base)
                .setIsSplit(true).build());
        assertRejectedBeforeMetadata(scan, new LogicalTopNOperator(
                List.of(new Ordering(scoreColumn, true, false)), Integer.MAX_VALUE, 1));
    }

    @Test
    public void testPredicateRulePrecheckAvoidsMetadataLoad() {
        AtomicInteger loads = new AtomicInteger();
        LogicalPaimonScanOperator scan = createScan(ConstantOperator.createBoolean(true), null);
        ApplyPredicateIndexRule rule = new ApplyPredicateIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> {
                    loads.incrementAndGet();
                    return metadata(List.of(scalarDescriptor()));
                });
        OptExpression input = OptExpression.create(scan);

        Assertions.assertFalse(rule.check(input, null));
        Assertions.assertTrue(rule.transform(input, null).isEmpty());
        Assertions.assertEquals(0, loads.get());
    }

    @Test
    public void testTopNRuleRecordsNullOrderingAndResidualPredicate() {
        BinaryPredicateOperator indexed =
                new BinaryPredicateOperator(BinaryType.EQ, idColumn, ConstantOperator.createInt(7));
        BinaryPredicateOperator residual =
                new BinaryPredicateOperator(BinaryType.GT, idColumn, ConstantOperator.createInt(1));
        ScalarOperator predicate = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, indexed, residual);
        ColumnRefOperator scoreColumn = new ColumnRefOperator(3, FloatType.FLOAT, "score", true);
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator scoreExpression = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        Projection projection = new Projection(Map.of(
                idColumn, idColumn, vectorColumn, vectorColumn, scoreColumn, scoreExpression));
        LogicalPaimonScanOperator scan = createScan(predicate, projection);
        LogicalTopNOperator topN = new LogicalTopNOperator(
                List.of(new Ordering(scoreColumn, true, true)), 10, 0);
        ApplyTopNIndexRule rule = new ApplyTopNIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN,
                ignored -> metadata(List.of(scalarDescriptor(), vectorDescriptor())));

        List<OptExpression> outputs = rule.transform(
                OptExpression.create(topN, OptExpression.create(scan)), null);

        Assertions.assertEquals(1, outputs.size());
        LogicalScanOperator annotated = (LogicalScanOperator) outputs.get(0).inputAt(0).getOp();
        TopNIndexCondition condition = (TopNIndexCondition) annotated.getIndexCondition();
        Assertions.assertSame(predicate, annotated.getPredicate());
        Assertions.assertEquals(indexed, condition.getPredicate());
        Assertions.assertEquals(residual, condition.getResidualPredicate());
        Assertions.assertTrue(condition.hasResidual());
        Assertions.assertTrue(condition.isNullsFirst());
    }

    @Test
    public void testTopNConditionValidatesNormalizedContract() {
        ScalarOperator score = ConstantOperator.createFloat(1);
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TopNIndexCondition(
                null, null, score, -1, "embedding", new float[] {1}, VectorIndexMetric.L2,
                1, 1, 0, true, false));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TopNIndexCondition(
                null, null, score, 1, "embedding", new float[0], VectorIndexMetric.L2,
                1, 1, 0, true, false));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TopNIndexCondition(
                null, null, score, 1, "embedding", new float[] {Float.NaN}, VectorIndexMetric.L2,
                1, 1, 0, true, false));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TopNIndexCondition(
                null, null, score, 1, "embedding", new float[] {1}, VectorIndexMetric.COSINE,
                1, 1, 0, true, false));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TopNIndexCondition(
                null, null, score, 1, "embedding", new float[] {1}, VectorIndexMetric.L2,
                1, 1, 1, true, false));
    }

    private void assertRejectedBeforeMetadata(LogicalPaimonScanOperator scan, LogicalTopNOperator topN) {
        AtomicInteger loads = new AtomicInteger();
        ApplyTopNIndexRule rule = new ApplyTopNIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> {
                    loads.incrementAndGet();
                    return metadata(List.of(vectorDescriptor()));
                });
        OptExpression input = OptExpression.create(topN, OptExpression.create(scan));
        Assertions.assertFalse(rule.check(input, null));
        Assertions.assertTrue(rule.transform(input, null).isEmpty());
        Assertions.assertEquals(0, loads.get());
    }

    private ConnectorIndexMetadata metadata(List<ConnectorIndexDescriptor> descriptors) {
        return ConnectorIndexMetadata.of(42, ConnectorIndexTableType.DATA_EVOLUTION, descriptors);
    }

    private ConnectorIndexDescriptor scalarDescriptor() {
        return new ConnectorIndexDescriptor(ConnectorIndexType.BITMAP, "test", idColumn.getId(),
                idColumn.getName(), Map.of(), Set.of(ConnectorIndexOperation.EQUAL), ConnectorIndexCoverage.FULL);
    }

    private ConnectorIndexDescriptor vectorDescriptor() {
        return new ConnectorIndexDescriptor(ConnectorIndexType.VECTOR, "test", vectorColumn.getId(),
                vectorColumn.getName(), false, false, Map.of(
                ConnectorIndexDescriptor.OPTION_METRIC, VectorIndexMetric.L2.name(),
                ConnectorIndexDescriptor.OPTION_DIMENSION, "2"),
                Set.of(ConnectorIndexOperation.VECTOR_TOP_N), ConnectorIndexCoverage.FULL);
    }

    private LogicalPaimonScanOperator createScan(ScalarOperator predicate, Projection projection) {
        Column id = new Column(idColumn.getName(), IntegerType.INT);
        Column vector = new Column(vectorColumn.getName(), ArrayType.ARRAY_FLOAT);
        LogicalPaimonScanOperator scan = new LogicalPaimonScanOperator(new PaimonTable(),
                Map.of(idColumn, id, vectorColumn, vector),
                Map.of(id, idColumn, vector, vectorColumn), -1, predicate);
        if (projection == null) {
            return scan;
        }
        return new LogicalPaimonScanOperator.Builder()
                .withOperator(scan)
                .setProjection(projection)
                .build();
    }
}
