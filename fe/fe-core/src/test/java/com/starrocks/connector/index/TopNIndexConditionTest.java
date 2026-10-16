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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TopNIndexConditionTest {
    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
    private final Map<Column, ColumnRefOperator> refs = new LinkedHashMap<>();

    @Test
    public void testDuplicatedScanRewritesEveryTopNExpression() {
        ColumnRefOperator id = column("id", IntegerType.INT);
        ColumnRefOperator partition = column("pt", IntegerType.INT);
        ColumnRefOperator residual = column("residual", IntegerType.INT);
        ColumnRefOperator vector = column("embedding", ArrayType.ARRAY_FLOAT);
        ScalarOperator indexPredicate = equality(id, 7);
        ScalarOperator partitionPredicate = equality(partition, 2024);
        ScalarOperator residualPredicate = equality(residual, 9);
        ScalarOperator score = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(
                vector, new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                        List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)))));
        TopNIndexCondition condition = new TopNIndexCondition(indexPredicate, partitionPredicate,
                residualPredicate, score, 17, "embedding", new float[] {1, 2}, VectorIndexMetric.L2,
                15, 10, 5, true, false);
        LogicalPaimonScanOperator original = scan(
                Utils.compoundAnd(List.of(indexPredicate, partitionPredicate, residualPredicate)), condition);
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(factory, null);
        LogicalPaimonScanOperator copied =
                duplicator.duplicate(OptExpression.create(original)).getOp().cast();
        TopNIndexCondition rewritten = (TopNIndexCondition) copied.getIndexCondition();
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(duplicator.getColumnMapping());

        Assertions.assertNotSame(condition, rewritten);
        Assertions.assertEquals(rewriter.rewrite(indexPredicate), rewritten.getPredicate());
        Assertions.assertEquals(rewriter.rewrite(partitionPredicate), rewritten.getPartitionPredicate());
        Assertions.assertEquals(rewriter.rewrite(residualPredicate), rewritten.getResidualPredicate());
        Assertions.assertEquals(rewriter.rewrite(score), rewritten.getScoreExpression());
        Assertions.assertEquals(17, rewritten.getFieldId());
        Assertions.assertEquals("embedding", rewritten.getColumnName());
        Assertions.assertArrayEquals(new float[] {1, 2}, rewritten.getQueryVector());
        Assertions.assertEquals(VectorIndexMetric.L2, rewritten.getMetric());
        Assertions.assertEquals(15, rewritten.getK());
        Assertions.assertEquals(10, rewritten.getLimit());
        Assertions.assertEquals(5, rewritten.getOffset());
        Assertions.assertTrue(rewritten.isAscending());
        Assertions.assertFalse(rewritten.isNullsFirst());
        Assertions.assertTrue(rewritten.hasResidual());
        assertOwnedColumns(copied);
        Assertions.assertEquals(new ColumnRefSet(columns.keySet()), condition.getUsedColumns());
        Assertions.assertSame(indexPredicate, condition.getPredicate());
        Assertions.assertSame(partitionPredicate, condition.getPartitionPredicate());
        Assertions.assertSame(residualPredicate, condition.getResidualPredicate());
        Assertions.assertSame(score, condition.getScoreExpression());

        // Unfiltered TopN requests must keep their absent predicates absent.
        TopNIndexCondition unfiltered = new TopNIndexCondition(null, null, score, 17, "embedding",
                new float[] {1, 2}, VectorIndexMetric.L2, 10, 10, 0, true, false).rewriteColumnRefs(rewriter);
        Assertions.assertNull(unfiltered.getPredicate());
        Assertions.assertNull(unfiltered.getPartitionPredicate());
        Assertions.assertFalse(unfiltered.hasResidual());
        Assertions.assertEquals(rewriter.rewrite(score), unfiltered.getScoreExpression());
    }

    @Test
    public void testTopNConditionCopiesAndParticipatesInScanIdentity() {
        ColumnRefOperator vector = column("embedding", ArrayType.ARRAY_FLOAT);
        ScalarOperator score = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(
                vector, new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                        List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)))));
        TopNIndexCondition condition = new TopNIndexCondition(null, null, score, 17, "embedding",
                new float[] {1, 2}, VectorIndexMetric.L2, 10, 10, 0, true, false);
        LogicalPaimonScanOperator original = scan(null, condition);
        assertPaimonCopies(original);

        LogicalPaimonScanOperator different = new LogicalPaimonScanOperator.Builder().withOperator(original)
                .setIndexCondition(new TopNIndexCondition(null, null, score, 17, "embedding",
                        new float[] {1, 2}, VectorIndexMetric.L2, 20, 20, 0, true, false)).build();
        Assertions.assertNotEquals(original, different);
        Assertions.assertNotEquals(new PhysicalPaimonScanOperator(original), new PhysicalPaimonScanOperator(different));
    }

    private void assertPaimonCopies(LogicalPaimonScanOperator original) {
        LogicalPaimonScanOperator.Builder builder = OperatorBuilderFactory.build(original);
        LogicalPaimonScanOperator copy = builder.withOperator(original).build();
        Assertions.assertSame(original.getIndexCondition(), copy.getIndexCondition());
        Assertions.assertEquals(original, copy);
        Assertions.assertEquals(original.hashCode(), copy.hashCode());
        PhysicalPaimonScanOperator physical = new PhysicalPaimonScanOperator(original);
        PhysicalPaimonScanOperator physicalCopy = new PhysicalPaimonScanOperator(copy);
        Assertions.assertSame(original.getIndexCondition(), physical.getIndexCondition());
        Assertions.assertEquals(physical, physicalCopy);
        Assertions.assertEquals(physical.hashCode(), physicalCopy.hashCode());
        boolean indexed = original.getIndexCondition() != null;
        Assertions.assertEquals(indexed, LogicalPlanPrinter.print(OptExpression.create(copy)).contains("index["));
        Assertions.assertEquals(indexed, LogicalPlanPrinter.print(OptExpression.create(copy), false, true).contains("index["));
        Assertions.assertEquals(indexed, LogicalPlanPrinter.print(OptExpression.create(physical)).contains("index["));
    }

    private ColumnRefOperator column(String name, Type type) {
        ColumnRefOperator ref = factory.create(name, type, false);
        factory.updateColumnToRelationIds(ref.getId(), 0);
        Column column = new Column(name, type);
        columns.put(ref, column);
        refs.put(column, ref);
        return ref;
    }

    private ScalarOperator equality(ColumnRefOperator ref, int value) {
        return new BinaryPredicateOperator(BinaryType.EQ, ref, ConstantOperator.createInt(value));
    }

    private LogicalPaimonScanOperator scan(ScalarOperator predicate, IndexCondition condition) {
        return new LogicalPaimonScanOperator.Builder()
                .withOperator(new LogicalPaimonScanOperator(new PaimonTable(), columns, refs, -1, predicate))
                .setIndexCondition(condition).build();
    }

    private void assertOwnedColumns(LogicalPaimonScanOperator scan) {
        Assertions.assertTrue(new ColumnRefSet(scan.getColRefToColumnMetaMap().keySet())
                .containsAll(scan.getIndexCondition().getUsedColumns()));
    }
}
