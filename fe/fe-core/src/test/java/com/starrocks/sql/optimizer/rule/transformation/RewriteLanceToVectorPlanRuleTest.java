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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.LanceTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class RewriteLanceToVectorPlanRuleTest {
    private boolean originalEnableExperimentalVector;

    @BeforeEach
    public void setUp() {
        originalEnableExperimentalVector = Config.enable_experimental_vector;
        Config.enable_experimental_vector = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_experimental_vector = originalEnableExperimentalVector;
    }

    @Test
    public void testRewriteL2DistanceAscending() {
        OptExpression rewritten = rewrite(FunctionSet.APPROX_L2_DISTANCE, true);

        LogicalLanceScanOperator scan = (LogicalLanceScanOperator) rewritten.getInputs().get(0).getOp();
        Assertions.assertTrue(scan.getVectorSearchOptions().isEnableUseANN());
        Assertions.assertEquals(0, scan.getVectorSearchOptions().toThrift().getResult_order());
        Assertions.assertEquals("emb", scan.getVectorSearchOptions().getQueryParams().get(
                RewriteLanceToVectorPlanRule.LANCE_VECTOR_COLUMN_PARAM));
        Assertions.assertEquals("l2", scan.getVectorSearchOptions().getQueryParams().get(
                RewriteLanceToVectorPlanRule.LANCE_VECTOR_METRIC_PARAM));
        Assertions.assertEquals(List.of("1.0", "2.0", "3.0"), scan.getVectorSearchOptions().getQueryVector());
    }

    @Test
    public void testRewriteCosineDistanceAscending() {
        OptExpression rewritten = rewrite(FunctionSet.APPROX_COSINE_DISTANCE, true);

        LogicalLanceScanOperator scan = (LogicalLanceScanOperator) rewritten.getInputs().get(0).getOp();
        Assertions.assertTrue(scan.getVectorSearchOptions().isEnableUseANN());
        Assertions.assertEquals(0, scan.getVectorSearchOptions().toThrift().getResult_order());
        Assertions.assertEquals("cosine", scan.getVectorSearchOptions().getQueryParams().get(
                RewriteLanceToVectorPlanRule.LANCE_VECTOR_METRIC_PARAM));
    }

    @Test
    public void testRewriteInnerProductDescending() {
        OptExpression rewritten = rewrite(FunctionSet.APPROX_INNER_PRODUCT, false);

        LogicalLanceScanOperator scan = (LogicalLanceScanOperator) rewritten.getInputs().get(0).getOp();
        Assertions.assertTrue(scan.getVectorSearchOptions().isEnableUseANN());
        Assertions.assertEquals(1, scan.getVectorSearchOptions().toThrift().getResult_order());
        Assertions.assertEquals("dot", scan.getVectorSearchOptions().getQueryParams().get(
                RewriteLanceToVectorPlanRule.LANCE_VECTOR_METRIC_PARAM));
    }

    @Test
    public void testRejectWrongOrder() {
        Assertions.assertTrue(tryRewrite(FunctionSet.APPROX_L2_DISTANCE, false).isEmpty());
        Assertions.assertTrue(tryRewrite(FunctionSet.APPROX_COSINE_DISTANCE, false).isEmpty());
        Assertions.assertTrue(tryRewrite(FunctionSet.APPROX_INNER_PRODUCT, true).isEmpty());
    }

    @Test
    public void testRejectCosineSimilarity() {
        Assertions.assertTrue(tryRewrite(FunctionSet.APPROX_COSINE_SIMILARITY, false).isEmpty());
    }

    private OptExpression rewrite(String functionName, boolean ascending) {
        List<OptExpression> rewritten = tryRewrite(functionName, ascending);
        Assertions.assertEquals(1, rewritten.size());
        return rewritten.get(0);
    }

    private List<OptExpression> tryRewrite(String functionName, boolean ascending) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator embRef = columnRefFactory.create("emb", new ArrayType(Type.FLOAT), true);
        ColumnRefOperator scoreRef = columnRefFactory.create(functionName, Type.FLOAT, true);

        Column embColumn = new Column("emb", new ArrayType(Type.FLOAT), true);
        LanceTable table = new LanceTable("lance_catalog", "default", "tbl", List.of(embColumn),
                ImmutableMap.of(LanceTable.DATASET_URI, "file:///tmp/tbl.lance"));
        LogicalLanceScanOperator scan = new LogicalLanceScanOperator(table, Map.of(embRef, embColumn),
                Map.of(embColumn, embRef), -1, null);
        scan.setProjection(new Projection(Map.of(scoreRef, vectorCall(functionName, embRef))));

        LogicalTopNOperator topN = new LogicalTopNOperator(List.of(new Ordering(scoreRef, ascending, false)), 10, 0);
        OptExpression input = OptExpression.create(topN, OptExpression.create(scan));
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);

        RewriteLanceToVectorPlanRule rule = new RewriteLanceToVectorPlanRule();
        Assertions.assertTrue(rule.check(input, context));
        return rule.transform(input, context);
    }

    private ScalarOperator vectorCall(String functionName, ColumnRefOperator embRef) {
        ArrayOperator queryVector = new ArrayOperator(new ArrayType(Type.FLOAT), false,
                List.of(ConstantOperator.createFloat(1.0),
                        ConstantOperator.createFloat(2.0),
                        ConstantOperator.createFloat(3.0)));
        return new CallOperator(functionName, Type.FLOAT, List.of(queryVector, embRef));
    }
}
