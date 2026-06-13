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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class MaterializedViewRewriterTest {
    @Test
    public void testCheckJoinOnPredicateFromPredicatesUsesEquivalentColumns(@Mocked MvRewriteContext mvRewriteContext,
                                                                            @Mocked MaterializationContext mvContext,
                                                                            @Mocked OptimizerContext optimizerContext) {
        QueryMaterializationContext queryMaterializationContext = new QueryMaterializationContext();
        new Expectations() {
            {
                mvRewriteContext.getMaterializationContext();
                result = mvContext;

                mvContext.getOptimizerContext();
                result = optimizerContext;

                optimizerContext.getQueryMaterializationContext();
                result = queryMaterializationContext;
            }
        };

        MaterializedViewRewriter rewriter = new MaterializedViewRewriter(mvRewriteContext);

        ColumnRefFactory predicateRefFactory = new ColumnRefFactory();
        ColumnRefOperator leftFromPredicate = predicateRefFactory.create("left_col", IntegerType.INT, true);
        ColumnRefOperator rightFromPredicate = predicateRefFactory.create("right_col", IntegerType.INT, true);
        List<ScalarOperator> predicates = List.of(
                BinaryPredicateOperator.eq(leftFromPredicate, ConstantOperator.createInt(1)),
                BinaryPredicateOperator.eq(rightFromPredicate, ConstantOperator.createInt(1)));

        ColumnRefFactory diffRefFactory = new ColumnRefFactory();
        diffRefFactory.create("padding_col", IntegerType.INT, true);
        ColumnRefOperator leftFromDiff = diffRefFactory.create("left_col", IntegerType.INT, true);
        ColumnRefOperator rightFromDiff = diffRefFactory.create("right_col", IntegerType.INT, true);
        Set<ScalarOperator> diffPredicates = Sets.newHashSet(BinaryPredicateOperator.eq(leftFromDiff, rightFromDiff));

        Assertions.assertTrue(rewriter.checkJoinOnPredicateFromPredicates(predicates, diffPredicates));
    }
}
