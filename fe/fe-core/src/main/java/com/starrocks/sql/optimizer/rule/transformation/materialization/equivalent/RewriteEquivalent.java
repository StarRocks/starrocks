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

package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.RewriteContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class RewriteEquivalent {
    public static final List<IAggregateRewriteEquivalent> AGGREGATE_EQUIVALENTS = Lists.newArrayList(
            CountRewriteEquivalent.INSTANCE,
            BitmapRewriteEquivalent.INSTANCE,
            ArrayRewriteEquivalent.INSTANCE,
            HLLRewriteEquivalent.INSTANCE,
            PercentileRewriteEquivalent.INSTANCE,
            AggStateRewriteEquivalent.INSTANCE
    );
    public static final List<IRewriteEquivalent> PREDICATE_EQUIVALENTS = Lists.newArrayList(
            TimeSliceRewriteEquivalent.INSTANCE,
            DateTruncEquivalent.INSTANCE);
    public static final List<IRewriteEquivalent> EQUIVALENTS = Stream.concat(
            AGGREGATE_EQUIVALENTS.stream(),
            PREDICATE_EQUIVALENTS.stream()
    ).collect(ImmutableList.toImmutableList());

    private final IRewriteEquivalent.RewriteEquivalentContext rewriteEquivalentContext;
    private final IRewriteEquivalent iRewriteEquivalent;

    public RewriteEquivalent(IRewriteEquivalent.RewriteEquivalentContext rewriteEquivalentContext,
                             IRewriteEquivalent iRewriteEquivalent,
                             ColumnRefOperator replace) {
        this.rewriteEquivalentContext = rewriteEquivalentContext;
        this.iRewriteEquivalent = iRewriteEquivalent;
        this.rewriteEquivalentContext.setReplace(replace);
    }

    public IRewriteEquivalent.RewriteEquivalentContext getEquivalentContext() {
        return rewriteEquivalentContext;
    }

    public IRewriteEquivalent.RewriteEquivalentType getRewriteEquivalentType() {
        return this.iRewriteEquivalent.getRewriteEquivalentType();
    }

    public ScalarOperator rewrite(EquivalentShuttleContext shuttleContext,
                                  Map<ColumnRefOperator, ColumnRefOperator> columnMapping,
                                  ScalarOperator newInput) {
        ScalarOperator result = null;
        if (columnMapping == null) {
            result = this.iRewriteEquivalent.rewrite(this.rewriteEquivalentContext,
                    shuttleContext, rewriteEquivalentContext.getReplace(), newInput);
        } else {
            ColumnRefOperator oldReplace = rewriteEquivalentContext.getReplace();
            if (!columnMapping.containsKey(oldReplace)) {
                return null;
            }
            ColumnRefOperator target = columnMapping.get(oldReplace);
            result = this.iRewriteEquivalent.rewrite(this.rewriteEquivalentContext,
                    shuttleContext, target, newInput);
        }
        if (result != null) {
            RewriteContext rewriteContext = shuttleContext.getRewriteContext();
            if (rewriteContext != null && rewriteContext.getAggregatePushDownContext() != null &&
                    newInput instanceof CallOperator && rewriteEquivalentContext.getInput() instanceof CallOperator) {
                rewriteContext.getAggregatePushDownContext().registerOrigAggRewriteInfo((CallOperator) newInput,
                        (CallOperator) rewriteEquivalentContext.getInput());
            }
        }
        return result;
    }
}
