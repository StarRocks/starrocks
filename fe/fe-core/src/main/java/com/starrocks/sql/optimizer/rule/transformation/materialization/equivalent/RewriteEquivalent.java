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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class RewriteEquivalent {
    public static final List<IRewriteEquivalent> EQUIVALENTS = Lists.newArrayList(
            TimeSliceRewriteEquivalent.INSTANCE,
            DateTruncEquivalent.INSTANCE,
            CountRewriteEquivalent.INSTANCE,
            BitmapRewriteEquivalent.INSTANCE);
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
        if (columnMapping == null) {
            return this.iRewriteEquivalent.rewrite(this.rewriteEquivalentContext,
                    shuttleContext, rewriteEquivalentContext.getReplace(), newInput);
        } else {
            ColumnRefOperator oldReplace = rewriteEquivalentContext.getReplace();
            if (!columnMapping.containsKey(oldReplace)) {
                return null;
            }
            ColumnRefOperator target = columnMapping.get(oldReplace);
            return this.iRewriteEquivalent.rewrite(this.rewriteEquivalentContext,
                    shuttleContext, target, newInput);
        }
    }
}
