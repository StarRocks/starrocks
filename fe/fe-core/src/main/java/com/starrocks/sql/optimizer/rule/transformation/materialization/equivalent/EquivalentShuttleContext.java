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

import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.RewriteContext;

import java.util.Map;

public class EquivalentShuttleContext {
    private final RewriteContext rewriteContext;
    private final boolean isRollup;
    private boolean isUseEquivalent;
    private boolean isRewrittenByEquivalent;
    private IRewriteEquivalent.RewriteEquivalentType rewriteEquivalentType;
    private Map<ColumnRefOperator, CallOperator> newColumnRefToAggFuncMap;

    // Per-rewrite signal raised by PercentileRewriteEquivalent when the MV's
    // stored compression is strictly smaller than the query's compression.
    // BestMvSelector inspects the flag through MvRewriteContext / RewriteResult
    // to prefer subsume MVs; strict mode (session var) turns this into a hard
    // skip (caller emits a logMVRewriteFailReason and returns null).
    private boolean hasPercentileNonSubsumeRewrite;
    private double percentileMismatchMvC;
    private double percentileMismatchQueryC;

    public EquivalentShuttleContext(RewriteContext rewriteContext, boolean isRollup, boolean isRewrittenByEquivalent,
                                    IRewriteEquivalent.RewriteEquivalentType type) {
        this.rewriteContext = rewriteContext;
        this.isRollup = isRollup;
        this.isUseEquivalent = isRewrittenByEquivalent;
        this.rewriteEquivalentType = type;
    }

    public boolean isUseEquivalent() {
        return isUseEquivalent;
    }

    public boolean isRollup() {
        return isRollup;
    }

    public boolean isRewrittenByEquivalent() {
        return isRewrittenByEquivalent;
    }

    public void setRewrittenByEquivalent(boolean rewrittenByEquivalent) {
        isRewrittenByEquivalent = rewrittenByEquivalent;
    }

    public boolean isRewrittenByRewriter() {
        return newColumnRefToAggFuncMap != null;
    }

    public void setNewColumnRefToAggFuncMap(Map<ColumnRefOperator, CallOperator> newColumnRefToAggFuncMap) {
        this.newColumnRefToAggFuncMap = newColumnRefToAggFuncMap;
    }

    public Map<ColumnRefOperator, CallOperator> getNewColumnRefToAggFuncMap() {
        return newColumnRefToAggFuncMap;
    }

    public RewriteContext getRewriteContext() {
        return rewriteContext;
    }

    public IRewriteEquivalent.RewriteEquivalentType getRewriteEquivalentType() {
        return rewriteEquivalentType;
    }

    public boolean hasPercentileNonSubsumeRewrite() {
        return hasPercentileNonSubsumeRewrite;
    }

    public void setPercentileNonSubsumeRewrite(boolean v) {
        this.hasPercentileNonSubsumeRewrite = v;
    }

    public double getPercentileMismatchMvC() {
        return percentileMismatchMvC;
    }

    public void setPercentileMismatchMvC(double v) {
        this.percentileMismatchMvC = v;
    }

    public double getPercentileMismatchQueryC() {
        return percentileMismatchQueryC;
    }

    public void setPercentileMismatchQueryC(double v) {
        this.percentileMismatchQueryC = v;
    }
}