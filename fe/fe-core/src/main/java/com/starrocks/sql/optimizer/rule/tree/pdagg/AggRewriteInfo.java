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

package com.starrocks.sql.optimizer.rule.tree.pdagg;

import com.starrocks.sql.optimizer.OptExpression;

import java.util.Optional;

public class AggRewriteInfo {
    private boolean rewritten = false;
    private AggColumnRefRemapping remapping;
    private OptExpression op;

    public AggregatePushDownContext getCtx() {
        return ctx;
    }

    public void setCtx(AggregatePushDownContext ctx) {
        this.ctx = ctx;
    }

    private AggregatePushDownContext ctx;

    public static final AggRewriteInfo NOT_REWRITE = new AggRewriteInfo(false, null, null, null);

    public AggRewriteInfo(boolean rewritten, AggColumnRefRemapping remapping,
                          OptExpression op, AggregatePushDownContext ctx) {
        this.rewritten = rewritten;
        this.remapping = remapping;
        this.op = op;
        this.ctx = ctx;
    }

    public boolean hasRewritten() {
        return rewritten;
    }

    public void setRewritten(boolean rewritten) {
        this.rewritten = rewritten;
    }

    private Optional<AggColumnRefRemapping> getRemapping(boolean isStrict) {
        if (!rewritten) {
            return Optional.empty();
        }
        if (isStrict && remapping.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(remapping);
    }

    public Optional<AggColumnRefRemapping> getRemapping() {
        return getRemapping(true);
    }

    /**
     * Get remapping without checking if it is empty which may happen when query contains no aggregate functions
     */
    public Optional<AggColumnRefRemapping> getRemappingUnChecked() {
        return getRemapping(false);
    }

    public void setRemapping(AggColumnRefRemapping remapping) {
        this.remapping = remapping;
    }

    public Optional<OptExpression> getOp() {
        return rewritten ? Optional.of(op) : Optional.empty();
    }

    public void setOp(OptExpression op) {
        this.op = op;
    }

    /**
     * Output rewrite info's remapping and context to the given remapping and context
     */
    public void output(AggColumnRefRemapping remapping, AggregatePushDownContext aggregatePushDownContext) {
        getRemapping().ifPresent(remapping::combine);
        aggregatePushDownContext.combine(ctx);
    }
}
