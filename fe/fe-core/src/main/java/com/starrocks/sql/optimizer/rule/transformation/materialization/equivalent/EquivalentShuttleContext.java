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

import com.starrocks.sql.optimizer.rule.transformation.materialization.RewriteContext;

public class EquivalentShuttleContext {
    private final RewriteContext rewriteContext;
    private final boolean isRollup;
    private boolean isUseEquivalent;
    private boolean isRewrittenByEquivalent;
    private IRewriteEquivalent.RewriteEquivalentType rewriteEquivalentType;

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

    public RewriteContext getRewriteContext() {
        return rewriteContext;
    }

    public IRewriteEquivalent.RewriteEquivalentType getRewriteEquivalentType() {
        return rewriteEquivalentType;
    }
}