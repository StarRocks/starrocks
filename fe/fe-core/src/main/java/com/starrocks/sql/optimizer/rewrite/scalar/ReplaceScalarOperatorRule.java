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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.Map;

public class ReplaceScalarOperatorRule extends BottomUpScalarOperatorRewriteRule {
    private Map<ScalarOperator, ColumnRefOperator> translateMap;

    public ReplaceScalarOperatorRule(Map<ScalarOperator, ColumnRefOperator> translateMap) {
        this.translateMap = translateMap;
    }

    @Override
    public ScalarOperator visit(ScalarOperator scalarOperator, ScalarOperatorRewriteContext context) {
        for (Map.Entry<ScalarOperator, ColumnRefOperator> m : translateMap.entrySet()) {
            if (ScalarOperator.isEquivalent(m.getKey(), scalarOperator)) {
                return m.getValue();
            }
        }
        return scalarOperator;
    }
}
