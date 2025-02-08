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

package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.rule.Rule;

import java.util.List;

/**
 * Rewrite the whole tree by DownTop way.
 */

public class RewriteDownTopTask extends RewriteTreeTask {

    public RewriteDownTopTask(TaskContext context, OptExpression root,
                              List<Rule> rules) {
        super(context, root, rules, true);
    }

    @Override
    protected void rewrite(OptExpression parent, int childIndex, OptExpression root) {
        // prune cte column depend on prune right child first
        for (int i = root.getInputs().size() - 1; i >= 0; i--) {
            rewrite(root, i, root.getInputs().get(i));
        }
        applyRules(parent, childIndex, root, rules);
    }
}
