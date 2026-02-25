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

/**
 * Rewrite the whole tree by TopDown way.
 * If a node had been rewritten, we don't need process the children and just finish the task.
 */

public class RewriteAtMostOnceTask extends RewriteTreeTask {

    public RewriteAtMostOnceTask(TaskContext context, OptExpression root,
                                 Rule rule) {
        super(context, root, rule, true);
    }

    @Override
    protected void rewrite(OptExpression parent, int childIndex, OptExpression root) {

        if (change > 0) {
            return;
        }
        super.rewrite(parent, childIndex, root);
    }
}
