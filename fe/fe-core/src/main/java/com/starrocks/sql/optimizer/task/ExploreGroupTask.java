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

import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;

/*
 * Explore group logical transform
 */
public class ExploreGroupTask extends OptimizerTask {
    private final Group group;

    ExploreGroupTask(TaskContext context, Group group) {
        super(context);
        this.group = group;
    }

    @Override
    public void execute() {
        if (group.isExplored()) {
            return;
        }

        for (GroupExpression logical : group.getLogicalExpressions()) {
            pushTask(new OptimizeExpressionTask(context, logical, true));
        }

        group.setExplored();
    }

    @Override
    public String toString() {
        return "ExploreGroupTask for group " + group.getId();
    }
}
