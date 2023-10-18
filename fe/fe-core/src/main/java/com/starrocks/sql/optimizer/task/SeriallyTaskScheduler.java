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

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.Memo;

import java.util.Stack;

public class SeriallyTaskScheduler implements TaskScheduler {
    private final Stack<OptimizerTask> tasks;

    private SeriallyTaskScheduler() {
        tasks = new Stack<>();
    }

    public static TaskScheduler create() {
        return new SeriallyTaskScheduler();
    }

    @Override
    public void executeTasks(TaskContext context) {
        long timeout = context.getOptimizerContext().getSessionVariable().getOptimizerExecuteTimeout();
        long watch = context.getOptimizerContext().getCostTimeMs();
        while (!tasks.empty()) {
            if (timeout > 0 && watch > timeout) {
                // Should have at least one valid plan
                // group will be null when in rewrite phase
                // memo may be null for rule-based optimizer
                Memo memo = context.getOptimizerContext().getMemo();
                Group group = memo == null ? null : memo.getRootGroup();
                if (group == null || !group.hasBestExpression(context.getRequiredProperty())) {
                    throw new StarRocksPlannerException("StarRocks planner use long time " + timeout +
                            " ms in " + (group == null ? "logical" : "memo") + " phase, This probably because " +
                            "1. FE Full GC, " +
                            "2. Hive external table fetch metadata took a long time, " +
                            "3. The SQL is very complex. " +
                            "You could " +
                            "1. adjust FE JVM config, " +
                            "2. try query again, " +
                            "3. enlarge new_planner_optimize_timeout session variable",
                            ErrorType.INTERNAL_ERROR);
                }
                break;
            }
            OptimizerTask task = tasks.pop();
            context.getOptimizerContext().setTaskContext(context);
            try (Timer ignore = Tracers.watchScope(Tracers.Module.OPTIMIZER, task.getClass().getSimpleName())) {
                task.execute();
            }
        }
    }

    @Override
    public void pushTask(OptimizerTask task) {
        tasks.push(task);
    }
}
