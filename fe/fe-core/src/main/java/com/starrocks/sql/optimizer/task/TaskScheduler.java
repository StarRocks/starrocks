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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.rule.Rule;

import java.util.Collections;
import java.util.List;
import java.util.Stack;

public class TaskScheduler {
    public final Stack<OptimizerTask> tasks;

    public TaskScheduler() {
        tasks = new Stack<>();
    }

    public static TaskScheduler create() {
        return new TaskScheduler();
    }

    public void executeTasks(TaskContext context) {
        while (!tasks.empty()) {
            context.getOptimizerContext().checkTimeout();
            OptimizerTask task = tasks.pop();
            context.getOptimizerContext().setTaskContext(context);
            try (Timer ignore = Tracers.watchScope(Tracers.Module.OPTIMIZER, task.getClass().getSimpleName())) {
                task.execute();
            }
        }
    }

    public void pushTask(OptimizerTask task) {
        tasks.push(task);
    }

    public void rewriteIterative(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        pushTask(new RewriteTreeTask(rootTaskContext, tree, rule, false));
        executeTasks(rootTaskContext);
    }

    public void rewriteOnce(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        RewriteTreeTask rewriteTreeTask = new RewriteTreeTask(rootTaskContext, tree, rule, true);
        pushTask(rewriteTreeTask);
        executeTasks(rootTaskContext);
    }

    public void rewriteAtMostOnce(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        pushTask(new RewriteAtMostOnceTask(rootTaskContext, tree, rule));
        executeTasks(rootTaskContext);
    }

    public void rewriteDownTop(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        List<Rule> rules = Collections.singletonList(rule);
        pushTask(new RewriteDownTopTask(rootTaskContext, tree, rules));
        executeTasks(rootTaskContext);
    }
}
