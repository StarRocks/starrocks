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

import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptimizerConfig;
import com.starrocks.sql.optimizer.rule.Rule;

import java.util.List;

/**
 * Base class for Cascades search task.
 */
public abstract class OptimizerTask {
    protected TaskContext context;

    OptimizerTask(TaskContext context) {
        this.context = context;
    }

    public abstract void execute();

    public void pushTask(OptimizerTask task) {
        context.getOptimizerContext().getTaskScheduler().pushTask(task);
    }

    /**
     * Filter invalid rules by group expression and the rule pattern
     *
     * @param groupExpression the groupExpression will be applied rule
     * @param candidateRules  all candidate rules need to be checked
     * @param validRules      the result valid rules
     */
    void filterInValidRules(GroupExpression groupExpression,
                            List<Rule> candidateRules,
                            List<Rule> validRules) {
        OptimizerConfig optimizerConfig = context.getOptimizerContext().getOptimizerConfig();
        for (Rule rule : candidateRules) {
            if (groupExpression.hasRuleExplored(rule)) {
                continue;
            }

            if (!rule.getPattern().matchWithoutChild(groupExpression)) {
                continue;
            }
            if (optimizerConfig.isRuleDisable(rule.type())) {
                continue;
            }
            if (rule.exhausted(context.getOptimizerContext())) {
                continue;
            }
            validRules.add(rule);
        }
    }
}
