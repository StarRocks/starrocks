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

package com.starrocks.sql.optimizer.validate;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public final class PlanValidator {

    private static final Logger LOGGER = LogManager.getLogger(PlanValidator.class);

    private List<Checker> checkerList;
    private boolean enableInputDependenciesChecker = false;

    public PlanValidator() {
        checkerList = ImmutableList.of(CTEUniqueChecker.getInstance());
    }

    /**
     * Enable inputDependencies checker for rule transformation phase.
     * This method should be called after column pruning to ensure InputDependenciesChecker
     * can properly validate column dependencies in the optimized plan.
     */
    public void enableInputDependenciesChecker() {
        if (!enableInputDependenciesChecker) {
            checkerList = ImmutableList.of(
                    CTEUniqueChecker.getInstance(),
                    InputDependenciesChecker.getInstance());
            enableInputDependenciesChecker = true;
        }
    }

    public void enableAllCheckers() {
        checkerList = ImmutableList.of(
                TypeChecker.getInstance(),
                CTEUniqueChecker.getInstance(),
                InputDependenciesChecker.getInstance(),
                ColumnReuseChecker.getInstance());
    }

    public void validatePlan(OptExpression optExpression, TaskContext taskContext) {
        boolean enablePlanValidation = ConnectContext.get().getSessionVariable().getEnablePlanValidation();
        try {
            for (Checker checker : checkerList) {
                try (Timer tracer = Tracers.watchScope(checker.getClass().getSimpleName())) {
                    checker.validate(optExpression, taskContext);
                }
            }
        } catch (IllegalArgumentException e) {
            String message = e.getMessage();
            if (!message.contains("Invalid plan")) {
                message = "Invalid plan:\n" + optExpression.debugString() + "\n" + message;
            }
            LOGGER.debug("Failed to validate plan.", e);
            if (enablePlanValidation) {
                throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
            }
        } catch (StarRocksPlannerException e) {
            LOGGER.debug("Failed to validate plan.", e);
            if (enablePlanValidation) {
                throw e;
            }
        } catch (Exception e) {
            LOGGER.debug("Failed to validate plan.", e);
            if (enablePlanValidation) {
                throw new StarRocksPlannerException("encounter exception when validate plan.", ErrorType.INTERNAL_ERROR);
            }
        }
    }

    /**
     * Validates the query plan after applying an optimizer rule for debugging purposes.
     * Provides detailed error information including rule name and before/after expressions.
     * 
     * @param beforeExpression The query plan expression before rule transformation
     * @param afterExpression The query plan expression after rule transformation
     * @param rule The optimizer rule that was applied
     * @param taskContext The task context containing session variables
     * @throws IllegalStateException if validation fails, with detailed rule information
     */
    public static void validateAfterRule(OptExpression beforeExpression, OptExpression afterExpression, 
                                       Rule rule, TaskContext taskContext) {
        if (!taskContext.getOptimizerContext().getSessionVariable().enableOptimizerRuleDebug()) {
            return;
        }
        
        try {
            OptExpression clonedOpt = OptExpression.create(afterExpression.getOp(), afterExpression.getInputs());
            clonedOpt.clearStatsAndInitOutputInfo();
            taskContext.getPlanValidator().validatePlan(clonedOpt, taskContext);
        } catch (Exception e) {
            String errorMsg = String.format("Optimizer rule debug: Plan validation failed after applying rule [%s].\n" +
                            "Validation error: %s\n" +
                            "Hint: This error was caught by enable_optimizer_rule_debug=true\n" +
                            "\n=== Query Plan Before Rule Application ===\n%s\n" +
                            "\n=== Query Plan After Rule Application ===\n%s\n",
                    rule.type(),
                    e.getMessage(),
                    beforeExpression.debugString(),
                    afterExpression.debugString()
            );
            LOGGER.error(errorMsg, e);
            
            throw new IllegalStateException(errorMsg, e);
        }
    }


    public interface Checker {
        void validate(OptExpression physicalPlan, TaskContext taskContext);
    }
}
