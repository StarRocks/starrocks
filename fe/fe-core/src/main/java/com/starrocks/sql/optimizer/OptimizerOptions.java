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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.BitSet;

public class OptimizerOptions {
    public enum OptimizerStrategy {
        RULE_BASED,
        COST_BASED,
        SHORT_CIRCUIT,
        BASELINE_PLAN,
    }

    private final OptimizerStrategy optimizerStrategy;

    private final BitSet ruleSwitches;

    public OptimizerOptions() {
        this(OptimizerStrategy.COST_BASED);
    }
    
    public OptimizerOptions(OptimizerStrategy optimizerStrategy) {
        this.optimizerStrategy = optimizerStrategy;
        this.ruleSwitches = new BitSet(RuleType.NUM_RULES.ordinal());
        this.ruleSwitches.flip(0, ruleSwitches.size());
    }

    public boolean isRuleBased() {
        return optimizerStrategy.equals(OptimizerStrategy.RULE_BASED);
    }

    public boolean isShortCircuit() {
        return optimizerStrategy.equals(OptimizerStrategy.SHORT_CIRCUIT);
    }

    public boolean isBaselinePlan() {
        return optimizerStrategy.equals(OptimizerStrategy.BASELINE_PLAN);
    }

    public void disableRule(RuleType ruleType) {
        ruleSwitches.clear(ruleType.ordinal());
    }

    public boolean isRuleDisable(RuleType ruleType) {
        return !ruleSwitches.get(ruleType.ordinal());
    }

    private static final OptimizerOptions DEFAULT_OPTIONS = new OptimizerOptions(OptimizerStrategy.COST_BASED);

    public static OptimizerOptions defaultOpt() {
        return DEFAULT_OPTIONS;
    }

    public static OptimizerOptions newRuleBaseOpt() {
        return new OptimizerOptions(OptimizerStrategy.RULE_BASED);
    }

    public static OptimizerOptions newShortCircuitOpt() {
        return new OptimizerOptions(OptimizerStrategy.SHORT_CIRCUIT);
    }
}
