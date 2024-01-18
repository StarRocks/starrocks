// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.BitSet;

public class OptimizerConfig {
    public enum OptimizerAlgorithm {
        RULE_BASED,
        COST_BASED
    }

    private OptimizerAlgorithm optimizerAlgorithm;

    private BitSet ruleSetSwitches;
    private BitSet ruleSwitches;

    private boolean isMVRewritePlan;

    private static final OptimizerConfig DEFAULT_CONFIG = new OptimizerConfig();

    public static OptimizerConfig defaultConfig() {
        return DEFAULT_CONFIG;
    }

    public OptimizerConfig() {
        this(OptimizerAlgorithm.COST_BASED);
    }

    public OptimizerConfig(OptimizerAlgorithm optimizerAlgorithm) {
        this.optimizerAlgorithm = optimizerAlgorithm;
        this.ruleSetSwitches = new BitSet(RuleSetType.NUM_RULE_SET.ordinal());
        this.ruleSetSwitches.flip(0, ruleSetSwitches.size());
        this.ruleSwitches = new BitSet(RuleType.NUM_RULES.ordinal());
        this.ruleSwitches.flip(0, ruleSwitches.size());
    }

    public boolean isRuleBased() {
        return optimizerAlgorithm.equals(OptimizerAlgorithm.RULE_BASED);
    }

    public void disableRuleSet(RuleSetType ruleSetType) {
        ruleSetSwitches.clear(ruleSetType.ordinal());
    }

    public boolean isRuleSetTypeDisable(RuleSetType ruleSetType) {
        return !ruleSetSwitches.get(ruleSetType.ordinal());
    }

    public void disableRule(RuleType ruleType) {
        ruleSwitches.clear(ruleType.ordinal());
    }

    public boolean isRuleDisable(RuleType ruleType) {
        return !ruleSwitches.get(ruleType.ordinal());
    }

    public boolean isMVRewritePlan() {
        return this.isMVRewritePlan;
    }

    public void setMVRewritePlan(boolean isMVRewritePlan) {
        this.isMVRewritePlan = isMVRewritePlan;
    }
}
