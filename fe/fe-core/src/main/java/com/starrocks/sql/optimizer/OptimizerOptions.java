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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

public class OptimizerOptions {
    private static final Logger LOG = LogManager.getLogger(OptimizerOptions.class);
    
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

    public void applyDisableRuleFromSessionVariable(SessionVariable sessionVariable) {
        if (sessionVariable == null) {
            return;
        }

        String disabledRulesStr = sessionVariable.getCboDisabledRules();
        if (Strings.isNullOrEmpty(disabledRulesStr)) {
            return;
        }

        Set<RuleType> disabledRules = parseDisabledRules(disabledRulesStr);
        for (RuleType ruleType : disabledRules) {
            ruleSwitches.clear(ruleType.ordinal());
        }
    }

    private static Set<RuleType> parseDisabledRules(String rulesStr) {
        Set<RuleType> result = Sets.newHashSet();

        if (Strings.isNullOrEmpty(rulesStr)) {
            return result;
        }

        try {
            List<String> ruleNames = Splitter.on(',')
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(rulesStr);

            for (String ruleName : ruleNames) {
                try {
                    RuleType ruleType = RuleType.valueOf(ruleName);
                    if (ruleType.name().startsWith("TF_") || ruleType.name().startsWith("GP_")) {
                        result.add(ruleType);
                    }
                } catch (IllegalArgumentException e) {
                    LOG.warn("Ignoring unknown rule name: {} (may be from different version)", ruleName);
                }
            }
        } catch (Exception e) {
            LOG.error("Unexpected error parsing disabled rules: '{}', returning empty set", rulesStr, e);
        }

        return result;
    }
}
