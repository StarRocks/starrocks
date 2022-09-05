// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

// OptimizerTraceInfo is used to record some important info during query optimization
public class OptimizerTraceInfo {
    private final UUID queryId;
    private final Map<String, Integer> rulesAppliedTimes = new HashMap<>();

    public OptimizerTraceInfo(UUID queryId) {
        this.queryId = queryId;
    }

    public void recordAppliedRule(String rule) {
        rulesAppliedTimes.merge(rule, 1, Integer::sum);
    }

    public Map<String, Integer> getRulesAppliedTimes() {
        return rulesAppliedTimes;
    }

    public UUID getQueryId() {
        return queryId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OptimizerTraceInfo");
        sb.append("\nRules' applied times\n").append(rulesAppliedTimes);
        return sb.toString();
    }
}
