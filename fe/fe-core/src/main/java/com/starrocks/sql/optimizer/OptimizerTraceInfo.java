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

import com.google.common.base.Stopwatch;
import com.starrocks.sql.ast.StatementBase;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

// OptimizerTraceInfo is used to record some important info during query optimization
public class OptimizerTraceInfo {
    private final UUID queryId;
    private final Map<String, Integer> rulesAppliedTimes = new HashMap<>();
    private final Stopwatch stopwatch;

    private boolean traceOptimizer;

    public OptimizerTraceInfo(UUID queryId, StatementBase stmt) {
        this.queryId = queryId;
        this.stopwatch = Stopwatch.createStarted();
        if (stmt != null && stmt.getExplainLevel() == StatementBase.ExplainLevel.OPTIMIZER) {
            traceOptimizer = true;
        }

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

    public Stopwatch getStopwatch() {
        return stopwatch;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OptimizerTraceInfo");
        sb.append("\nRules' applied times\n").append(rulesAppliedTimes);
        return sb.toString();
    }

    public boolean isTraceOptimizer() {
        return traceOptimizer;
    }
}
