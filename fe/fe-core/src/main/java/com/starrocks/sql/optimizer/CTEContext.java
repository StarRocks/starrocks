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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/*
 * Recorder CTE node info, contains:
 * 1. produce statistics
 * 2. consume node num(for CTE inline)
 * 3. consume required columns(for column prune)
 * 4. predicates/limit(for push down rule)
 *
 * Should update context when CTE node was update or
 * before CTE optimize(push down predicate/limit or inline CTE)
 *
 * */
public class CTEContext {
    private static final int MIN_EVERY_CTE_REFS = 3;

    // All CTE produce
    private List<Integer> produces;

    private Map<Integer, Statistics> produceStatistics;

    // Num of CTE consume
    private Map<Integer, Integer> consumeNums;

    private Map<Integer, List<ScalarOperator>> consumePredicates;

    private Map<Integer, List<Long>> consumeLimits;

    private boolean enableCTE;

    private final List<Integer> forceCTEList;

    private double inlineCTERatio = 2.0;

    private int maxCTELimit = 10;

    private int cteIdSequence = 0;

    public CTEContext() {
        forceCTEList = Lists.newArrayList();
    }

    public void reset() {
        produces = Lists.newArrayList();
        consumeNums = Maps.newHashMap();
        consumePredicates = Maps.newHashMap();
        consumeLimits = Maps.newHashMap();

        produceStatistics = Maps.newHashMap();
        cteIdSequence = 0;
    }

    public void setEnableCTE(boolean enableCTE) {
        this.enableCTE = enableCTE;
    }

    public void setInlineCTERatio(double ratio) {
        this.inlineCTERatio = ratio;
    }

    public void addCTEProduce(int cteId) {
        this.produces.add(cteId);
        cteIdSequence = Math.max(cteId, cteIdSequence);
    }

    public void setMaxCTELimit(int maxCTELimit) {
        this.maxCTELimit = maxCTELimit;
    }

    public void addCTEConsume(int cteId) {
        int i = this.consumeNums.getOrDefault(cteId, 0);
        this.consumeNums.put(cteId, i + 1);
    }

    public void addCTEStatistics(int cteId, Statistics statistics) {
        produceStatistics.put(cteId, statistics);
    }

    public Optional<Statistics> getCTEStatistics(int cteId) {
        if (produceStatistics.containsKey(cteId)) {
            return Optional.of(produceStatistics.get(cteId));
        }
        return Optional.empty();
    }

    public List<Integer> getAllCTEProduce() {
        return produces;
    }

    public int getCTEConsumeNum(int cteId) {
        if (!consumeNums.containsKey(cteId)) {
            return 0;
        }
        return consumeNums.get(cteId);
    }

    public Map<Integer, List<ScalarOperator>> getConsumePredicates() {
        return consumePredicates;
    }

    public Map<Integer, List<Long>> getConsumeLimits() {
        return consumeLimits;
    }

    public boolean needOptimizeCTE() {
        return consumeNums.values().stream().reduce(Integer::max).orElse(0) > 0 || !produces.isEmpty();
    }

    public boolean needPushPredicate() {
        for (Map.Entry<Integer, Integer> entry : consumeNums.entrySet()) {
            int cteId = entry.getKey();
            int nums = entry.getValue();

            if (consumePredicates.getOrDefault(cteId, Collections.emptyList()).size() >= nums) {
                return true;
            }
        }

        return false;
    }

    public boolean needPushLimit() {
        for (Map.Entry<Integer, Integer> entry : consumeNums.entrySet()) {
            int cteId = entry.getKey();
            int num = entry.getValue();

            if (consumeLimits.getOrDefault(cteId, Collections.emptyList()).size() >= num) {
                return true;
            }
        }

        return false;
    }

    /*
     * Inline CTE consume sense:
     * 0. All CTEConsumer been pruned
     * 1. Disable CTE reuse, must inline all CTE
     * 2. CTE consume only use once, it's meanings none CTE data reuse
     * 3. CTE ratio less zero
     * 4. limit CTE num strategy
     */
    public boolean needInline(int cteId) {
        // 0. All CTEConsumer been pruned
        if (!consumeNums.containsKey(cteId)) {
            return true;
        }

        if (forceCTEList.contains(cteId)) {
            return false;
        }

        // 1. Disable CTE reuse
        // 2. CTE consume only use once
        if (!enableCTE || consumeNums.getOrDefault(cteId, 0) <= 1) {
            return true;
        }

        // 3. CTE ratio less zero, force inline
        if (inlineCTERatio < 0) {
            return true;
        }

        if (inlineCTERatio == 0) {
            return false;
        }

        // 4. limit cte num strategy
        // when actual CTE > maxCTELimit
        // actual CTE refs > every CTE refs: force CTE
        // actual CTE refs < every CTE refs: force inline
        if (produces.size() > maxCTELimit) {
            return consumeNums.get(cteId) < MIN_EVERY_CTE_REFS;
        }
        return false;
    }

    public boolean hasInlineCTE() {
        for (int cteId : produces) {
            if (needInline(cteId)) {
                return true;
            }
        }

        return false;
    }

    public void addForceCTE(int cteId) {
        this.forceCTEList.add(cteId);
    }

    public boolean isForceCTE(int cteId) {
        // 1. rewrite to CTE rule, force CTE
        if (this.forceCTEList.contains(cteId)) {
            return true;
        }

        // 2. ratio is zero, force CTE reuse
        if (inlineCTERatio == 0) {
            return true;
        }

        if (inlineCTERatio < 0) {
            return false;
        }

        // 3. limit cte num strategy
        // when actual CTE > maxCTELimit
        // actual CTE refs > CTE num * every CTE refs: force CTE
        // actual CTE refs < CTE num * every CTE refs: force inline
        if (produces.size() > maxCTELimit) {
            return consumeNums.get(cteId) >= MIN_EVERY_CTE_REFS;
        }

        return false;
    }

    public int getNextCteId() {
        return ++cteIdSequence;
    }
}
