// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
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
    // All CTE produce
    private List<Integer> produces;

    private Map<Integer, Statistics> produceStatistics;

    // Num of CTE consume
    private Map<Integer, Integer> consumeNums;

    // Consume required columns
    private Map<Integer, ColumnRefSet> requiredColumns;

    private Map<Integer, List<ScalarOperator>> consumePredicates;

    private Map<Integer, List<Long>> consumeLimits;

    private boolean enableCTE;

    private final List<Integer> forceCTEList;

    private double inlineCTERatio = 2.0;

    public CTEContext() {
        forceCTEList = Lists.newArrayList();
    }

    public void reset() {
        produces = Lists.newArrayList();
        consumeNums = Maps.newHashMap();
        requiredColumns = Maps.newHashMap();
        consumePredicates = Maps.newHashMap();
        consumeLimits = Maps.newHashMap();

        produceStatistics = Maps.newHashMap();
    }

    public void setEnableCTE(boolean enableCTE) {
        this.enableCTE = enableCTE;
    }

    public void setInlineCTERatio(double ratio) {
        this.inlineCTERatio = ratio;
    }

    public void addCTEProduce(int cteId) {
        this.produces.add(cteId);
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

    public Map<Integer, ColumnRefSet> getRequiredColumns() {
        return requiredColumns;
    }

    public ColumnRefSet getAllRequiredColumns() {
        ColumnRefSet all = new ColumnRefSet();
        requiredColumns.values().forEach(all::union);
        return all;
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
     * 1. Disable CTE reuse, must inline all CTE
     * 2. CTE consume only use once, it's meanings none CTE data reuse
     * 3. CTE ratio less zero
     */
    public boolean needInline(int cteId) {
        if (forceCTEList.contains(cteId)) {
            return false;
        }

        // 1. Disable CTE reuse
        // 2. CTE consume only use once
        if (!enableCTE || consumeNums.getOrDefault(cteId, 0) <= 1) {
            return true;
        }

        // 3. Force inline
        return inlineCTERatio < 0;
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
        return inlineCTERatio == 0 || this.forceCTEList.contains(cteId);
    }
}
