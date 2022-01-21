// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/*
 * Recorder CTE node info, contains:
 * 1. produce groupExpression(for get statistics)
 * 2. consume inline plan costs(for CTE inline)
 * 3. produce costs(for CTE inline)
 * 4. consume node nums(for CTE inline)
 * 5. consume required columns(for column prune)
 * 6. predicates/limit(for push down rule)
 *
 * Should update context when CTE node was update or
 * before CTE optimize(push down predicate/limit or inline CTE)
 *
 * */
public class CTEContext {
    // All CTE produce, OptExpression should bind GroupExpression
    private Map<String, OptExpression> produces;

    // All CTE consume inline costs
    private Map<String, List<Double>> consumeInlineCosts;

    // All Cte produce costs
    private Map<String, Double> produceCosts;

    // Nums of CTE consume
    private Map<String, Integer> consumeNums;

    // Consume required columns
    private Map<String, ColumnRefSet> requiredColumns;

    private Map<String, List<ScalarOperator>> consumePredicates;

    private Map<String, List<Long>> consumeLimits;

    private boolean enableCTE;

    public CTEContext() {
    }

    public void reset() {
        produces = Maps.newHashMap();
        consumeNums = Maps.newHashMap();
        requiredColumns = Maps.newHashMap();
        consumePredicates = Maps.newHashMap();
        consumeLimits = Maps.newHashMap();

        consumeInlineCosts = Maps.newHashMap();
        produceCosts = Maps.newHashMap();
    }

    public void setEnableCTE(boolean enableCTE) {
        this.enableCTE = enableCTE;
    }

    public void addCTEProduce(String cteId, OptExpression produce) {
        this.produces.put(cteId, produce);
    }

    public void addCTEConsume(String cteId) {
        int i = this.consumeNums.getOrDefault(cteId, 0);
        this.consumeNums.put(cteId, i + 1);
    }

    public void addCTEProduceCost(String cteId, double cost) {
        produceCosts.put(cteId, cost);
    }

    public void addCTEConsumeInlineCost(String cteId, double cost) {
        if (consumeInlineCosts.containsKey(cteId)) {
            consumeInlineCosts.get(cteId).add(cost);
        } else {
            consumeInlineCosts.put(cteId, Lists.newArrayList(cost));
        }
    }

    public OptExpression getCTEProduce(String cteId) {
        Preconditions.checkState(produces.containsKey(cteId));
        return produces.get(cteId);
    }

    public int getCTEConsumeNums(String cteId) {
        Preconditions.checkState(consumeNums.containsKey(cteId));
        return consumeNums.get(cteId);
    }

    public Map<String, ColumnRefSet> getRequiredColumns() {
        return requiredColumns;
    }

    public ColumnRefSet getAllRequiredColumns() {
        ColumnRefSet all = new ColumnRefSet();
        requiredColumns.values().forEach(all::union);
        return all;
    }

    public Map<String, List<ScalarOperator>> getConsumePredicates() {
        return consumePredicates;
    }

    public Map<String, List<Long>> getConsumeLimits() {
        return consumeLimits;
    }

    public boolean needOptimizeCTE() {
        return consumeNums.values().stream().reduce(Integer::max).orElse(0) > 1;
    }

    public boolean needPushPredicate() {
        for (Map.Entry<String, Integer> entry : consumeNums.entrySet()) {
            String cteId = entry.getKey();
            int nums = entry.getValue();

            if (consumePredicates.getOrDefault(cteId, Collections.emptyList()).size() >= nums) {
                return true;
            }
        }

        return false;
    }

    public boolean needPushLimit() {
        for (Map.Entry<String, Integer> entry : consumeNums.entrySet()) {
            String cteId = entry.getKey();
            int nums = entry.getValue();

            if (consumeLimits.getOrDefault(cteId, Collections.emptyList()).size() >= nums) {
                return true;
            }
        }

        return false;
    }

    /*
     * Inline CTE consume sense:
     * 1. Disable CTE reuse, must inline all CTE
     * 2. CTE consume only use once, it's meanings none CTE data reuse
     * 3. CTE produce cost > sum of all consume inline costs, should considered CTE extract cost(network/memory),
     *    so produce cost will multi a rate
     *
     */
    public boolean needInline(String cteId) {
        // 1. Disable CTE reuse
        // 2. CTE consume only use once
        if (!enableCTE || consumeNums.getOrDefault(cteId, 0) <= 1) {
            return true;
        }

        // Wasn't collect costs, don't inline
        if (produceCosts.isEmpty() && consumeInlineCosts.isEmpty()) {
            return false;
        }

        Preconditions.checkState(produceCosts.containsKey(cteId));
        Preconditions.checkState(consumeInlineCosts.containsKey(cteId));

        double p = produceCosts.get(cteId);
        double c = consumeInlineCosts.get(cteId).stream().reduce(Double::sum).orElse(Double.MAX_VALUE);

        // if was none statistic, default don't inline
        if (p <= 0 || c <= 0) {
            return false;
        }

        // 3. CTE produce cost > sum of all consume inline costs
        // Consume output rows less produce rows * rate choose inline
        return p * StatisticsEstimateCoefficient.DEFAULT_CTE_INLINE_COST_RATE >= c;
    }

    public boolean hasInlineCTE() {
        for (String cteId : produces.keySet()) {
            if (needInline(cteId)) {
                return true;
            }
        }

        return false;
    }

}
