// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger LOG = LogManager.getLogger(CTEContext.class);

    // All CTE produce, OptExpression should bind GroupExpression
    private Map<Integer, OptExpression> produces;

    // All CTE consume inline costs
    private Map<Integer, List<Double>> consumeInlineCosts;

    // All Cte produce costs
    private Map<Integer, Double> produceCosts;

    // Nums of CTE consume
    private Map<Integer, Integer> consumeNums;

    // Consume required columns
    private Map<Integer, ColumnRefSet> requiredColumns;

    private Map<Integer, List<ScalarOperator>> consumePredicates;

    private Map<Integer, List<Long>> consumeLimits;

    // Evaluate the complexity of the produce plan.
    // Why need this?
    // * Join will produce GlobalRuntimeFilter, will let us can't
    //   calculate the exact join cost
    // * Multi-Equivalence predicate will case inaccurate rows selectivity
    //
    // So in some simpler cases, the effect of directly reuse cte may be worse.
    // We want to keep complexity score between 0~2
    private Map<Integer, Double> produceComplexityScores;

    private boolean enableCTE;

    private final List<Integer> forceCTEList;

    private double inlineCTERatio = 2.0;

    private int cteIdSequence = 0;

    public CTEContext() {
        forceCTEList = Lists.newArrayList();
    }

    public void reset() {
        produces = Maps.newHashMap();
        consumeNums = Maps.newHashMap();
        requiredColumns = Maps.newHashMap();
        consumePredicates = Maps.newHashMap();
        consumeLimits = Maps.newHashMap();

        consumeInlineCosts = Maps.newHashMap();
        produceCosts = Maps.newHashMap();
        produceComplexityScores = Maps.newHashMap();
        cteIdSequence = 0;
    }

    public void setEnableCTE(boolean enableCTE) {
        this.enableCTE = enableCTE;
    }

    public void setInlineCTERatio(double ratio) {
        this.inlineCTERatio = ratio;
    }

    public void addCTEProduce(int cteId, OptExpression produce) {
        this.produces.put(cteId, produce);
        cteIdSequence = Math.max(cteId, cteIdSequence);
    }

    public void addCTEConsume(int cteId) {
        int i = this.consumeNums.getOrDefault(cteId, 0);
        this.consumeNums.put(cteId, i + 1);
    }

    public void addCTEProduceComplexityScores(int cteId, double score) {
        produceComplexityScores.put(cteId, score);
    }

    public void addCTEProduceCost(int cteId, double cost) {
        produceCosts.put(cteId, cost);
    }

    public void addCTEConsumeInlineCost(int cteId, double cost) {
        if (consumeInlineCosts.containsKey(cteId)) {
            consumeInlineCosts.get(cteId).add(cost);
        } else {
            consumeInlineCosts.put(cteId, Lists.newArrayList(cost));
        }
    }

    public OptExpression getCTEProduce(int cteId) {
        Preconditions.checkState(produces.containsKey(cteId));
        return produces.get(cteId);
    }

    public Map<Integer, OptExpression> getAllCTEProduce() {
        return produces;
    }

    public int getCTEConsumeNums(int cteId) {
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
        return consumeNums.values().stream().reduce(Integer::max).orElse(0) > 0 || produces.size() > 0;
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
            int nums = entry.getValue();

            if (consumeLimits.getOrDefault(cteId, Collections.emptyList()).size() >= nums) {
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
     * 3. CTE produce cost > sum of all consume inline costs, should considered CTE extract cost(network/memory),
     *    so produce cost will multi a rate
     *
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

        // Wasn't collect costs, don't inline
        if (produceCosts.isEmpty() && consumeInlineCosts.isEmpty()) {
            return false;
        }

        Preconditions.checkState(produceCosts.containsKey(cteId));
        if (!consumeInlineCosts.containsKey(cteId)) {
            return false;
        }

        if (inlineCTERatio <= 0) {
            return false;
        }

        double p = produceCosts.get(cteId);
        double c = consumeInlineCosts.get(cteId).stream().reduce(Double::sum).orElse(Double.MAX_VALUE);

        // 3. Statistic is unknown, must inline
        if (p <= 0 || c <= 0) {
            return true;
        }

        // 4. CTE produce cost > sum of all consume inline costs
        // Consume output rows less produce rows * rate choose inline
        return p * (inlineCTERatio + produceComplexityScores.getOrDefault(cteId, 0D)) > c;
    }

    public boolean hasInlineCTE() {
        for (int cteId : produces.keySet()) {
            if (needInline(cteId)) {
                return true;
            }
        }

        return false;
    }

    public void addForceCTE(int cteId) {
        this.forceCTEList.add(cteId);
    }

    public int getNextCteId() {
        return ++cteIdSequence;
    }
}
