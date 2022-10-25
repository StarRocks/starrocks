// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.cost;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class HashJoinCostModel {

    private static final Logger LOG = LogManager.getLogger(HashJoinCostModel.class);
    private static final String EMPTY = "EMPTY";

    private static final String BROADCAST = "BROADCAST";

    private static final String SHUFFLE = "SHUFFLE";

    private static final int BOTTOM_NUMBER = 100000;

    private final Statistics leftStatistics;

    private final Statistics rightStatistics;

    private final ExpressionContext context;

    private final List<PhysicalPropertySet> inputProperties;

    private final List<BinaryPredicateOperator> eqOnPredicates;

    public HashJoinCostModel(ExpressionContext context, List<PhysicalPropertySet> inputProperties,
                             List<BinaryPredicateOperator> eqOnPredicates) {
        this.context = context;
        this.leftStatistics = context.getChildStatistics(0);
        this.rightStatistics = context.getChildStatistics(1);
        this.inputProperties = inputProperties;
        this.eqOnPredicates = eqOnPredicates;
    }

    public double getCpuCost() {
        String execMode = deriveJoinExecMode();
        double buildCost;
        double probeCost;
        double leftOutput = leftStatistics.getOutputSize(context.getChildOutputColumns(0));
        double rightOutput = rightStatistics.getOutputSize(context.getChildOutputColumns(1));
        int beNum = Math.max(1, ConnectContext.get().getAliveBackendNumber());
        switch (execMode) {
            case BROADCAST:
                buildCost = rightOutput;
                probeCost = leftOutput * getAvgProbeCost();
                break;
            case SHUFFLE:
                buildCost = rightOutput / beNum;
                probeCost = leftOutput * getAvgProbeCost();
                break;
            default:
                buildCost = rightOutput;
                probeCost = leftOutput;
        }
        return buildCost + probeCost;
    }

    public double getMemCost() {
        String execMode = deriveJoinExecMode();
        double rightOutput = rightStatistics.getOutputSize(context.getChildOutputColumns(1));
        double memCost;
        int beNum = Math.max(1, ConnectContext.get().getAliveBackendNumber());
        switch (execMode) {
            case BROADCAST:
                memCost = rightOutput * beNum;
                break;
            default:
                memCost = rightOutput;
        }
        return memCost;
    }

    private double getAvgProbeCost() {
        String execMode = deriveJoinExecMode();
        double keySize = 0;
        for (BinaryPredicateOperator predicateOperator : eqOnPredicates) {
            ColumnRefOperator leftCol = (ColumnRefOperator) predicateOperator.getChild(0);
            ColumnRefOperator rightCol = (ColumnRefOperator) predicateOperator.getChild(1);
            if (context.getChildStatistics(1).getColumnStatistics().containsKey(leftCol)) {
                keySize += context.getChildStatistics(1).getColumnStatistic(leftCol).getAverageRowSize();
            } else if (context.getChildStatistics(1).getColumnStatistics().containsKey(rightCol)) {
                keySize += context.getChildStatistics(1).getColumnStatistic(rightCol).getAverageRowSize();
            }
        }
        double degradeRatio;
        int parallelFactor = Math.max(ConnectContext.get().getAliveBackendNumber(),
                ConnectContext.get().getSessionVariable().getDegreeOfParallelism()) * 2;
        double mapSize = Math.min(1, keySize) * rightStatistics.getOutputRowCount();
        switch (execMode) {
            case BROADCAST:
                degradeRatio = Math.max(1, Math.log(mapSize / BOTTOM_NUMBER));
                break;
            default:
                degradeRatio = Math.max(1, (Math.log(mapSize / BOTTOM_NUMBER) -
                        Math.log(parallelFactor) / Math.log(2)));
        }
        LOG.debug("execMode: {}, degradeRatio: {}", execMode, degradeRatio);
        return degradeRatio;
    }

    private String deriveJoinExecMode() {
        if (inputProperties == null) {
            return EMPTY;
        } else if (inputProperties.get(1).getDistributionProperty().isBroadcast()) {
            return BROADCAST;
        } else {
            return SHUFFLE;
        }
    }
}
