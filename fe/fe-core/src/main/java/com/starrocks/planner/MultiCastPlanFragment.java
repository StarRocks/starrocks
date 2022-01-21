// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.planner;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.starrocks.analysis.Analyzer;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.util.List;
import java.util.stream.Collectors;

/*
 *  For Multi-cast, like:
 *
 *  Fragment-2              Fragment-3
 *      \                       /
 *   shuffle by v1      shuffle by v2
 *             \         /
 *              Fragment-1
 *
 */
public class MultiCastPlanFragment extends PlanFragment {
    private final List<ExchangeNode> destNodeList = Lists.newArrayList();

    public List<ExchangeNode> getDestNodeList() {
        return destNodeList;
    }

    public MultiCastPlanFragment(PlanFragment planFragment) {
        super(planFragment.fragmentId, planFragment.planRoot, planFragment.getDataPartition());
        this.outputPartition = planFragment.getOutputPartition();
        this.children.addAll(planFragment.getChildren());
    }

    public List<PlanFragment> getDestFragmentList() {
        return destNodeList.stream().map(PlanNode::getFragment).collect(Collectors.toList());
    }

    @Override
    public void finalize(Analyzer analyzer, boolean validateFileFormats) {
        if (sink != null) {
            return;
        }

        Preconditions.checkState(!destNodeList.isEmpty(), "MultiCastPlanFragment don't support return result");

        MultiCastDataSink multiCastDataSink = new MultiCastDataSink();
        this.sink = multiCastDataSink;

        for (ExchangeNode f : destNodeList) {
            DataStreamSink streamSink = new DataStreamSink(f.getId());
            streamSink.setPartition(f.getFragment().getOutputPartition());
            streamSink.setFragment(this);
            multiCastDataSink.getDataStreamSinks().add(streamSink);
            multiCastDataSink.getDestinations().add(Lists.newArrayList());
        }
    }

    @Override
    public void finalizeForStatistic(boolean isStatistic) {
        Preconditions.checkState(false, "MultiCastPlanFragment don't support statistic query");
    }

    @Override
    public PlanFragment getDestFragment() {
        Preconditions.checkState(false);
        return null;
    }

    @Override
    public void setDestination(ExchangeNode destNode) {
        Preconditions.checkState(false);
    }

    @Override
    public void setParallelExecNum(int parallelExecNum) {
        Preconditions.checkState(false);
    }

    @Override
    public int getNumNodes() {
        Preconditions.checkState(false);
        return 0;
    }

    @Override
    public void setOutputPartition(DataPartition outputPartition) {
        Preconditions.checkState(false);
    }

    @Override
    public void setSink(DataSink sink) {
        Preconditions.checkState(false);
    }

    @Override
    public void setTransferQueryStatisticsWithEveryBatch(boolean value) {
        Preconditions.checkState(false);
    }

    @Override
    public void setLoadGlobalDicts(List<Pair<Integer, ColumnDict>> loadGlobalDicts) {
        Preconditions.checkState(false);
    }
}
