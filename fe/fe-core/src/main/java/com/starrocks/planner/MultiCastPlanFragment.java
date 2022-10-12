// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.starrocks.thrift.TResultSinkType;

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
        // Use random, only send to self
        this.outputPartition = DataPartition.RANDOM;
        this.children.addAll(planFragment.getChildren());
        this.setLoadGlobalDicts(planFragment.loadGlobalDicts);
        this.setQueryGlobalDicts(planFragment.queryGlobalDicts);
    }

    public List<PlanFragment> getDestFragmentList() {
        return destNodeList.stream().map(PlanNode::getFragment).collect(Collectors.toList());
    }

    public ExchangeNode getDestNode(int index) {
        return destNodeList.get(index);
    }

    @Override
    public void createDataSink(TResultSinkType resultSinkType) {
        if (sink != null) {
            return;
        }

        Preconditions.checkState(!destNodeList.isEmpty(), "MultiCastPlanFragment don't support return result");

        MultiCastDataSink multiCastDataSink = new MultiCastDataSink();
        this.sink = multiCastDataSink;

        for (ExchangeNode f : destNodeList) {
            DataStreamSink streamSink = new DataStreamSink(f.getId());
            streamSink.setPartition(DataPartition.RANDOM);
            streamSink.setFragment(this);
            streamSink.setOutputColumnIds(f.getReceiveColumns());
            multiCastDataSink.getDataStreamSinks().add(streamSink);
            multiCastDataSink.getDestinations().add(Lists.newArrayList());
        }
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

}
