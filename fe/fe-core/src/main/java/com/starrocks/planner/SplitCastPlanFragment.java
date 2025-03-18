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

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.thrift.TResultSinkType;

import java.util.List;
import java.util.stream.Collectors;

public class SplitCastPlanFragment extends PlanFragment {
    private final List<ExchangeNode> destNodeList = Lists.newArrayList();
    private final List<Expr> splitExprs = Lists.newArrayList();

    private final List<DataPartition> outputPartitions = Lists.newArrayList();

    public List<ExchangeNode> getDestNodeList() {
        return destNodeList;
    }

    public List<Expr> getSplitExprs() {
        return splitExprs;
    }

    public SplitCastPlanFragment(PlanFragment planFragment) {
        super(planFragment.fragmentId, planFragment.planRoot, planFragment.getDataPartition());
        this.outputPartition = DataPartition.HYBRID_HASH_PARTITIONED;
        this.children.addAll(planFragment.getChildren());
        this.setLoadGlobalDicts(planFragment.loadGlobalDicts);
        this.setQueryGlobalDicts(planFragment.queryGlobalDicts);
    }

    // get the list of destination fragments by exchange nodes
    public List<PlanFragment> getDestFragmentList() {
        return destNodeList.stream().map(PlanNode::getFragment).collect(Collectors.toList());
    }

    public ExchangeNode getDestNode(int index) {
        return destNodeList.get(index);
    }

    public List<DataPartition> getOutputPartitions() {
        return outputPartitions;
    }

    @Override
    public void createDataSink(TResultSinkType resultSinkType) {
        if (sink != null) {
            return;
        }

        Preconditions.checkState(!destNodeList.isEmpty(), "SplitPlanFragment don't support return result");

        SplitCastDataSink splitCastDataSink = new SplitCastDataSink();

        this.sink = splitCastDataSink;

        for (int i = 0; i < destNodeList.size(); i++) {
            DataStreamSink streamSink = new DataStreamSink(destNodeList.get(i).getId());
            streamSink.setPartition(outputPartitions.get(i));
            streamSink.setFragment(this);
            splitCastDataSink.getDataStreamSinks().add(streamSink);
            splitCastDataSink.getDestinations().add(Lists.newArrayList());
            splitCastDataSink.getSplitExprs().add(splitExprs.get(i));
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
    public void setOutputPartition(DataPartition outputPartition) {
        Preconditions.checkState(false);
    }

    @Override
    public void setSink(DataSink sink) {
        Preconditions.checkState(false);
    }

    @Override
    public void reset() {
        SplitCastDataSink splitCastDataSink = (SplitCastDataSink) getSink();
        // clear destinations before retry
        splitCastDataSink.getDestinations().forEach(List::clear);
    }
}

