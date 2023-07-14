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
    public void setOutputPartition(DataPartition outputPartition) {
        Preconditions.checkState(false);
    }

    @Override
    public void setSink(DataSink sink) {
        Preconditions.checkState(false);
    }

    @Override
    public void reset() {
        MultiCastDataSink multiSink = (MultiCastDataSink) getSink();
        multiSink.getDestinations().forEach(List::clear);
    }
}
