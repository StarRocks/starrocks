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

package com.starrocks.qe.scheduler.dag;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DataStreamSink;
import com.starrocks.planner.ExceptNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.IntersectNode;
import com.starrocks.planner.MultiCastDataSink;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;

import java.util.List;
import java.util.Map;

// build a reverse schedule order
// eg:
// ┌─────────────────────────────┐
// │                             │
// │           Join              │
// │             │               │
// │             │               │
// │        ┌────┴──────┐        │
// │        │           │        │
// │        ▼           ▼        │
// │       Join      Exchange    │
// │        │           │        │
// │        │           │        │
// │   ┌────┴───┐       │        │
// │   │        │       │        │
// │   ▼        ▼       │        │
// │ Exchange  Exchange │        │
// └───┬────────┬───────┼────────┘
//     │        │       │
//     │        │       │
//     │        │       │
//     ▼        ▼       ▼
//     F1       F2      F3
// the fragment child schedule order should be [F1, F2, F3]
// In phased schedule we should schedule F3 firstly. When F3 finished, then schedule F2
public class FragmentSequence {

    private final ExecutionDAG dag;
    private final List<PlanFragmentId> scheduleOrderList;

    public FragmentSequence(ExecutionDAG dag, List<PlanFragmentId> fragmentIds) {
        this.dag = dag;
        this.scheduleOrderList = fragmentIds;
    }

    public ExecutionFragment getAt(int i) {
        final PlanFragmentId fragmentId = scheduleOrderList.get(i);
        return dag.getFragment(fragmentId);
    }

    public static class Builder {
        private static void buildExgNodeIdToSourceFragmentId(ExecutionFragment fragment,
                                                             Map<PlanNodeId, PlanFragmentId> exchangeToSource) {
            final DataSink sink = fragment.getPlanFragment().getSink();
            if (sink instanceof DataStreamSink) {
                exchangeToSource.put(sink.getExchNodeId(), fragment.getFragmentId());
            } else if (sink instanceof MultiCastDataSink) {
                for (DataStreamSink dataStreamSink : ((MultiCastDataSink) sink).getDataStreamSinks()) {
                    exchangeToSource.put(dataStreamSink.getExchNodeId(), fragment.getFragmentId());
                }
            }
            for (int i = 0; i < fragment.childrenSize(); i++) {
                buildExgNodeIdToSourceFragmentId(fragment.getChild(i), exchangeToSource);
            }
        }

        public static Map<PlanFragmentId, FragmentSequence> build(ExecutionDAG dag, ExecutionFragment root) {

            Map<PlanNodeId, PlanFragmentId> exchangeToSource = Maps.newHashMap();
            buildExgNodeIdToSourceFragmentId(root, exchangeToSource);

            Map<PlanFragmentId, FragmentSequence> result = Maps.newHashMap();
            buildIfNotExists(dag, root, exchangeToSource, result);

            return result;
        }

        private static void buildIfNotExists(ExecutionDAG dag, ExecutionFragment node,
                                             Map<PlanNodeId, PlanFragmentId> exchangeToFragment,
                                             Map<PlanFragmentId, FragmentSequence> sequenceMap) {
            if (sequenceMap.containsKey(node.getFragmentId())) {
                return;
            }

            final List<PlanFragmentId> childFragments = Lists.newArrayList();
            build(node.getPlanFragment().getPlanRoot(), childFragments, exchangeToFragment);
            final FragmentSequence sequence = new FragmentSequence(dag, Lists.reverse(childFragments));
            sequenceMap.put(node.getFragmentId(), sequence);

            for (int i = 0; i < node.childrenSize(); i++) {
                buildIfNotExists(dag, node.getChild(i), exchangeToFragment, sequenceMap);
            }
        }

        private static void build(PlanNode node, List<PlanFragmentId> list,
                                  Map<PlanNodeId, PlanFragmentId> exchangeToFragment) {

            if (node instanceof ExceptNode || node instanceof IntersectNode) {
                for (PlanNode child : node.getChildren()) {
                    if (child instanceof ExchangeNode) {
                        list.add(exchangeToFragment.get(child.getId()));
                        continue;
                    }
                    build(child, list, exchangeToFragment);
                }
            } else {
                if (node instanceof ExchangeNode) {
                    list.add(exchangeToFragment.get(node.getId()));
                    return;
                }
                for (int i = node.getChildren().size() - 1; i >= 0; i--) {
                    final PlanNode child = node.getChild(i);
                    if (child instanceof ExchangeNode) {
                        list.add(exchangeToFragment.get(child.getId()));
                        continue;
                    }
                    build(child, list, exchangeToFragment);
                }

            }

        }
    }

}
