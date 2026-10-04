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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/RepeatNode.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.Pair;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TNormalRepeatNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TRepeatNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used for grouping sets.
 * It will add some new rows and a column of groupingId according to grouping sets info.
 */
public class RepeatNode extends PlanNode {
    private List<Set<Integer>> repeatSlotIdList;
    private Set<Integer> allSlotId;
    private TupleDescriptor outputTupleDesc;
    private List<List<Long>> groupingList;
    private List<Integer> filter_null_value_columns = Lists.newArrayList();

    // only for unittest
    protected RepeatNode(PlanNodeId id, PlanNode input, List<Set<SlotId>> repeatSlotIdList,
                         TupleDescriptor outputTupleDesc, List<List<Long>> groupingList) {
        super(id, input.getTupleIds(), "REPEAT_NODE");
        this.children.add(input);
        this.repeatSlotIdList = buildIdSetList(repeatSlotIdList);
        this.groupingList = groupingList;
        this.outputTupleDesc = outputTupleDesc;
        tupleIds.add(outputTupleDesc.getId());
    }

    public RepeatNode(PlanNodeId id, PlanNode input, TupleDescriptor outputTupleDesc,
                      List<Set<Integer>> repeatSlotIdList,
                      List<List<Long>> groupingList) {
        super(id, input.getTupleIds(), "REPEAT_NODE");
        this.children.add(input);

        this.outputTupleDesc = outputTupleDesc;
        this.repeatSlotIdList = repeatSlotIdList;
        this.groupingList = groupingList;
        Set<Integer> allSlotId = new HashSet<>();
        for (Set<Integer> s : this.repeatSlotIdList) {
            allSlotId.addAll(s);
        }
        this.allSlotId = allSlotId;
        tupleIds.add(outputTupleDesc.getId());
    }

    private static List<Set<Integer>> buildIdSetList(List<Set<SlotId>> repeatSlotIdList) {
        List<Set<Integer>> slotIdList = new ArrayList<>();
        for (Set slotSet : repeatSlotIdList) {
            Set<Integer> intSet = new HashSet<>();
            for (Object slotId : slotSet) {
                intSet.add(((SlotId) slotId).asInt());
            }
            slotIdList.add(intSet);
        }

        return slotIdList;
    }

    @Override
    public void computeStats() {
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REPEAT_NODE;
        msg.repeat_node = new TRepeatNode(outputTupleDesc.getId().asInt(), repeatSlotIdList, groupingList.get(0),
                groupingList, allSlotId);
        msg.setFilter_null_value_columns(filter_null_value_columns);
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("Repeat", repeatSlotIdList.size()).addValue(
                super.debugString()).toString();
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(detailPrefix + "repeat: repeat ");
        output.append(repeatSlotIdList.size() - 1);
        output.append(" lines ");
        output.append(repeatSlotIdList);
        output.append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
        }

        if (CollectionUtils.isNotEmpty(outputTupleDesc.getSlots()) &&
                outputTupleDesc.getSlots().stream().noneMatch(slot -> slot.getColumn() == null)) {
            output.append(detailPrefix + "generate: ");
            output.append(outputTupleDesc.getSlots().stream().map(slot -> "`" + slot.getColumn().getName() + "`")
                    .collect(Collectors.joining(", ")) + "\n");
        }

        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterPushDownContext context, Expr probeExpr,
                                          List<Expr> partitionByExprs) {
        // Below this node a row still carries its real value in every grouping key, but above it the
        // row is also replicated into the grouping sets that replace some of those keys with NULL.
        // Dropping the row on such a key here therefore also drops it from those NULL-keyed groups.
        // That is only harmless when the filter would have rejected the NULL-keyed copy as well;
        // otherwise e.g. the ROLLUP grand-total row silently loses rows.
        if (mayPassNull(context.getDescription()) && usesNulledSlot(probeExpr)) {
            return false;
        }
        // A partitioned (shuffle) filter picks the partition to probe by hashing partitionByExprs. On a
        // nulled key that hash differs between the real value seen here and the NULL a copy carries above,
        // so a row can be probed against the wrong partition and dropped regardless of the filter's NULL
        // policy.
        if (partitionByExprs.stream().anyMatch(this::usesNulledSlot)) {
            return false;
        }
        return super.pushDownRuntimeFilters(context, probeExpr, partitionByExprs);
    }

    private static boolean mayPassNull(RuntimeFilterDescription description) {
        // TOPN_FILTER and AGG_IN_FILTER are built from the aggregation's own groups, so they can keep a
        // NULL-keyed group (NULLS FIRST, or a NULL group among the first LIMIT groups).
        return description.runtimeFilterType().isTopNFilter()
                || description.runtimeFilterType().isAggInFilter()
                || description.getEqualForNull();
    }

    // Whether expr uses a grouping key that some grouping set replaces with NULL.
    private boolean usesNulledSlot(Expr expr) {
        for (int slotId : expr.getUsedSlotIds()) {
            boolean grouped = repeatSlotIdList.stream().anyMatch(s -> s.contains(slotId));
            boolean nulled = repeatSlotIdList.stream().anyMatch(s -> !s.contains(slotId));
            if (grouped && nulled) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkRuntimeFilterOnNullValue(RuntimeFilterDescription description, Expr probeExpr) {
        // note(yan): repeat node may generate null values, and if runtime filter does not accept null value
        // we have opportunity to filter those values out.
        boolean slotRefWithNullValue = false;
        SlotId slotId = null;
        if (probeExpr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) probeExpr;
            if (slotRef.isNullable()) {
                slotRefWithNullValue = true;
                slotId = slotRef.getSlotId();
            }
        }

        if (!description.getEqualForNull() && slotRefWithNullValue) {
            filter_null_value_columns.add(slotId.asInt());
        }
    }
    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalRepeatNode repeatNode = new TNormalRepeatNode();
        repeatNode.setOutput_tuple_id(normalizer.remapTupleId(outputTupleDesc.getId()).asInt());
        List<Integer> allSlotIds = new ArrayList<>(allSlotId);
        repeatNode.setAll_slot_ids(
                normalizer.remapIntegerSlotIds(allSlotIds).stream().sorted().collect(Collectors.toList()));
        List<List<Integer>> slotIdSetList = repeatSlotIdList.stream()
                .map(s -> {
                    List<Integer> slotIds = normalizer.remapIntegerSlotIds(new ArrayList<>(s)).stream().sorted()
                            .collect(Collectors.toList());
                    String key = Strings.join(slotIds.stream().map(id -> "" + id).collect(Collectors.toList()), ',');
                    return new Pair<>(slotIds, key);
                }).sorted(Pair.comparingBySecond()).map(p -> p.first).collect(Collectors.toList());
        repeatNode.setSlot_id_set_list(slotIdSetList);
        repeatNode.setRepeat_id_list(groupingList.get(0));
        repeatNode.setGrouping_list(groupingList);
        planNode.setRepeat_node(repeatNode);
        planNode.setNode_type(TPlanNodeType.REPEAT_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }

    @Override
    public boolean needCollectExecStats() {
        return true;
    }
}
