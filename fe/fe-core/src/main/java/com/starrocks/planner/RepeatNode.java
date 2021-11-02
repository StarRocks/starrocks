// This file is made available under Elastic License 2.0.
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
import com.google.common.base.Preconditions;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.GroupingInfo;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TRepeatNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used for grouping sets.
 * It will add some new rows and a column of groupingId according to grouping sets info.
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class RepeatNode extends PlanNode {
    private List<Set<Integer>> repeatSlotIdList;
    private Set<Integer> allSlotId;
    private TupleDescriptor outputTupleDesc;
    private List<List<Long>> groupingList;
    private GroupingInfo groupingInfo;
    private PlanNode input;
    private GroupByClause groupByClause;

    protected RepeatNode(PlanNodeId id, PlanNode input, GroupingInfo groupingInfo, GroupByClause groupByClause) {
        super(id, input.getTupleIds(), "REPEAT_NODE");
        this.children.add(input);
        this.groupingInfo = groupingInfo;
        this.input = input;
        this.groupByClause = groupByClause;
    }

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
    public void computeStats(Analyzer analyzer) {
        avgRowSize = 0;
        cardinality = 0;
        numNodes = 1;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        Preconditions.checkState(conjuncts.isEmpty());
        groupByClause.substituteGroupingExprs(groupingInfo.getGroupingSlots(), input.getOutputSmap(),
                analyzer);

        for (Expr expr : groupByClause.getGroupingExprs()) {
            if (expr instanceof SlotRef || (expr instanceof GroupingFunctionCallExpr)) {
                continue;
            }
            throw new AnalysisException("function or expr is not allowed in grouping sets clause.");
        }

        // build new BitSet List for tupleDesc
        Set<SlotDescriptor> slotDescSet = new HashSet<>();
        for (TupleId tupleId : input.getTupleIds()) {
            TupleDescriptor tupleDescriptor = analyzer.getDescTbl().getTupleDesc(tupleId);
            slotDescSet.addAll(tupleDescriptor.getSlots());
        }

        // build tupleDesc according to child's tupleDesc info
        outputTupleDesc = groupingInfo.getVirtualTuple();
        //set aggregate nullable
        for (Expr slot : groupByClause.getGroupingExprs()) {
            if (slot instanceof SlotRef) {
                ((SlotRef) slot).getDesc().setIsNullable(true);
            }
        }
        outputTupleDesc.computeMemLayout();

        List<Set<SlotId>> groupingIdList = new ArrayList<>();
        List<Expr> exprList = groupByClause.getGroupingExprs();
        Preconditions.checkState(exprList.size() >= 2);
        allSlotId = new HashSet<>();
        for (BitSet bitSet : Collections.unmodifiableList(groupingInfo.getGroupingIdList())) {
            Set<SlotId> slotIdSet = new HashSet<>();
            for (SlotDescriptor slotDesc : slotDescSet) {
                SlotId slotId = slotDesc.getId();
                if (slotId == null) {
                    continue;
                }
                for (int i = 0; i < exprList.size(); i++) {
                    if (exprList.get(i) instanceof SlotRef) {
                        SlotRef slotRef = (SlotRef) (exprList.get(i));
                        if (bitSet.get(i) && slotRef.getSlotId() == slotId) {
                            slotIdSet.add(slotId);
                            break;
                        }
                    }
                }
            }
            groupingIdList.add(slotIdSet);
        }

        this.repeatSlotIdList = buildIdSetList(groupingIdList);
        for (Set<Integer> s : this.repeatSlotIdList) {
            allSlotId.addAll(s);
        }
        this.groupingList = groupingInfo.genGroupingList(groupByClause.getGroupingExprs());
        tupleIds.add(outputTupleDesc.getId());
        for (TupleId id : tupleIds) {
            analyzer.getTupleDesc(id).setIsMaterialized(true);
        }
        computeMemLayout(analyzer);
        computeStats(analyzer);
        createDefaultSmap(analyzer);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REPEAT_NODE;
        msg.repeat_node = new TRepeatNode(outputTupleDesc.getId().asInt(), repeatSlotIdList, groupingList.get(0),
                groupingList, allSlotId);
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
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }

    @Override
    public boolean isVectorized() {
        for (PlanNode node : getChildren()) {
            if (!node.isVectorized()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }
}
