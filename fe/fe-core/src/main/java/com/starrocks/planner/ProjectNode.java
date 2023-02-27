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

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TNormalProjectNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TProjectNode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ProjectNode extends PlanNode {
    private final Map<SlotId, Expr> slotMap;
    private final Map<SlotId, Expr> commonSlotMap;

    public ProjectNode(PlanNodeId id, TupleDescriptor tupleDescriptor, PlanNode child,
                       Map<SlotId, Expr> slotMap,
                       Map<SlotId, Expr> commonSlotMap) {
        super(id, tupleDescriptor.getId().asList(), "Project");
        addChild(child);
        this.slotMap = slotMap;
        this.commonSlotMap = commonSlotMap;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.PROJECT_NODE;
        msg.project_node = new TProjectNode();
        slotMap.forEach((key, value) -> msg.project_node.putToSlot_map(key.asInt(), value.treeToThrift()));
        commonSlotMap.forEach((key, value) -> msg.project_node.putToCommon_slot_map(key.asInt(), value.treeToThrift()));
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        Preconditions.checkState(conjuncts.isEmpty());
        computeStats(analyzer);
        createDefaultSmap(analyzer);
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        if (detailLevel == TExplainLevel.VERBOSE) {
            output.append(prefix);
            output.append("output columns:\n");
        }

        List<Pair<SlotId, Expr>> outputColumns = new ArrayList<>();
        for (Map.Entry<SlotId, Expr> kv : slotMap.entrySet()) {
            outputColumns.add(new Pair<>(kv.getKey(), kv.getValue()));
        }
        outputColumns.sort(Comparator.comparingInt(o -> o.first.asInt()));

        for (Pair<SlotId, Expr> kv : outputColumns) {
            output.append(prefix);
            if (detailLevel == TExplainLevel.VERBOSE) {
                output.append(kv.first).append(" <-> ")
                        .append(kv.second.explain()).append("\n");
            } else {
                output.append("<slot ").
                        append(kv.first).
                        append("> : ").
                        append(kv.second.toSql()).
                        append("\n");
            }
        }
        if (!commonSlotMap.isEmpty()) {
            output.append(prefix);
            output.append("common expressions:\n");
            for (Map.Entry<SlotId, Expr> kv : commonSlotMap.entrySet()) {
                output.append(prefix);
                if (detailLevel == TExplainLevel.VERBOSE) {
                    output.append(kv.getKey()).append(" <-> ").append(kv.getValue().explain()).append("\n");
                } else {
                    output.append("<slot ").
                            append(kv.getKey()).
                            append("> : ").
                            append(kv.getValue().toSql()).
                            append("\n");
                }
            }
        }

        return output.toString();
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    @Override
    public Optional<List<Expr>> candidatesOfSlotExpr(Expr expr) {
        if (!(expr instanceof SlotRef)) {
            return Optional.empty();
        }
        if (!expr.isBoundByTupleIds(getTupleIds())) {
            return Optional.empty();
        }
        List<Expr> newExprs = Lists.newArrayList();
        for (Map.Entry<SlotId, Expr> kv : slotMap.entrySet()) {
            // Replace the probeExpr only when:
            // 1. when probeExpr is slot ref
            // 2. and probe expr slot id == kv.getKey()
            // then replace probeExpr with kv.getValue()
            // and push down kv.getValue()
            if (expr.isBound(kv.getKey())) {
                newExprs.add(kv.getValue());
            }
        }
        return newExprs.size() > 0 ? Optional.of(newExprs) : Optional.empty();
    }

    @Override
    public Optional<List<List<Expr>>> candidatesOfSlotExprs(List<Expr> exprs) {
        if (!exprs.stream().allMatch(expr -> candidatesOfSlotExpr(expr).isPresent())) {
            // NOTE: This is necessary, when expr is partition_by_epxr because
            // partition_by_exprs may exist in JoinNode below the ProjectNode.
            return Optional.of(ImmutableList.of(exprs));
        }
        List<List<Expr>> candidatesOfSlotExprs =
                exprs.stream().map(expr -> candidatesOfSlotExpr(expr).get()).collect(Collectors.toList());
        return Optional.of(candidateOfPartitionByExprs(candidatesOfSlotExprs));
    }

    @Override
    public boolean pushDownRuntimeFilters(DescriptorTable descTbl, RuntimeFilterDescription description,
                                          Expr probeExpr,
                                          List<Expr> partitionByExprs) {
        if (!canPushDownRuntimeFilter()) {
            return false;
        }

        if (!probeExpr.isBoundByTupleIds(getTupleIds())) {
            return false;
        }

        return pushdownRuntimeFilterForChildOrAccept(descTbl, description, probeExpr, candidatesOfSlotExpr(probeExpr),
                partitionByExprs, candidatesOfSlotExprs(partitionByExprs), 0, true);
    }

    // This functions is used by query cache to compute digest of fragments. for examples:
    // Q1: select count(v1) from t0;
    // Q2: select count(v1) from t0 where dt between '2022-01-01' and '2022-01-01';
    // in Q1 PlanFragment is: OlapScanNode(v1)->Aggregation(count(v1))
    // in Q2 PlanFragment is: OlapScanNode(v1, dt)->ProjectNode(v1)->Aggregation(count(v1))
    // Explicitly, Q2 can reuse Q1's result, but presence of ProjectNode in Q2 gives a different
    // digest from Q1, the ProjectNode is trivial, it just extract v1 from outputs of the
    // OlapScanNode, so we can ignore the trivial project when we compute digest of the fragment.
    public boolean isTrivial() {
        return slotMap.values().stream().allMatch(e -> e instanceof SlotRef) && commonSlotMap.isEmpty();
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalProjectNode projectNode = new TNormalProjectNode();
        normalizer.addSlotsUseAggColumns(commonSlotMap);
        normalizer.addSlotsUseAggColumns(slotMap);
        Pair<List<Integer>, List<ByteBuffer>> cseSlotIdsAndExprs = normalizer.normalizeSlotIdsAndExprs(commonSlotMap);
        projectNode.setCse_slot_ids(cseSlotIdsAndExprs.first);
        projectNode.setCse_exprs(cseSlotIdsAndExprs.second);

        Pair<List<Integer>, List<ByteBuffer>> slotIdAndExprs = normalizer.normalizeSlotIdsAndExprs(slotMap);
        projectNode.setSlot_ids(slotIdAndExprs.first);
        projectNode.setExprs(slotIdAndExprs.second);
        planNode.setNode_type(TPlanNodeType.PROJECT_NODE);
        planNode.setProject_node(projectNode);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }

    @Override
    public List<SlotId> getOutputSlotIds(DescriptorTable descriptorTable) {
        return slotMap.keySet().stream().sorted(Comparator.comparing(SlotId::asInt)).collect(Collectors.toList());
    }

    @Override
    public void collectEquivRelation(FragmentNormalizer normalizer) {
        slotMap.forEach((k, v) -> {
            if (v instanceof SlotRef) {
                normalizer.getEquivRelation().union(k, ((SlotRef) v).getSlotId());
            }
        });
    }
}
