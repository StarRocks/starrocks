// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TProjectNode;

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
    public boolean pushDownRuntimeFilters(RuntimeFilterDescription description,
                                          Expr probeExpr,
                                          List<Expr> partitionByExprs) {
        if (!canPushDownRuntimeFilter()) {
            return false;
        }

        if (!probeExpr.isBoundByTupleIds(getTupleIds())) {
            return false;
        }

        return pushdownRuntimeFilterForChildOrAccept(description, probeExpr, candidatesOfSlotExpr(probeExpr),
                partitionByExprs, candidatesOfSlotExprs(partitionByExprs), 0, true);
    }
}
