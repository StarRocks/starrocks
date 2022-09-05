// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.Id;
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
import java.util.TreeMap;

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

        List<Pair<SlotId, Expr>> outputColumns = new ArrayList<>();
        for (Map.Entry<SlotId, Expr> kv : slotMap.entrySet()) {
            outputColumns.add(new Pair<>(kv.getKey(), kv.getValue()));
        }
        outputColumns.sort(Comparator.comparingInt(o -> o.first.asInt()));

        for (Pair<SlotId, Expr> kv : outputColumns) {
            output.append(prefix);
            output.append("<slot ").
                    append(kv.first).
                    append("> : ").
                    append(kv.second.toSql()).
                    append("\n");
        }
        if (!commonSlotMap.isEmpty()) {
            output.append(prefix);
            output.append("common expressions:\n");
            for (Map.Entry<SlotId, Expr> kv : commonSlotMap.entrySet()) {
                output.append(prefix);
                output.append("<slot ").
                        append(kv.getKey()).
                        append("> : ").
                        append(kv.getValue().toSql()).
                        append("\n");
            }
        }
        return output.toString();
    }

    @Override
    protected String getNodeVerboseExplain(String prefix) {
        StringBuilder output = new StringBuilder();
        output.append(prefix);
        output.append("output columns:\n");

        List<Pair<SlotId, Expr>> outputColumns = new ArrayList<>();
        for (Map.Entry<SlotId, Expr> kv : slotMap.entrySet()) {
            outputColumns.add(new Pair<>(kv.getKey(), kv.getValue()));
        }
        outputColumns.sort(Comparator.comparingInt(o -> o.first.asInt()));

        for (Pair<SlotId, Expr> kv : outputColumns) {
            output.append(prefix);
            output.append(kv.first).append(" <-> ")
                    .append(kv.second.explain()).append("\n");
        }
        if (!commonSlotMap.isEmpty()) {
            output.append(prefix);
            output.append("common expressions:\n");
            for (Map.Entry<SlotId, Expr> kv : commonSlotMap.entrySet()) {
                output.append(prefix);
                output.append(kv.getKey()).append(" <-> ").append(kv.getValue().explain()).append("\n");
            }
        }
        return output.toString();
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public List<Expr> candidatesOfSlotExpr(Expr expr) {
        List<Expr> newExprs = Lists.newArrayList();
        if (!(expr instanceof SlotRef)) {
            return newExprs;
        }
        if (!expr.isBoundByTupleIds(getTupleIds())) {
            return newExprs;
        }
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
        // NOTE: This is necessary, when expr is partition_by_epxr because
        // partition_by_exprs may exists in JoinNode below the ProjectNode.
        if (newExprs.isEmpty()) {
            newExprs.add(expr);
        }
        return newExprs;
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

        List<Expr> probeExprCandidates = candidatesOfSlotExpr(probeExpr);
        List<List<Expr>> partitionByExprsCandidates = candidatesOfSlotExprs(partitionByExprs);
        return canPushDownRuntimeFilterForChild(description, probeExpr, probeExprCandidates,
                partitionByExprs, partitionByExprsCandidates, 0, true);
    }
}
