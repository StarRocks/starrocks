// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.plan;

import com.google.common.collect.Maps;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.common.IdGenerator;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.Explain;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.thrift.TExplainLevel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecPlan {
    private final ConnectContext connectContext;
    private final List<String> colNames;
    private final List<ScanNode> scanNodes = new ArrayList<>();
    private final List<Expr> outputExprs = new ArrayList<>();
    private final DescriptorTable descTbl = new DescriptorTable();
    private final Map<ColumnRefOperator, Expr> colRefToExpr = new HashMap<>();
    private final ArrayList<PlanFragment> fragments = new ArrayList<>();
    private final Map<String, PlanFragment> cteProduceFragments = Maps.newHashMap();
    private int planCount = 0;

    private final OptExpression physicalPlan;
    private final List<ColumnRefOperator> outputColumns;

    private final IdGenerator<PlanNodeId> nodeIdGenerator = PlanNodeId.createGenerator();
    private final IdGenerator<PlanFragmentId> fragmentIdGenerator = PlanFragmentId.createGenerator();

    public ExecPlan(ConnectContext connectContext, List<String> colNames,
                    OptExpression physicalPlan, List<ColumnRefOperator> outputColumns) {
        this.connectContext = connectContext;
        this.colNames = colNames;
        this.physicalPlan = physicalPlan;
        this.outputColumns = outputColumns;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public List<Expr> getOutputExprs() {
        return outputExprs;
    }

    public ArrayList<PlanFragment> getFragments() {
        return fragments;
    }

    public DescriptorTable getDescTbl() {
        return descTbl;
    }

    public List<String> getColNames() {
        return colNames;
    }

    public PlanNodeId getNextNodeId() {
        return nodeIdGenerator.getNextId();
    }

    public PlanFragmentId getNextFragmentId() {
        return fragmentIdGenerator.getNextId();
    }

    public Map<ColumnRefOperator, Expr> getColRefToExpr() {
        return colRefToExpr;
    }

    public void setPlanCount(int planCount) {
        this.planCount = planCount;
    }

    public int getPlanCount() {
        return planCount;
    }

    public Map<String, PlanFragment> getCteProduceFragments() {
        return cteProduceFragments;
    }

    public OptExpression getPhysicalPlan() {
        return physicalPlan;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return outputColumns;
    }

    public String getExplainString(TExplainLevel level) {
        StringBuilder str = new StringBuilder();
        if (level == null) {
            str.append(Explain.toString(physicalPlan, outputColumns));
        } else {
            if (planCount != 0) {
                str.append("There are ").append(planCount).append(" plans in optimizer search space\n");
            }

            for (int i = 0; i < fragments.size(); ++i) {
                PlanFragment fragment = fragments.get(i);
                if (i > 0) {
                    // a blank line between plan fragments
                    str.append("\n");
                }
                if (level.equals(TExplainLevel.NORMAL)) {
                    str.append("PLAN FRAGMENT ").append(i).append("\n");
                    str.append(fragment.getExplainString(TExplainLevel.NORMAL));
                } else if (level.equals(TExplainLevel.COSTS)) {
                    str.append("PLAN FRAGMENT ").append(i).append("(").append(fragment.getFragmentId()).append(")\n");
                    str.append(fragment.getCostExplain());
                } else {
                    str.append("PLAN FRAGMENT ").append(i).append("(").append(fragment.getFragmentId()).append(")\n");
                    str.append(fragment.getVerboseExplain());
                }
            }
        }
        return str.toString();
    }
}