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

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.common.DebugOperatorTracer;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.rule.mv.KeyInference;
import com.starrocks.sql.optimizer.rule.mv.MVOperatorProperty;
import com.starrocks.sql.optimizer.rule.mv.ModifyInference;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.List;

/**
 * A expression is an operator with zero or more input expressions.
 * We refer to an expression as logical or physical
 * based on the type of its operator.
 * <p>
 * Logical Expression: (A ⨝ B) ⨝ C
 * Physical Expression: (AF ⨝HJ BF) ⨝NLJ CF
 */
public class OptExpression {

    private Operator op;
    private List<OptExpression> inputs;

    private LogicalProperty property;
    private Statistics statistics;
    private double cost = 0;
    // The number of plans in the entire search space，this parameter is valid only when cbo_use_nth_exec_plan configured.
    // Default value is 0
    private int planCount = 0;

    // For easily convert a GroupExpression to OptExpression when pattern match
    // we just use OptExpression to wrap GroupExpression
    private GroupExpression groupExpression;
    // Required properties for children.
    private List<PhysicalPropertySet> requiredProperties;
    // MV Operator property, inferred from best plan
    private MVOperatorProperty mvOperatorProperty;
    private PhysicalPropertySet outputProperty;

    private Boolean isShortCircuit = false;

    public OptExpression(Operator op) {
        this.op = op;
        this.inputs = Lists.newArrayList();
    }

    public static OptExpression create(Operator op, OptExpression... inputs) {
        OptExpression expr = new OptExpression(op);
        expr.inputs = Lists.newArrayList(inputs);
        return expr;
    }

    public static OptExpression createForShortCircuit(Operator op, OptExpression input, boolean isShortCircuit) {
        OptExpression expr = new OptExpression(op);
        expr.inputs = Lists.newArrayList(input);
        expr.setShortCircuit(isShortCircuit);
        return expr;
    }

    public static OptExpression create(Operator op, List<OptExpression> inputs) {
        OptExpression expr = new OptExpression(op);
        expr.inputs = inputs;
        return expr;
    }

    public OptExpression(GroupExpression groupExpression, List<OptExpression> inputs) {
        this.op = groupExpression.getOp();
        this.inputs = inputs;
        this.groupExpression = groupExpression;
        this.property = groupExpression.getGroup().getLogicalProperty();
    }

    public Operator getOp() {
        return op;
    }

    public List<OptExpression> getInputs() {
        return inputs;
    }

    public int arity() {
        return inputs.size();
    }

    public LogicalProperty getLogicalProperty() {
        return property;
    }

    public void setLogicalProperty(LogicalProperty property) {
        this.property = property;
    }

    public OptExpression inputAt(int i) {
        return inputs.get(i);
    }

    public void setChild(int index, OptExpression child) {
        this.inputs.set(index, child);
    }

    public GroupExpression getGroupExpression() {
        return groupExpression;
    }

    public void attachGroupExpression(GroupExpression groupExpression) {
        this.groupExpression = groupExpression;
    }

    // Note: Required this OptExpression produced by {@Binder}
    public ColumnRefSet getChildOutputColumns(int index) {
        return inputAt(index).getOutputColumns();
    }

    public ColumnRefSet getOutputColumns() {
        Preconditions.checkState(property != null);
        return property.getOutputColumns();
    }

    public RowOutputInfo getRowOutputInfo() {
        return op.getRowOutputInfo(inputs);
    }

    public void initRowOutputInfo() {
        for (OptExpression optExpression : inputs) {
            optExpression.initRowOutputInfo();
        }
        getRowOutputInfo();
    }

    public void setRequiredProperties(List<PhysicalPropertySet> requiredProperties) {
        this.requiredProperties = requiredProperties;
    }

    public List<PhysicalPropertySet> getRequiredProperties() {
        return this.requiredProperties;
    }

    public void setOutputProperty(PhysicalPropertySet requiredProperties) {
        this.outputProperty = requiredProperties;
    }

    public PhysicalPropertySet getOutputProperty() {
        return this.outputProperty;
    }

    // This function assume the child expr logical property has been derived
    public void deriveLogicalPropertyItself() {
        ExpressionContext context = new ExpressionContext(this);
        context.deriveLogicalProperty();
        setLogicalProperty(context.getRootProperty());
    }

    public void deriveMVProperty() {
        KeyInference.KeyPropertySet keyPropertySet = KeyInference.infer(this, null);
        ModifyInference.ModifyOp modifyOp = ModifyInference.infer(this);
        this.mvOperatorProperty = new MVOperatorProperty(keyPropertySet, modifyOp);
    }

    public MVOperatorProperty getMvOperatorProperty() {
        return this.mvOperatorProperty;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    public int getPlanCount() {
        return planCount;
    }

    public void setPlanCount(int planCount) {
        this.planCount = planCount;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public Boolean getShortCircuit() {
        return isShortCircuit;
    }

    public void setShortCircuit(Boolean shortCircuit) {
        isShortCircuit = shortCircuit;
    }

    @Override
    public String toString() {
        return op + " child size " + inputs.size();
    }

    public String debugString() {
        return debugString("", "", Integer.MAX_VALUE);
    }

    public String debugString(int limitLine) {
        return debugString("", "", limitLine);
    }

    private String debugString(String headlinePrefix, String detailPrefix, int limitLine) {
        StringBuilder sb = new StringBuilder();
        sb.append(headlinePrefix).append(op.accept(new DebugOperatorTracer(), null));
        limitLine -= 1;
        if (limitLine <= 0 || inputs.isEmpty()) {
            return sb.toString();
        }

        sb.append('\n');
        String childHeadlinePrefix = detailPrefix + "->  ";
        String childDetailPrefix = detailPrefix + "    ";
        for (OptExpression input : inputs) {
            sb.append(input.debugString(childHeadlinePrefix, childDetailPrefix, limitLine));
        }
        return sb.toString();
    }
}
