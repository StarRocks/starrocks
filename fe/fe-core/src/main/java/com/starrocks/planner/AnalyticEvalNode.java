// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/AnalyticEvalNode.java

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

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TAnalyticNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Computation of analytic exprs.
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class AnalyticEvalNode extends PlanNode {
    private static final Logger LOG = LoggerFactory.getLogger(AnalyticEvalNode.class);

    private List<Expr> analyticFnCalls;

    // Partitioning exprs from the AnalyticInfo
    private final List<Expr> partitionExprs;

    // TODO: Remove when the BE uses partitionByLessThan rather than the exprs
    private List<Expr> substitutedPartitionExprs;
    private List<OrderByElement> orderByElements;

    private final AnalyticWindow analyticWindow;

    // Physical tuples used/produced by this analytic node.
    private final TupleDescriptor intermediateTupleDesc;
    private final TupleDescriptor outputTupleDesc;

    // maps from the logical output slots in logicalTupleDesc_ to their corresponding
    // physical output slots in outputTupleDesc_
    private final ExprSubstitutionMap logicalToPhysicalSmap;

    // predicates constructed from partitionExprs_/orderingExprs_ to
    // compare input to buffered tuples
    private final Expr partitionByEq;
    private final Expr orderByEq;
    private final TupleDescriptor bufferedTupleDesc;

    public AnalyticEvalNode(
            PlanNodeId id, PlanNode input, List<Expr> analyticFnCalls,
            List<Expr> partitionExprs, List<OrderByElement> orderByElements,
            AnalyticWindow analyticWindow, TupleDescriptor intermediateTupleDesc,
            TupleDescriptor outputTupleDesc, ExprSubstitutionMap logicalToPhysicalSmap,
            Expr partitionByEq, Expr orderByEq, TupleDescriptor bufferedTupleDesc) {
        super(id, input.getTupleIds(), "ANALYTIC");
        Preconditions.checkState(!tupleIds.contains(outputTupleDesc.getId()));
        // we're materializing the input row augmented with the analytic output tuple
        tupleIds.add(outputTupleDesc.getId());
        this.analyticFnCalls = analyticFnCalls;
        this.partitionExprs = partitionExprs;
        this.orderByElements = orderByElements;
        this.analyticWindow = analyticWindow;
        this.intermediateTupleDesc = intermediateTupleDesc;
        this.outputTupleDesc = outputTupleDesc;
        this.logicalToPhysicalSmap = logicalToPhysicalSmap;
        this.partitionByEq = partitionByEq;
        this.orderByEq = orderByEq;
        this.bufferedTupleDesc = bufferedTupleDesc;
        children.add(input);
        nullableTupleIds = Sets.newHashSet(input.getNullableTupleIds());
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        analyzer.getDescTbl().computeMemLayout();
        intermediateTupleDesc.computeMemLayout();
        // we add the analyticInfo's smap to the combined smap of our child
        outputSmap = logicalToPhysicalSmap;
        createDefaultSmap(analyzer);

        // Do not assign any conjuncts here: the conjuncts out of our SelectStmt's
        // Where clause have already been assigned, and conjuncts coming out of an
        // enclosing scope need to be evaluated *after* all analytic computations.

        // do this at the end so it can take all conjuncts into account
        computeStats(analyzer);
        if (LOG.isDebugEnabled()) {
            LOG.debug("desctbl: " + analyzer.getDescTbl().debugString());
        }

        // point fn calls, partition and ordering exprs at our input
        ExprSubstitutionMap childSmap = getCombinedChildSmap();
        analyticFnCalls = Expr.substituteList(analyticFnCalls, childSmap, analyzer, false);
        substitutedPartitionExprs = Expr.substituteList(partitionExprs, childSmap,
                analyzer, false);
        orderByElements = OrderByElement.substitute(orderByElements, childSmap, analyzer);
        if (LOG.isDebugEnabled()) {
            LOG.debug("evalnode: " + debugString());
        }
        hasNullableGenerateChild = checkHasNullableGenerateChild();
    }

    @Override
    protected void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        cardinality = getChild(0).cardinality;
    }

    @Override
    protected String debugString() {
        List<String> orderByElementStrs = Lists.newArrayList();

        for (OrderByElement element : orderByElements) {
            orderByElementStrs.add(element.toSql());
        }

        return MoreObjects.toStringHelper(this)
                .add("analyticFnCalls", Expr.debugString(analyticFnCalls))
                .add("partitionExprs", Expr.debugString(partitionExprs))
                .add("subtitutedPartitionExprs", Expr.debugString(substitutedPartitionExprs))
                .add("orderByElements", Joiner.on(", ").join(orderByElementStrs))
                .add("window", analyticWindow)
                .add("intermediateTid", intermediateTupleDesc.getId())
                .add("intermediateTid", outputTupleDesc.getId())
                .add("outputTid", outputTupleDesc.getId())
                .add("partitionByEq",
                        partitionByEq != null ? partitionByEq.debugString() : "null")
                .add("orderByEq",
                        orderByEq != null ? orderByEq.debugString() : "null")
                .addValue(super.debugString())
                .toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ANALYTIC_EVAL_NODE;
        msg.analytic_node = new TAnalyticNode();
        if (intermediateTupleDesc != null) {
            msg.analytic_node.setIntermediate_tuple_id(intermediateTupleDesc.getId().asInt());
        }
        msg.analytic_node.setOutput_tuple_id(outputTupleDesc.getId().asInt());
        msg.analytic_node.setPartition_exprs(Expr.treesToThrift(substitutedPartitionExprs));
        msg.analytic_node.setOrder_by_exprs(
                Expr.treesToThrift(OrderByElement.getOrderByExprs(orderByElements)));
        msg.analytic_node.setAnalytic_functions(Expr.treesToThrift(analyticFnCalls));

        if (analyticWindow == null) {
            if (!orderByElements.isEmpty()) {
                msg.analytic_node.setWindow(AnalyticWindow.DEFAULT_WINDOW.toThrift());
            }
        } else {
            // TODO: Window boundaries should have range_offset_predicate set
            msg.analytic_node.setWindow(analyticWindow.toThrift());
        }

        if (partitionByEq != null) {
            msg.analytic_node.setPartition_by_eq(partitionByEq.treeToThrift());
        }

        if (orderByEq != null) {
            msg.analytic_node.setOrder_by_eq(orderByEq.treeToThrift());
        }

        if (bufferedTupleDesc != null) {
            msg.analytic_node.setBuffered_tuple_id(bufferedTupleDesc.getId().asInt());
        }
        msg.analytic_node.setHas_outer_join_child(hasNullableGenerateChild);
    }

    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("functions: ");
        List<String> strings = Lists.newArrayList();

        for (Expr fnCall : analyticFnCalls) {
            strings.add("[");
            if (detailLevel.equals(TExplainLevel.NORMAL)) {
                strings.add(fnCall.toSql());
            } else {
                strings.add(fnCall.explain());
            }
            strings.add("]");
        }

        output.append(Joiner.on(", ").join(strings));
        output.append("\n");

        if (!partitionExprs.isEmpty()) {
            output.append(prefix).append("partition by: ");
            strings.clear();

            for (Expr partitionExpr : partitionExprs) {
                if (detailLevel.equals(TExplainLevel.NORMAL)) {
                    strings.add(partitionExpr.toSql());
                } else {
                    strings.add(partitionExpr.explain());
                }
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        if (!orderByElements.isEmpty()) {
            output.append(prefix).append("order by: ");
            strings.clear();

            for (OrderByElement element : orderByElements) {
                if (detailLevel.equals(TExplainLevel.NORMAL)) {
                    strings.add(element.toSql());
                } else {
                    strings.add(element.explain());
                }
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        if (analyticWindow != null) {
            output.append(prefix).append("window: ");
            output.append(analyticWindow.toSql());
            output.append("\n");
        }

        return output.toString();
    }

    @Override
    public boolean isVectorized() {
        for (PlanNode node : getChildren()) {
            if (!node.isVectorized()) {
                return false;
            }
        }

        for (Expr expr : substitutedPartitionExprs) {
            if (!expr.isVectorized()) {
                return false;
            }
        }

        for (Expr expr : analyticFnCalls) {
            if (!expr.isVectorized()) {
                return false;
            }
        }

        List<Expr> orderExprs = OrderByElement.getOrderByExprs(orderByElements);
        for (Expr expr : orderExprs) {
            if (!expr.isVectorized()) {
                return false;
            }
        }

        return true;
    }

    public void setSubstitutedPartitionExprs(List<Expr> substitutedPartitionExprs) {
        this.substitutedPartitionExprs = substitutedPartitionExprs;
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }
}
