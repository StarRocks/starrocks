// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/AggregationNode.java

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
import com.google.common.collect.Lists;
import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TAggregationNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TStreamingPreaggregationMode;

import java.util.List;
import java.util.Optional;

public class AggregationNode extends PlanNode {
    private final AggregateInfo aggInfo;

    // Set to true if this aggregation node needs to run the Finalize step. This
    // node is the root node of a distributed aggregation.
    private boolean needsFinalize;

    // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
    private boolean useStreamingPreagg;

    private String streamingPreaggregationMode = "auto";

    private boolean useSortAgg = false;

    /**
     * Create an agg node that is not an intermediate node.
     * isIntermediate is true if it is a slave node in a 2-part agg plan.
     */
    public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
        super(id, aggInfo.getOutputTupleId().asList(), "AGGREGATE");
        this.aggInfo = aggInfo;
        this.children.add(input);
        this.needsFinalize = true;
        updateplanNodeName();
    }


    // Unsets this node as requiring finalize. Only valid to call this if it is
    // currently marked as needing finalize.
    public void unsetNeedsFinalize() {
        Preconditions.checkState(needsFinalize);
        needsFinalize = false;
        updateplanNodeName();
    }

    /**
     * Sets this node as a preaggregation. Only valid to call this if it is not marked
     * as a preaggregation
     */
    public void setIsPreagg(boolean canUseStreamingPreAgg) {
        useStreamingPreagg = canUseStreamingPreAgg && aggInfo.getGroupingExprs().size() > 0;
    }

    /**
     * Have this node materialize the aggregation's intermediate tuple instead of
     * the output tuple.
     */
    public void setIntermediateTuple() {
        Preconditions.checkState(!tupleIds.isEmpty());
        Preconditions.checkState(tupleIds.get(0).equals(aggInfo.getOutputTupleId()));
        tupleIds.clear();
        tupleIds.add(aggInfo.getIntermediateTupleId());
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
    }

    public void setStreamingPreaggregationMode(String mode) {
        this.streamingPreaggregationMode = mode;
    }

    public void setUseSortAgg(boolean useSortAgg) {
        this.useSortAgg = useSortAgg;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
    }

    private void updateplanNodeName() {
        StringBuilder sb = new StringBuilder();
        sb.append("AGGREGATE");
        sb.append(" (");
        if (aggInfo.isMerge()) {
            sb.append("merge");
        } else {
            sb.append("update");
        }
        if (needsFinalize) {
            sb.append(" finalize");
        } else {
            sb.append(" serialize");
        }
        sb.append(")");
        setPlanNodeName(sb.toString());
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("aggInfo", aggInfo.debugString()).addValue(
                super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.AGGREGATION_NODE;

        List<TExpr> aggregateFunctions = Lists.newArrayList();
        StringBuilder sqlAggFuncBuilder = new StringBuilder();
        // only serialize agg exprs that are being materialized
        for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
            aggregateFunctions.add(e.treeToThrift());
            if (sqlAggFuncBuilder.length() > 0) {
                sqlAggFuncBuilder.append(", ");
            }
            sqlAggFuncBuilder.append(e.toSql());
        }

        msg.agg_node =
                new TAggregationNode(
                        aggregateFunctions,
                        aggInfo.getIntermediateTupleId().asInt(),
                        aggInfo.getOutputTupleId().asInt(), needsFinalize);
        msg.agg_node.setUse_streaming_preaggregation(useStreamingPreagg);
        if (sqlAggFuncBuilder.length() > 0) {
            msg.agg_node.setSql_aggregate_functions(sqlAggFuncBuilder.toString());
        }
        msg.agg_node.setUse_sort_agg(useSortAgg);

        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (groupingExprs != null) {
            msg.agg_node.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
            StringBuilder sqlGroupingKeysBuilder = new StringBuilder();
            for (Expr e : groupingExprs) {
                if (sqlGroupingKeysBuilder.length() > 0) {
                    sqlGroupingKeysBuilder.append(", ");
                }
                sqlGroupingKeysBuilder.append(e.toSql());
            }
            if (sqlGroupingKeysBuilder.length() > 0) {
                msg.agg_node.setSql_grouping_keys(sqlGroupingKeysBuilder.toString());
            }
        }
        msg.agg_node.setHas_outer_join_child(hasNullableGenerateChild);
        if (streamingPreaggregationMode.equalsIgnoreCase("force_streaming")) {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_STREAMING);
        } else if (streamingPreaggregationMode.equalsIgnoreCase("force_preaggregation")) {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_PREAGGREGATION);
        } else {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.AUTO);
        }
        msg.agg_node.setAgg_func_set_version(3);
    }

    protected String getDisplayLabelDetail() {
        if (useStreamingPreagg) {
            return "STREAMING";
        }
        return null;
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        String nameDetail = getDisplayLabelDetail();
        if (nameDetail != null) {
            output.append(detailPrefix).append(nameDetail).append("\n");
        }
        if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
            output.append(detailPrefix).append("output: ").append(
                    getExplainString(aggInfo.getAggregateExprs())).append("\n");
        }
        output.append(detailPrefix).append("group by: ").append(
                getExplainString(aggInfo.getGroupingExprs())).append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getExplainString(conjuncts)).append("\n");
        }
        return output.toString();
    }

    @Override
    protected String getNodeVerboseExplain(String detailPrefix) {
        StringBuilder output = new StringBuilder();
        String nameDetail = getDisplayLabelDetail();
        if (nameDetail != null) {
            output.append(detailPrefix).append(nameDetail).append("\n");
        }
        if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
            output.append(detailPrefix).append("aggregate: ").append(
                    getVerboseExplain(aggInfo.getAggregateExprs())).append("\n");
        }
        if (aggInfo.getGroupingExprs() != null && aggInfo.getGroupingExprs().size() > 0) {
            output.append(detailPrefix).append("group by: ").append(
                    getVerboseExplain(aggInfo.getGroupingExprs())).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getVerboseExplain(conjuncts)).append("\n");
        }
        if (useSortAgg) {
            output.append(detailPrefix).append("sorted streaming: true\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }

    @Override
    public Optional<List<Expr>> candidatesOfSlotExpr(Expr expr) {
        if (!expr.isBoundByTupleIds(getTupleIds())) {
            return Optional.empty();
        }
        if (!(expr instanceof SlotRef)) {
            return Optional.empty();
        }
        List<Expr> newSlotExprs = Lists.newArrayList();
        for (Expr gexpr : aggInfo.getGroupingExprs()) {
            if (!(gexpr instanceof SlotRef)) {
                continue;
            }
            if (((SlotRef)gexpr).getSlotId().asInt() == ((SlotRef)expr).getSlotId().asInt()) {
                newSlotExprs.add(gexpr);
            }
        }
        return newSlotExprs.size() > 0 ? Optional.of(newSlotExprs) : Optional.empty();
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterDescription description, Expr probeExpr, List<Expr> partitionByExprs) {
        if (!canPushDownRuntimeFilter()) {
            return false;
        }

        if (!probeExpr.isBoundByTupleIds(getTupleIds())) {
            return false;
        }

        return pushdownRuntimeFilterForChildOrAccept(description, probeExpr, candidatesOfSlotExpr(probeExpr),
                partitionByExprs, candidatesOfSlotExprs(partitionByExprs), 0, true);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }
}
