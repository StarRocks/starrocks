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
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TAggregationNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TStreamingPreaggregationMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class AggregationNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(AggregationNode.class);
    private final AggregateInfo aggInfo;

    // Set to true if this aggregation node needs to run the Finalize step. This
    // node is the root node of a distributed aggregation.
    private boolean needsFinalize;

    // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
    private boolean useStreamingPreagg;

    private String streamingPreaggregationMode = "auto";

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

    /**
     * Copy c'tor used in clone().
     */
    private AggregationNode(PlanNodeId id, AggregationNode src) {
        super(id, src, "AGGREGATE");
        aggInfo = src.aggInfo;
        needsFinalize = src.needsFinalize;
    }

    public AggregateInfo getAggInfo() {
        return aggInfo;
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
    public void setIsPreagg(PlannerContext ctx_) {
        useStreamingPreagg = aggInfo.getGroupingExprs().size() > 0;
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
        // Assign predicates to the top-most agg in the single-node plan that can evaluate
        // them, as follows: For non-distinct aggs place them in the 1st phase agg node. For
        // distinct aggs place them in the 2nd phase agg node. The conjuncts are
        // transferred to the proper place in the multi-node plan via transferConjuncts().
        if (tupleIds.get(0).equals(aggInfo.getResultTupleId()) && !aggInfo.isMerge()) {
            // Ignore predicates bound by a grouping slot produced by a SlotRef grouping expr.
            // Those predicates are already evaluated below this agg node (e.g., in a scan),
            // because the grouping slot must be in the same equivalence class as another slot
            // below this agg node. We must not ignore other grouping slots in order to retain
            // conjuncts bound by those grouping slots in createEquivConjuncts() (IMPALA-2089).
            // Those conjuncts cannot be redundant because our equivalence classes do not
            // capture dependencies with non-SlotRef exprs.
            // Set<SlotId> groupBySlots = Sets.newHashSet();
            // for (int i = 0; i < aggInfo.getGroupingExprs().size(); ++i) {
            //    if (aggInfo.getGroupingExprs().get(i).unwrapSlotRef(true) == null) continue;
            //    groupBySlots.add(aggInfo.getOutputTupleDesc().getSlots().get(i).getId());
            // }
            // ArrayList<Expr> bindingPredicates =
            //         analyzer.getBoundPredicates(tupleIds.get(0), groupBySlots, true);
            ArrayList<Expr> bindingPredicates = Lists.newArrayList();
            conjuncts.addAll(bindingPredicates);

            // also add remaining unassigned conjuncts_
            assignConjuncts(analyzer);

            // TODO(zc)
            // analyzer.createEquivConjuncts(tupleIds_.get(0), conjuncts_, groupBySlots);
        }
        // TODO(zc)
        // conjuncts_ = orderConjunctsByCost(conjuncts_);

        // Compute the mem layout for both tuples here for simplicity.
        aggInfo.getOutputTupleDesc().computeMemLayout();
        aggInfo.getIntermediateTupleDesc().computeMemLayout();

        // do this at the end so it can take all conjuncts into account
        computeStats(analyzer);

        // don't call createDefaultSMap(), it would point our conjuncts (= Having clause)
        // to our input; our conjuncts don't get substituted because they already
        // refer to our output
        outputSmap = getCombinedChildSmap();
        aggInfo.substitute(outputSmap, analyzer);

        // assert consistent aggregate expr and slot materialization
        // aggInfo.checkConsistency();

        hasNullableGenerateChild = checkHasNullableGenerateChild();
        streamingPreaggregationMode = analyzer.getContext().getSessionVariable().getStreamingPreaggregationMode();
    }

    public void setStreamingPreaggregationMode(String mode) {
        this.streamingPreaggregationMode = mode;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        cardinality = 1;
        // cardinality: product of # of distinct values produced by grouping exprs
        for (Expr groupingExpr : groupingExprs) {
            long numDistinct = groupingExpr.getNumDistinctValues();
            // TODO: remove these before 1.0
            LOG.debug("grouping expr: " + groupingExpr.toSql() + " #distinct=" + Long.toString(
                    numDistinct));
            if (numDistinct == -1) {
                cardinality = -1;
                break;
            }
            // This is prone to overflow, because we keep multiplying cardinalities,
            // even if the grouping exprs are functionally dependent (example:
            // group by the primary key of a table plus a number of other columns from that
            // same table)
            // TODO: try to recognize functional dependencies
            // TODO: as a shortcut, instead of recognizing functional dependencies,
            // limit the contribution of a single table to the number of rows
            // of that table (so that when we're grouping by the primary key col plus
            // some others, the estimate doesn't overshoot dramatically)
            cardinality *= numDistinct;
        }
        // take HAVING predicate into account
        LOG.debug("Agg: cardinality=" + Long.toString(cardinality));
        if (cardinality > 0) {
            cardinality = Math.round((double) cardinality * computeSelectivity());
            LOG.debug("sel=" + Double.toString(computeSelectivity()));
        }
        // if we ended up with an overflow, the estimate is certain to be wrong
        if (cardinality < 0) {
            cardinality = -1;
        }
        LOG.debug("stats Agg: cardinality=" + Long.toString(cardinality));
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
        msg.agg_node.setAgg_func_set_version(2);
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
        return output.toString();
    }

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);

        // we indirectly reference all grouping slots (because we write them)
        // so they're all materialized.
        aggInfo.getRefdSlots(ids);
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }

    public boolean hasOuterJoinChild() {
        return hasNullableGenerateChild;
    }

    public void setHasOuterJoinChild(boolean hasOuterJoinChild) {
        this.hasNullableGenerateChild = hasOuterJoinChild;
    }

    @Override
    public boolean isVectorized() {
        for (PlanNode node : getChildren()) {
            if (!node.isVectorized()) {
                return false;
            }
        }

        for (Expr expr : aggInfo.getAggregateExprs()) {
            if (!expr.isVectorized()) {
                return false;
            }
        }

        for (Expr expr : aggInfo.getGroupingExprs()) {
            if (!expr.isVectorized()) {
                return false;
            }
        }

        for (Expr expr : conjuncts) {
            if (!expr.isVectorized()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterDescription description, Expr probeExpr) {
        if (probeExpr.isBoundByTupleIds(getTupleIds())) {
            if (probeExpr instanceof SlotRef) {
                for (Expr gexpr : aggInfo.getGroupingExprs()) {
                    // push down only when both of them are slot ref and slot id match.
                    if ((gexpr instanceof SlotRef) &&
                            (((SlotRef) gexpr).getSlotId().asInt() == ((SlotRef) probeExpr).getSlotId().asInt())) {
                        gexpr.setUseVectorized(gexpr.isVectorized());
                        if (children.get(0).pushDownRuntimeFilters(description, gexpr)) {
                            return true;
                        }
                    }
                }
            }

            if (description.canProbeUse(this)) {
                // can not push down to children.
                // use runtime filter at this level.
                description.addProbeExpr(id.asInt(), probeExpr);
                probeRuntimeFilters.add(description);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }
}
