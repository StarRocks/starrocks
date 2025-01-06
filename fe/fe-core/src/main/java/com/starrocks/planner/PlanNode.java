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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/PlanNode.java

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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleId;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.TreeNode;
import com.starrocks.common.UserException;
import com.starrocks.sql.common.PermutationGenerator;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlan;
import com.starrocks.thrift.TPlanNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Each PlanNode represents a single relational operator
 * and encapsulates the information needed by the planner to
 * make optimization decisions.
 * <p/>
 * finalize(): Computes internal state, such as keys for scan nodes; gets called once on
 * the root of the plan tree before the call to toThrift(). Also finalizes the set
 * of conjuncts, such that each remaining one requires all of its referenced slots to
 * be materialized (ie, can be evaluated by calling GetValue(), rather than being
 * implicitly evaluated as part of a scan key).
 * <p/>
 * conjuncts: Each node has a list of conjuncts that can be executed in the context of
 * this node, ie, they only reference tuples materialized by this node or one of
 * its children (= are bound by tupleIds).
 */
abstract public class PlanNode extends TreeNode<PlanNode> {
    protected String planNodeName;

    protected PlanNodeId id;  // unique w/in plan tree; assigned by planner
    protected PlanFragmentId fragmentId;  // assigned by planner after fragmentation step
    protected long limit; // max. # of rows to be returned; 0: no limit

    // ids materialized by the tree rooted at this node
    protected ArrayList<TupleId> tupleIds;

    // A set of nullable TupleId produced by this node. It is a subset of tupleIds.
    // A tuple is nullable within a particular plan tree if it's the "nullable" side of
    // an outer join, which has nothing to do with the schema.
    protected Set<TupleId> nullableTupleIds = Sets.newHashSet();

    protected List<Expr> conjuncts = Lists.newArrayList();

    // Fragment that this PlanNode is executed in. Valid only after this PlanNode has been
    // assigned to a fragment. Set and maintained by enclosing PlanFragment.
    protected PlanFragment fragment_;

    // estimate of the output cardinality of this node; set in computeStats();
    // invalid: -1
    protected long cardinality;

    // sum of tupleIds' avgSerializedSizes; set in computeStats()
    protected float avgRowSize;

    protected Map<ColumnRefOperator, ColumnStatistic> columnStatistics;

    // For vector query engine
    // case 1: If agg node hash outer join child
    // Vector agg node must handle all agg and group by column by nullable
    // we couldn't do this work in BE for merge phase
    //  
    // case 2: If children has RepeatNode,
    // we should generate nullable columns for group by columns
    protected boolean hasNullableGenerateChild = false;

    protected boolean isColocate = false; // the flag for colocate join

    protected boolean isReplicated = false; // the flag for replication join

    // Runtime filters be consumed by this node.
    protected List<RuntimeFilterDescription> probeRuntimeFilters = Lists.newArrayList();
    protected Set<Integer> localRfWaitingSet = Sets.newHashSet();
    protected ExprSubstitutionMap outputSmap;

    // set if you want to collect execution statistics for this plan node
    protected boolean needCollectExecStats = false;

    protected PlanNode(PlanNodeId id, ArrayList<TupleId> tupleIds, String planNodeName) {
        this.id = id;
        this.limit = -1;
        // make a copy, just to be on the safe side
        this.tupleIds = Lists.newArrayList(tupleIds);
        this.cardinality = -1;
        this.planNodeName = planNodeName;
    }

    protected PlanNode(PlanNodeId id, String planNodeName) {
        this.id = id;
        this.limit = -1;
        this.tupleIds = Lists.newArrayList();
        this.cardinality = -1;
        this.planNodeName = planNodeName;
    }

    /**
     * Copy c'tor. Also passes in new id.
     */
    protected PlanNode(PlanNodeId id, PlanNode node, String planNodeName) {
        this.id = id;
        this.limit = node.limit;
        this.tupleIds = Lists.newArrayList(node.tupleIds);
        this.nullableTupleIds = Sets.newHashSet(node.nullableTupleIds);
        this.conjuncts = Expr.cloneList(node.conjuncts, null);
        this.cardinality = -1;
        this.planNodeName = planNodeName;
    }

    public List<RuntimeFilterDescription> getProbeRuntimeFilters() {
        return probeRuntimeFilters;
    }

    public void setProbeRuntimeFilters(List<RuntimeFilterDescription> runtimeFilters) {
        this.probeRuntimeFilters = runtimeFilters;
    }

    public void clearProbeRuntimeFilters() {
        probeRuntimeFilters.removeIf(RuntimeFilterDescription::isHasRemoteTargets);
    }

    public void fillLocalRfWaitingSet(Set<Integer> runtimeFilterBuildNode) {
        for (RuntimeFilterDescription filter : probeRuntimeFilters) {
            if (runtimeFilterBuildNode.contains(filter.getBuildPlanNodeId())) {
                localRfWaitingSet.add(filter.getBuildPlanNodeId());
            }
        }
    }

    public Set<Integer> getLocalRfWaitingSet() {
        return localRfWaitingSet;
    }

    public void computeTupleIds() {
        Preconditions.checkState(children.isEmpty() || !tupleIds.isEmpty());
    }

    /**
     * Clears tblRefIds_, tupleIds_, and nullableTupleIds_.
     */
    protected void clearTupleIds() {
        tupleIds.clear();
        nullableTupleIds.clear();
    }

    protected void setPlanNodeName(String s) {
        this.planNodeName = s;
    }

    public String getPlanNodeName() {
        return planNodeName;
    }

    public PlanNodeId getId() {
        return id;
    }

    public void setId(PlanNodeId id) {
        Preconditions.checkState(this.id == null);
        this.id = id;
    }

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public void setFragmentId(PlanFragmentId id) {
        fragmentId = id;
    }

    public PlanFragment getFragment() {
        return fragment_;
    }

    public void setFragment(PlanFragment fragment) {
        fragmentId = fragment.getFragmentId();
        fragment_ = fragment;
    }

    public long getLimit() {
        return limit;
    }

    /**
     * Set the limit to the given limit only if the limit hasn't been set, or the new limit
     * is lower.
     */
    public void setLimit(long limit) {
        if (this.limit == -1 || (limit != -1 && this.limit > limit)) {
            this.limit = limit;
        }
    }

    public boolean hasLimit() {
        return limit > -1;
    }

    public long getCardinality() {
        return cardinality;
    }

    public float getAvgRowSize() {
        return avgRowSize;
    }

    public void unsetLimit() {
        limit = -1;
    }

    public ArrayList<TupleId> getTupleIds() {
        Preconditions.checkState(tupleIds != null);
        return tupleIds;
    }

    public Set<TupleId> getNullableTupleIds() {
        Preconditions.checkState(nullableTupleIds != null);
        return nullableTupleIds;
    }

    public List<Expr> getConjuncts() {
        return conjuncts;
    }

    public void addConjuncts(List<Expr> conjuncts) {
        if (conjuncts == null) {
            return;
        }
        this.conjuncts.addAll(conjuncts);
    }

    public boolean isReplicated() {
        return isReplicated;
    }

    public void setReplicated(boolean replicated) {
        isReplicated = replicated;
    }

    /**
     * Call computeMemLayout() for all materialized tuples.
     */
    protected void computeMemLayout(Analyzer analyzer) {
        for (TupleId id : tupleIds) {
            analyzer.getDescTbl().getTupleDesc(id).computeMemLayout();
        }
    }

    public String getExplainString() {
        return getExplainString("", "", TExplainLevel.VERBOSE);
    }

    /**
     * Generate the explain plan tree. The plan will be in the form of:
     * <p/>
     * root
     * |
     * |----child 2
     * |      limit:1
     * |
     * |----child 3
     * |      limit:2
     * |
     * child 1
     * <p/>
     * The root node header line will be prefixed by rootPrefix and the remaining plan
     * output will be prefixed by prefix.
     */
    protected final String getExplainString(String rootPrefix, String prefix, TExplainLevel detailLevel) {
        StringBuilder expBuilder = new StringBuilder();
        String detailPrefix = prefix;
        boolean traverseChildren = children != null
                && children.size() > 0
                && !(this instanceof ExchangeNode);
        // if (children != null && children.size() > 0) {
        if (traverseChildren) {
            detailPrefix += "|  ";
        } else {
            detailPrefix += "   ";
        }

        // Print the current node
        // The plan node header line will be prefixed by rootPrefix and the remaining details
        // will be prefixed by detailPrefix.
        expBuilder.append(rootPrefix + id.asInt() + ":" + planNodeName + "\n");
        expBuilder.append(getNodeExplainString(detailPrefix, detailLevel));
        if (limit != -1) {
            expBuilder.append(detailPrefix + "limit: " + limit + "\n");
        }
        // Output Tuple Ids only when explain plan level is set to verbose
        if (detailLevel.equals(TExplainLevel.VERBOSE)) {
            expBuilder.append(detailPrefix + "tuple ids: ");
            for (TupleId tupleId : tupleIds) {
                String nullIndicator = nullableTupleIds.contains(tupleId) ? "N" : "";
                expBuilder.append(tupleId.asInt() + nullIndicator + " ");
            }
            expBuilder.append("\n");
        }
        // Print the children
        // if (children != null && children.size() > 0) {
        if (traverseChildren) {
            expBuilder.append(detailPrefix + "\n");
            String childHeadlinePrefix = prefix + "|----";
            String childDetailPrefix = prefix + "|    ";
            for (int i = 1; i < children.size(); ++i) {
                PlanNode child = children.get(i);
                expBuilder.append(child.getExplainString(childHeadlinePrefix, childDetailPrefix, detailLevel));
                expBuilder.append(childDetailPrefix + "\n");
            }
            expBuilder.append(children.get(0).getExplainString(prefix, prefix, detailLevel));
        }
        return expBuilder.toString();
    }

    protected final String getVerboseExplain(String rootPrefix, String prefix) {
        StringBuilder expBuilder = new StringBuilder();
        String detailPrefix = prefix;
        boolean traverseChildren = children != null
                && children.size() > 0
                && !(this instanceof ExchangeNode);
        if (traverseChildren) {
            detailPrefix += "|  ";
        } else {
            detailPrefix += "   ";
        }

        // Print the current node
        // The plan node header line will be prefixed by rootPrefix and the remaining details
        // will be prefixed by detailPrefix.
        expBuilder.append(rootPrefix).append(id.asInt()).append(":").append(planNodeName).append("\n");
        expBuilder.append(getNodeVerboseExplain(detailPrefix));
        if (hasNullableGenerateChild) {
            expBuilder.append(detailPrefix).append("hasNullableGenerateChild: ")
                    .append(hasNullableGenerateChild).append("\n");
        }
        if (limit != -1) {
            expBuilder.append(detailPrefix).append("limit: ").append(limit).append("\n");
        }
        expBuilder.append(detailPrefix).append("cardinality: ").append(cardinality).append("\n");
        if (!probeRuntimeFilters.isEmpty()) {
            expBuilder.append(detailPrefix + "probe runtime filters:\n");
            for (RuntimeFilterDescription rf : probeRuntimeFilters) {
                expBuilder.append(detailPrefix + "- " + rf.toExplainString(id.asInt()) + "\n");
            }
        }
        // Print the children
        if (traverseChildren) {
            expBuilder.append(detailPrefix).append("\n");
            String childHeadlinePrefix = prefix + "|----";
            String childDetailPrefix = prefix + "|    ";

            for (int i = 1; i < children.size(); ++i) {
                PlanNode child = children.get(i);
                expBuilder.append(child.getVerboseExplain(childHeadlinePrefix, childDetailPrefix));
                expBuilder.append(childDetailPrefix).append("\n");
            }
            expBuilder.append(children.get(0).getVerboseExplain(prefix, prefix));
        }
        return expBuilder.toString();
    }

    protected final String getCostExplain(String rootPrefix, String prefix) {
        StringBuilder expBuilder = new StringBuilder();
        String detailPrefix = prefix;
        boolean traverseChildren = children != null
                && children.size() > 0
                && !(this instanceof ExchangeNode);
        if (traverseChildren) {
            detailPrefix += "|  ";
        } else {
            detailPrefix += "   ";
        }

        // Print the current node
        // The plan node header line will be prefixed by rootPrefix and the remaining details
        // will be prefixed by detailPrefix.
        expBuilder.append(rootPrefix).append(id.asInt()).append(":").append(planNodeName).append("\n");
        expBuilder.append(getNodeVerboseExplain(detailPrefix));
        if (hasNullableGenerateChild) {
            expBuilder.append(detailPrefix).append("hasNullableGenerateChild: ")
                    .append(hasNullableGenerateChild).append("\n");
        }
        if (limit != -1) {
            expBuilder.append(detailPrefix).append("limit: ").append(limit).append("\n");
        }
        expBuilder.append(detailPrefix).append("cardinality: ").append(cardinality).append("\n");
        if (!probeRuntimeFilters.isEmpty()) {
            expBuilder.append(detailPrefix + "probe runtime filters:\n");
            for (RuntimeFilterDescription rf : probeRuntimeFilters) {
                expBuilder.append(detailPrefix + "- " + rf.toExplainString(id.asInt()) + "\n");
            }
        }
        if (!planNodeName.equals("EXCHANGE")) {
            expBuilder.append(detailPrefix).append("column statistics: \n").append(getColumnStatistics(detailPrefix));
        }
        // Print the children
        if (traverseChildren) {
            expBuilder.append(detailPrefix).append("\n");
            String childHeadlinePrefix = prefix + "|----";
            String childDetailPrefix = prefix + "|    ";
            for (int i = 1; i < children.size(); ++i) {
                expBuilder.append(
                        children.get(i).getCostExplain(childHeadlinePrefix, childDetailPrefix));
                expBuilder.append(childDetailPrefix).append("\n");
            }
            expBuilder.append(children.get(0).getCostExplain(prefix, prefix));
        }
        return expBuilder.toString();
    }

    protected String getColumnStatistics(String prefix) {
        if (MapUtils.isEmpty(columnStatistics)) {
            return "";
        }
        StringBuilder outputBuilder = new StringBuilder();
        TreeMap<ColumnRefOperator, ColumnStatistic> sortMap =
                new TreeMap<>(Comparator.comparingInt(ColumnRefOperator::getId));
        sortMap.putAll(columnStatistics);
        sortMap.forEach((key, value) -> {
            outputBuilder.append(prefix).append("* ").append(key.getName());
            outputBuilder.append("-->").append(value).append("\n");
        });
        return outputBuilder.toString();
    }

    /**
     * Return the node-specific details.
     * Subclass should override this function.
     * Each line should be prefix by detailPrefix.
     */
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return "";
    }

    private String getNodeVerboseExplain(String prefix) {
        return getNodeExplainString(prefix, TExplainLevel.VERBOSE);
    }

    // Convert this plan node, including all children, to its Thrift representation.
    public TPlan treeToThrift() {
        TPlan result = new TPlan();
        treeToThriftHelper(result);
        return result;
    }

    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
    }

    // Append a flattened version of this plan node, including all children, to 'container'.
    private void treeToThriftHelper(TPlan container) {
        TPlanNode msg = new TPlanNode();
        msg.node_id = id.asInt();
        msg.num_children = children.size();
        msg.limit = limit;
        for (TupleId tid : tupleIds) {
            msg.addToRow_tuples(tid.asInt());
            msg.addToNullable_tuples(nullableTupleIds.contains(tid));
        }
        for (Expr e : conjuncts) {
            msg.addToConjuncts(e.treeToThrift());
        }
        toThrift(msg);
        container.addToNodes(msg);
        if (this instanceof ExchangeNode) {
            msg.num_children = 0;
        } else {
            msg.num_children = children.size();
            for (PlanNode child : children) {
                child.treeToThriftHelper(container);
            }
        }
        if (!probeRuntimeFilters.isEmpty()) {
            msg.setProbe_runtime_filters(
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(probeRuntimeFilters));
        }
        msg.setLocal_rf_waiting_set(getLocalRfWaitingSet());
        msg.setNeed_create_tuple_columns(false);
    }

    /**
     * Computes internal state, including planner-relevant statistics.
     * Call this once on the root of the plan tree before calling toThrift().
     * Subclasses need to override this.
     */
    public void finalizeStats(Analyzer analyzer) throws UserException {
        for (PlanNode child : children) {
            child.finalizeStats(analyzer);
        }
        computeStats(analyzer);
    }

    /**
     * Computes planner statistics: avgRowSize, cardinality.
     * Subclasses need to override this.
     * Assumes that it has already been called on all children.
     * This is broken out of finalize() so that it can be called separately
     * from finalize() (to facilitate inserting additional nodes during plan
     * partitioning w/o the need to call finalize() recursively on the whole tree again).
     */
    protected void computeStats(Analyzer analyzer) {
        avgRowSize = 0.0F;
        for (TupleId tid : tupleIds) {
            avgRowSize += 4;
        }
    }

    public void computeStatistics(Statistics statistics) {
        if (null == statistics) {
            return;
        }
        cardinality = Math.round(statistics.getOutputRowCount());
        avgRowSize = (float) statistics.getColumnStatistics().values().stream().
                mapToDouble(columnStatistic -> columnStatistic.getAverageRowSize()).sum();
        columnStatistics = statistics.getColumnStatistics();
    }

    public ExprSubstitutionMap getOutputSmap() {
        return outputSmap;
    }

    public void init(Analyzer analyzer) throws UserException {
    }

    /**
     * Assign remaining unassigned conjuncts.
     */
    protected void assignConjuncts(Analyzer analyzer) {
        List<Expr> unassigned = analyzer.getUnassignedConjuncts(this.getTupleIds());
        conjuncts.addAll(unassigned);
        analyzer.markConjunctsAssigned(unassigned);
    }

    /**
     * Returns an smap that combines the childrens' smaps.
     */
    protected ExprSubstitutionMap getCombinedChildSmap() {
        if (getChildren().size() == 0) {
            return new ExprSubstitutionMap();
        }

        if (getChildren().size() == 1) {
            return getChild(0).getOutputSmap();
        }

        ExprSubstitutionMap result = ExprSubstitutionMap.combine(
                getChild(0).getOutputSmap(), getChild(1).getOutputSmap());

        for (int i = 2; i < getChildren().size(); ++i) {
            result = ExprSubstitutionMap.combine(result, getChild(i).getOutputSmap());
        }

        return result;
    }

    /**
     * Sets outputSmap_ to compose(existing smap, combined child smap). Also
     * substitutes conjuncts_ using the combined child smap.
     *
     * @throws AnalysisException
     */
    protected void createDefaultSmap(Analyzer analyzer) throws UserException {
        ExprSubstitutionMap combinedChildSmap = getCombinedChildSmap();
        outputSmap =
                ExprSubstitutionMap.compose(outputSmap, combinedChildSmap, analyzer);

        conjuncts = Expr.substituteList(conjuncts, outputSmap, analyzer, false);
    }

    public void setHasNullableGenerateChild() {
        this.hasNullableGenerateChild = checkHasNullableGenerateChild();
    }

    public boolean isHasNullableGenerateChild() {
        return hasNullableGenerateChild;
    }

    protected boolean checkHasNullableGenerateChild() {
        List<RepeatNode> repeatNodes = Lists.newArrayList();
        collectAll(Predicates.instanceOf(RepeatNode.class), repeatNodes);
        if (repeatNodes.size() > 0) {
            return true;
        }

        List<JoinNode> joinNodes = Lists.newArrayList();
        collectAll(Predicates.instanceOf(JoinNode.class), joinNodes);
        for (JoinNode node : joinNodes) {
            if (node.getJoinOp().isOuterJoin()) {
                return true;
            }
        }
        return false;
    }

    // Convert this plan node into msg (excluding children), which requires setting
    // the node type and the node-specific field.
    protected abstract void toThrift(TPlanNode msg);

    protected String debugString() {
        // not using Objects.toStrHelper because
        String output = "preds=" + Expr.debugString(conjuncts) +
                " limit=" + limit;
        return output;
    }

    protected String getVerboseExplain(List<? extends Expr> exprs, TExplainLevel level) {
        if (exprs == null) {
            return "";
        }
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < exprs.size(); ++i) {
            if (i > 0) {
                output.append(", ");
            }
            if (level.equals(TExplainLevel.NORMAL)) {
                output.append(exprs.get(i).toSql());
            } else {
                output.append(exprs.get(i).explain());
            }
        }
        return output.toString();
    }

    protected String getExplainString(List<? extends Expr> exprs) {
        return getVerboseExplain(exprs, TExplainLevel.NORMAL);
    }

    protected String getVerboseExplain(List<? extends Expr> exprs) {
        return getVerboseExplain(exprs, TExplainLevel.VERBOSE);
    }

    public void appendTrace(StringBuilder sb) {
        sb.append(planNodeName);
        if (!children.isEmpty()) {
            sb.append("(");
            int idx = 0;
            for (PlanNode child : children) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                child.appendTrace(sb);
            }
            sb.append(")");
        }
    }

    public boolean isColocate() {
        return isColocate;
    }

    public void setColocate(boolean colocate) {
        isColocate = colocate;
    }

    public boolean canUsePipeLine() {
        return true;
    }

    public boolean canUseRuntimeAdaptiveDop() {
        return false;
    }

    public boolean canPushDownRuntimeFilter() {
        // RuntimeFilter can only be pushed into multicast fragment iff.
        // this runtime filter is applied to all consumers. It's quite hard to do
        // thorough analysis, so we disable it for safety.
        return !(fragment_ instanceof MultiCastPlanFragment);
    }

    public void checkRuntimeFilterOnNullValue(RuntimeFilterDescription description, Expr probeExpr) {
    }

    /**
     * Return candidate slot exprs which is same to the expr, eg: tb1 join tb2 on tb1.a = tb2.b, when expr param is tb1.a,
     * tb2.b is the candidate slot expr for tb1.a which has the same syntax for the query.
     *
     * @param expr: the slot expr that need to find its candidate slot exprs.
     * @return List<Expr>: all the slot expr's candidate slot exprs.
     */
    public Optional<List<Expr>> candidatesOfSlotExpr(Expr expr, Function<Expr, Boolean> couldBound) {
        // NOTE: No need to check expr is slot or not here, each node should implement its `candidatesOfSlotExpr` itself.
        if (!couldBound.apply(expr)) {
            return Optional.empty();
        }
        return Optional.of(Lists.newArrayList(expr));
    }

    public Optional<List<List<Expr>>> candidatesOfSlotExprs(List<Expr> exprs, Function<Expr, Boolean> couldBound) {
        if (!exprs.stream().allMatch(expr -> candidatesOfSlotExpr(expr, couldBound).isPresent())) {
            return Optional.empty();
        }
        List<List<Expr>> candidatesOfSlotExprs =
                exprs.stream().map(expr -> candidatesOfSlotExpr(expr, couldBound).get()).collect(Collectors.toList());
        return Optional.of(candidateOfPartitionByExprs(candidatesOfSlotExprs));
    }

    public static List<List<Expr>> candidateOfPartitionByExprs(List<List<Expr>> partitionByExprs) {
        if (partitionByExprs.isEmpty()) {
            return Lists.newArrayList();
        }
        PermutationGenerator generator = new PermutationGenerator<Expr>(partitionByExprs);
        int totalCount = 0;
        List<List<Expr>> candidates = Lists.newArrayList();
        while (generator.hasNext() && totalCount < 8) {
            candidates.add(generator.next());
            totalCount++;
        }
        return candidates;
    }

    public Optional<List<List<Expr>>> canPushDownRuntimeFilterCrossExchange(List<Expr> partitionByExprs) {
        if (CollectionUtils.isEmpty(partitionByExprs)) {
            return Optional.of(Lists.newArrayList());
        }

        // rf be crossed exchange when partitionByExprs are slot refs and bound by the plan node.
        return candidatesOfSlotExprs(partitionByExprs, couldBoundForPartitionExpr());
    }

    /**
     * When push down runtime filter cross exchange, need take care partitionByExprs of exchange.
     */
    public boolean pushDownRuntimeFilters(RuntimeFilterPushDownContext context, Expr probeExpr,
                                          List<Expr> partitionByExprs) {
        RuntimeFilterDescription description = context.getDescription();
        DescriptorTable descTbl = context.getDescTbl();
        if (!canPushDownRuntimeFilter()) {
            return false;
        }

        Optional<List<List<Expr>>> optCandidatePartitionByExprs =
                canPushDownRuntimeFilterCrossExchange(partitionByExprs);
        if (!optCandidatePartitionByExprs.isPresent()) {
            return false;
        }
        List<List<Expr>> candidatePartitionByExprs = optCandidatePartitionByExprs.get();

        // theoretically runtime filter can be applied on multiple child nodes.
        boolean accept = false;
        for (PlanNode node : children) {
            if (candidatePartitionByExprs.isEmpty()) {
                if (node.pushDownRuntimeFilters(context, probeExpr, Lists.newArrayList())) {
                    accept = true;
                    break;
                }
            } else {
                for (List<Expr> candidateOfPartitionByExprs : candidatePartitionByExprs) {
                    if (node.pushDownRuntimeFilters(context, probeExpr, candidateOfPartitionByExprs)) {
                        accept = true;
                        break;
                    }
                }
            }
        }

        boolean isBound = couldBound(probeExpr, description, descTbl);
        if (isBound) {
            checkRuntimeFilterOnNullValue(description, probeExpr);
        }
        if (accept) {
            return true;
        }
        if (isBound && description.canProbeUse(this, context)) {
            description.addProbeExpr(id.asInt(), probeExpr);
            description.addPartitionByExprsIfNeeded(id.asInt(), probeExpr, partitionByExprs);
            probeRuntimeFilters.add(description);
            return true;
        }
        return false;
    }

    protected Function<Expr, Boolean> couldBound(RuntimeFilterDescription rfDesc, DescriptorTable descTbl) {
        return (Expr expr) -> couldBound(expr, rfDesc, descTbl);
    }

    protected Function<Expr, Boolean> couldBoundForPartitionExpr() {
        return (Expr expr) -> expr.isBoundByTupleIds(getTupleIds());
    }

    private RoaringBitmap cachedSlotIds = null;

    public RoaringBitmap getSlotIds(DescriptorTable descTbl) {
        if (cachedSlotIds == null) {
            cachedSlotIds = new RoaringBitmap();
            getTupleIds().stream().map(descTbl::getTupleDesc)
                    .flatMap(tupleDesc -> tupleDesc.getSlots().stream().map(SlotDescriptor::getId))
                    .map(SlotId::asInt).forEach(cachedSlotIds::add);
        }
        return cachedSlotIds;
    }

    protected boolean couldBound(Expr probeExpr, RuntimeFilterDescription rfDesc, DescriptorTable descTbl) {
        if (probeExpr instanceof SlotRef &&
                rfDesc.runtimeFilterType().equals(RuntimeFilterDescription.RuntimeFilterType.TOPN_FILTER)) {
            SlotRef slotRef = (SlotRef) probeExpr;
            for (TupleId tupleId : getTupleIds()) {
                for (SlotDescriptor slot : descTbl.getTupleDesc(tupleId).getSlots()) {
                    if (!slot.getId().equals(slotRef.getSlotId())) {
                        continue;
                    }
                    return true;
                }
            }
            return false;
        } else {
            return getSlotIds(descTbl).contains(probeExpr.getUsedSlotIds());
        }
    }

    protected boolean canEliminateNull(Expr expr, SlotDescriptor slot) {
        if (expr.isBound(slot.getId())) {
            ScalarOperator operator = SqlToScalarOperatorTranslator.translate(expr);
            ColumnRefOperator column = new ColumnRefOperator(slot.getId().asInt(), slot.getType(),
                    "any", true);
            return Utils.canEliminateNull(Sets.newHashSet(column), operator);
        }
        return false;
    }

    protected boolean canEliminateNull(SlotDescriptor slot) {
        return conjuncts.stream().anyMatch(expr -> canEliminateNull(expr, slot));
    }

    private boolean tryPushdownRuntimeFilterToChild(RuntimeFilterPushDownContext context,
                                                    Optional<List<Expr>> optProbeExprCandidates,
                                                    Optional<List<List<Expr>>> optPartitionByExprsCandidates,
                                                    int childIdx) {
        if (!optProbeExprCandidates.isPresent() || !optPartitionByExprsCandidates.isPresent()) {
            return false;
        }
        List<Expr> probeExprCandidates = optProbeExprCandidates.get();
        List<List<Expr>> partitionByExprsCandidates = optPartitionByExprsCandidates.get();

        for (Expr candidateOfProbeExpr : probeExprCandidates) {
            if (partitionByExprsCandidates.isEmpty()) {
                if (children.get(childIdx).pushDownRuntimeFilters(context, candidateOfProbeExpr,
                        Lists.newArrayList())) {
                    return true;
                }
            } else {
                for (List<Expr> candidateOfPartitionByExprs : partitionByExprsCandidates) {
                    if (children.get(childIdx)
                            .pushDownRuntimeFilters(context, candidateOfProbeExpr,
                                    candidateOfPartitionByExprs)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Push down a runtime filter for the specific child with childIdx. `addProbeInfo` indicates whether
     * add runtime filter info into this PlanNode.
     */
    protected boolean pushdownRuntimeFilterForChildOrAccept(RuntimeFilterPushDownContext context,
                                                            Expr probeExpr,
                                                            Optional<List<Expr>> optProbeExprCandidates,
                                                            List<Expr> partitionByExprs,
                                                            Optional<List<List<Expr>>> optPartitionByExprsCandidates,
                                                            int childIdx,
                                                            boolean addProbeInfo) {
        RuntimeFilterDescription description = context.getDescription();
        DescriptorTable descTbl = context.getDescTbl();
        boolean accept = tryPushdownRuntimeFilterToChild(context, optProbeExprCandidates,
                optPartitionByExprsCandidates, childIdx);
        RoaringBitmap slotIds = getSlotIds(descTbl);
        boolean isBound = slotIds.contains(probeExpr.getUsedSlotIds()) &&
                partitionByExprs.stream().allMatch(expr -> slotIds.contains(expr.getUsedSlotIds()));
        if (isBound) {
            checkRuntimeFilterOnNullValue(description, probeExpr);
        }
        if (accept) {
            return true;
        }
        if (isBound && addProbeInfo && description.canProbeUse(this, context)) {
            // can not push down to children.
            // use runtime filter at this level.
            description.addProbeExpr(id.asInt(), probeExpr);
            description.addPartitionByExprsIfNeeded(id.asInt(), probeExpr, partitionByExprs);
            probeRuntimeFilters.add(description);
            return true;
        }
        return false;
    }

    public boolean canDoReplicatedJoin() {
        boolean canDoReplicatedJoin = false;
        for (PlanNode childNode : children) {
            canDoReplicatedJoin |= childNode.canDoReplicatedJoin();
        }
        return canDoReplicatedJoin;
    }

    public boolean extractConjunctsToNormalize(FragmentNormalizer normalizer) {
        List<Expr> conjuncts = normalizer.getConjunctsByPlanNodeId(this);
        normalizer.filterOutPartColRangePredicates(getId(), conjuncts, Collections.emptySet());
        return true;
    }

    public void normalizeConjuncts(FragmentNormalizer normalizer, TNormalPlanNode planNode, List<Expr> conjuncts) {
        final DescriptorTable descriptorTable = normalizer.getExecPlan().getDescTbl();
        List<SlotId> slotIds = tupleIds.stream().map(descriptorTable::getTupleDesc)
                .flatMap(tupleDesc -> tupleDesc.getSlots().stream().map(SlotDescriptor::getId))
                .collect(Collectors.toList());
        normalizer.remapSlotIds(slotIds);
        planNode.setConjuncts(normalizer.normalizeExprs(normalizer.getConjunctsByPlanNodeId(this)));
    }

    public TNormalPlanNode normalize(FragmentNormalizer normalizer) {
        TNormalPlanNode planNode = new TNormalPlanNode();
        planNode.setNode_id(normalizer.remapPlanNodeId(this.id).asInt());
        planNode.setNum_children(this.getChildren().size());
        planNode.setLimit(this.getLimit());
        planNode.setRow_tuples(normalizer.remapTupleIds(tupleIds));

        List<Boolean> nullable_tuples = tupleIds.stream().map(id -> this.nullableTupleIds.contains(id))
                .collect(Collectors.toList());
        planNode.setNullable_tuples(nullable_tuples);
        toNormalForm(planNode, normalizer);
        normalizer.disableMultiversionIfExprsUseAggColumns(conjuncts);

        return planNode;
    }

    public List<SlotId> getOutputSlotIds(DescriptorTable descriptorTable) {
        return descriptorTable.getTupleDesc(getTupleIds().get(0)).getSlots()
                .stream().map(SlotDescriptor::getId).collect(Collectors.toList());
    }

    // Used to collect equivalence relations produced by PlanNodes. there are
    // three cases:
    // 1. ProjectNode: slotId to slotId mapping in slotMap;
    // 2. SetOperation: input slotId and its corresponding input slotIds of the child PlanNode;
    // 3. HashJoinNode: slotIds of both sides of Join equal conditions in semi join and inner join.
    public void collectEquivRelation(FragmentNormalizer normalizer) {
    }

    // disable optimize depends on physical order
    // eg: sortedStreamingAGG/ PerBucketCompute
    public void disablePhysicalPropertyOptimize() {
    }

    public void forceCollectExecStats() {
        this.needCollectExecStats = true;
    }

    public boolean needCollectExecStats() {
        return needCollectExecStats;
    }
}
