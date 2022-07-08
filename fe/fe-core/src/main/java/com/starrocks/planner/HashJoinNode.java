// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/HashJoinNode.java

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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.ColumnStats;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TEqJoinCondition;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THashJoinNode;
import com.starrocks.thrift.TJoinDistributionMode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class HashJoinNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(HashJoinNode.class);

    private final TableRef innerRef;
    private final JoinOperator joinOp;
    // predicates of the form 'a=b' or 'a<=>b'
    private List<BinaryPredicate> eqJoinConjuncts = Lists.newArrayList();
    // join conjuncts from the JOIN clause that aren't equi-join predicates
    private List<Expr> otherJoinConjuncts;
    private boolean isPushDown;
    private DistributionMode distrMode;
    private String colocateReason = ""; // if can not do colocate join, set reason here
    // the flag for local bucket shuffle join
    private boolean isLocalHashBucket = false;
    // the flag for runtime bucket shuffle join
    private boolean isShuffleHashBucket = false;

    private final List<RuntimeFilterDescription> buildRuntimeFilters = Lists.newArrayList();
    private final List<Integer> filter_null_value_columns = Lists.newArrayList();
    private List<Expr> partitionExprs;
    private List<Integer> outputSlots;

    public List<RuntimeFilterDescription> getBuildRuntimeFilters() {
        return buildRuntimeFilters;
    }

    public void clearBuildRuntimeFilters() {
        buildRuntimeFilters.removeIf(RuntimeFilterDescription::isHasRemoteTargets);
    }

    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                        List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super(id, "HASH JOIN");
        Preconditions.checkArgument(eqJoinConjuncts != null && !eqJoinConjuncts.isEmpty());
        Preconditions.checkArgument(otherJoinConjuncts != null);
        tupleIds.addAll(outer.getTupleIds());
        tupleIds.addAll(inner.getTupleIds());
        this.innerRef = innerRef;
        this.joinOp = innerRef.getJoinOp();
        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            Preconditions.checkArgument(eqJoinPredicate instanceof BinaryPredicate);
            this.eqJoinConjuncts.add((BinaryPredicate) eqJoinPredicate);
        }
        this.distrMode = DistributionMode.NONE;
        this.otherJoinConjuncts = otherJoinConjuncts;
        children.add(outer);
        children.add(inner);
        this.isPushDown = false;

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
        }
    }

    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
                        List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super(id, "HASH JOIN");
        Preconditions.checkArgument(eqJoinConjuncts != null && !eqJoinConjuncts.isEmpty());
        Preconditions.checkArgument(otherJoinConjuncts != null);
        tupleIds.addAll(outer.getTupleIds());
        tupleIds.addAll(inner.getTupleIds());

        innerRef = null;
        this.joinOp = joinOp;
        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            Preconditions.checkArgument(eqJoinPredicate instanceof BinaryPredicate);
            this.eqJoinConjuncts.add((BinaryPredicate) eqJoinPredicate);
        }
        this.distrMode = DistributionMode.NONE;
        this.otherJoinConjuncts = otherJoinConjuncts;
        children.add(outer);
        children.add(inner);
        this.isPushDown = false;

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
        }
    }

    public void buildRuntimeFilters(IdGenerator<RuntimeFilterId> runtimeFilterIdIdGenerator,
                                    PlanNode inner, List<BinaryPredicate> eqJoinConjuncts,
                                    JoinOperator joinOp) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        if (!joinOp.isInnerJoin() && !joinOp.isLeftSemiJoin() && !joinOp.isRightJoin()) {
            return;
        }

        if (distrMode.equals(DistributionMode.PARTITIONED) || distrMode.equals(DistributionMode.LOCAL_HASH_BUCKET)) {
            // if it's partitioned join, and we can not get correct ndv
            // then it's hard to estimate right bloom filter size, or it's too big.
            // so we'd better to skip this global runtime filter.
            long card = inner.getCardinality();
            long buildMax = sessionVariable.getGlobalRuntimeFilterBuildMaxSize();
            if (buildMax > 0 && card <= 0 || card > buildMax) {
                return;
            }
        }

        for (int i = 0; i < eqJoinConjuncts.size(); ++i) {
            BinaryPredicate joinConjunct = eqJoinConjuncts.get(i);
            Preconditions.checkArgument(BinaryPredicate.IS_EQ_NULL_PREDICATE.apply(joinConjunct) ||
                    BinaryPredicate.IS_EQ_PREDICATE.apply(joinConjunct));
            RuntimeFilterDescription rf = new RuntimeFilterDescription(sessionVariable);
            rf.setFilterId(runtimeFilterIdIdGenerator.getNextId().asInt());
            rf.setBuildPlanNodeId(this.id.asInt());
            rf.setExprOrder(i);
            rf.setJoinMode(distrMode);
            rf.setEqualCount(eqJoinConjuncts.size());
            rf.setBuildCardinality(inner.getCardinality());
            rf.setEqualForNull(BinaryPredicate.IS_EQ_NULL_PREDICATE.apply(joinConjunct));

            Expr left = joinConjunct.getChild(0);
            Expr right = joinConjunct.getChild(1);
            ArrayList<TupleId> buildTupleIds = inner.getTupleIds();
            // swap left and right if necessary, and always push down right.
            if (!left.isBoundByTupleIds(buildTupleIds)) {
                Expr temp = left;
                left = right;
                right = temp;
            }

            // push down rf to left child node, and build it only when it
            // can be accepted by left child node.
            rf.setBuildExpr(left);
            boolean accept = getChild(0).pushDownRuntimeFilters(rf, right);
            if (accept) {
                buildRuntimeFilters.add(rf);
            }
        }
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterDescription description, Expr probeExpr) {
        if (!canPushDownRuntimeFilter()) {
            return false;
        }
        if (probeExpr.isBoundByTupleIds(getTupleIds())) {
            boolean hasPushedDown = false;
            // If probeExpr is SlotRef(a), there exits an equalJoinConjunct SlotRef(a)=SlotRef(b) in SemiJoin
            // or InnerJoin, then the rf also can pushed down to both sides of HashJoin because SlotRef(a) and
            // SlotRef(b) are equivalent.
            boolean isInnerOrSemiJoin = joinOp.isSemiJoin() || joinOp.isInnerJoin();
            if ((probeExpr instanceof SlotRef) && isInnerOrSemiJoin) {
                for (BinaryPredicate eqConjunct : eqJoinConjuncts) {
                    Expr lhs = eqConjunct.getChild(0);
                    Expr rhs = eqConjunct.getChild(1);
                    SlotRef eqSlotRef = null;
                    Expr otherExpr = null;
                    if ((lhs instanceof SlotRef) && probeExpr.isBound(((SlotRef) lhs).getSlotId())) {
                        eqSlotRef = (SlotRef) lhs;
                        otherExpr = rhs;
                    } else if ((rhs instanceof SlotRef) && probeExpr.isBound(((SlotRef) rhs).getSlotId())) {
                        eqSlotRef = (SlotRef) rhs;
                        otherExpr = lhs;
                    }
                    if (eqSlotRef == null) {
                        continue;
                    }
                    hasPushedDown |= getChild(0).pushDownRuntimeFilters(description, eqSlotRef);
                    hasPushedDown |= getChild(1).pushDownRuntimeFilters(description, eqSlotRef);
                    if (otherExpr instanceof SlotRef) {
                        hasPushedDown |= getChild(0).pushDownRuntimeFilters(description, otherExpr);
                        hasPushedDown |= getChild(1).pushDownRuntimeFilters(description, otherExpr);
                    }
                    if (hasPushedDown) {
                        break;
                    }
                }
            }
            // fall back to PlanNode.pushDownRuntimeFilters for HJ if rf cannot pushed down via equivalent
            // equalJoinConjuncts
            if (hasPushedDown || super.pushDownRuntimeFilters(description, probeExpr)) {
                return true;
            }

            // can not push down to children.
            // use runtime filter at this level.
            if (description.canProbeUse(this)) {
                description.addProbeExpr(id.asInt(), probeExpr);
                probeRuntimeFilters.add(description);
                return true;
            }
        }
        return false;
    }

    public List<BinaryPredicate> getEqJoinConjuncts() {
        return eqJoinConjuncts;
    }

    public JoinOperator getJoinOp() {
        return joinOp;
    }

    public TableRef getInnerRef() {
        return innerRef;
    }

    public void setDistributionMode(DistributionMode distrMode) {
        this.distrMode = distrMode;
    }

    public DistributionMode getDistributionMode() {
        return this.distrMode;
    }

    public boolean isBroadcast() {
        return this.distrMode == DistributionMode.BROADCAST;
    }

    public boolean isLocalHashBucket() {
        return isLocalHashBucket;
    }

    public void setColocate(boolean colocate, String reason) {
        isColocate = colocate;
        colocateReason = reason;
    }

    public void setLocalHashBucket(boolean localHashBucket) {
        isLocalHashBucket = localHashBucket;
    }

    public void setPartitionExprs(List<Expr> exprs) {
        partitionExprs = exprs;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        assignConjuncts(analyzer);

        // Set smap to the combined children's smaps and apply that to all conjuncts_.
        createDefaultSmap(analyzer);

        computeStats(analyzer);

        ExprSubstitutionMap combinedChildSmap = getCombinedChildWithoutTupleIsNullSmap();
        List<Expr> newEqJoinConjuncts =
                Expr.substituteList(eqJoinConjuncts, combinedChildSmap, analyzer, false);
        eqJoinConjuncts = newEqJoinConjuncts.stream()
                .map(entity -> (BinaryPredicate) entity).collect(Collectors.toList());
        otherJoinConjuncts =
                Expr.substituteList(otherJoinConjuncts, combinedChildSmap, analyzer, false);
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);

        // For a join between child(0) and child(1), we look for join conditions "L.c = R.d"
        // (with L being from child(0) and R from child(1)) and use as the cardinality
        // estimate the maximum of
        //   child(0).cardinality * R.cardinality / # distinct values for R.d
        //     * child(1).cardinality / R.cardinality
        // across all suitable join conditions, which simplifies to
        //   child(0).cardinality * child(1).cardinality / # distinct values for R.d
        // The reasoning is that
        // - each row in child(0) joins with R.cardinality/#DV_R.d rows in R
        // - each row in R is 'present' in child(1).cardinality / R.cardinality rows in
        //   child(1)
        //
        // This handles the very frequent case of a fact table/dimension table join
        // (aka foreign key/primary key join) if the primary key is a single column, with
        // possible additional predicates against the dimension table. An example:
        // FROM FactTbl F JOIN Customers C D ON (F.cust_id = C.id) ... WHERE C.region = 'US'
        // - if there are 5 regions, the selectivity of "C.region = 'US'" would be 0.2
        //   and the output cardinality of the Customers scan would be 0.2 * # rows in
        //   Customers
        // - # rows in Customers == # of distinct values for Customers.id
        // - the output cardinality of the join would be F.cardinality * 0.2

        long maxNumDistinct = 0;
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            Expr lhsJoinExpr = eqJoinPredicate.getChild(0);
            Expr rhsJoinExpr = eqJoinPredicate.getChild(1);
            if (lhsJoinExpr.unwrapSlotRef() == null) {
                continue;
            }
            SlotRef rhsSlotRef = rhsJoinExpr.unwrapSlotRef();
            if (rhsSlotRef == null) {
                continue;
            }
            SlotDescriptor slotDesc = rhsSlotRef.getDesc();
            if (slotDesc == null) {
                continue;
            }
            ColumnStats stats = slotDesc.getStats();
            if (!stats.hasNumDistinctValues()) {
                continue;
            }
            long numDistinct = stats.getNumDistinctValues();
            maxNumDistinct = Math.max(maxNumDistinct, numDistinct);
            LOG.debug("min slotref: {}, #distinct: {}", rhsSlotRef.toSql(), numDistinct);
        }

        if (maxNumDistinct == 0) {
            // if we didn't find any suitable join predicates or don't have stats
            // on the relevant columns, we very optimistically assume we're doing an
            // FK/PK join (which doesn't alter the cardinality of the left-hand side)
            cardinality = getChild(0).cardinality;
        } else {
            cardinality = Math.round((double) getChild(0).cardinality * (double) getChild(
                    1).cardinality / (double) maxNumDistinct);
            if (cardinality < -1) {
                cardinality = Long.MAX_VALUE;
            }
            LOG.debug("lhs card: {}, rhs card: {}", getChild(0).cardinality, getChild(1).cardinality);
        }
        LOG.debug("stats HashJoin: cardinality {}", cardinality);
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("eqJoinConjuncts",
                eqJoinConjunctsDebugString()).addValue(super.debugString()).toString();
    }

    private String eqJoinConjunctsDebugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        for (BinaryPredicate expr : eqJoinConjuncts) {
            helper.add("lhs", expr.getChild(0)).add("rhs", expr.getChild(1));
        }
        return helper.toString();
    }

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);
        // we also need to materialize everything referenced by eqJoinConjuncts
        // and otherJoinConjuncts
        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            eqJoinPredicate.getIds(null, ids);
        }
        for (Expr e : otherJoinConjuncts) {
            e.getIds(null, ids);
        }
    }

    public void setIsPushDown(boolean isPushDown) {
        this.isPushDown = isPushDown;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
        msg.hash_join_node = new THashJoinNode();
        msg.hash_join_node.join_op = joinOp.toThrift();
        msg.hash_join_node.distribution_mode = distrMode.toThrift();
        StringBuilder sqlJoinPredicatesBuilder = new StringBuilder();
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(eqJoinPredicate.getChild(0).treeToThrift(),
                    eqJoinPredicate.getChild(1).treeToThrift());
            eqJoinCondition.setOpcode(eqJoinPredicate.getOp().getOpcode());
            msg.hash_join_node.addToEq_join_conjuncts(eqJoinCondition);
            if (sqlJoinPredicatesBuilder.length() > 0) {
                sqlJoinPredicatesBuilder.append(", ");
            }
            sqlJoinPredicatesBuilder.append(eqJoinPredicate.toSql());
        }
        for (Expr e : otherJoinConjuncts) {
            msg.hash_join_node.addToOther_join_conjuncts(e.treeToThrift());
            if (sqlJoinPredicatesBuilder.length() > 0) {
                sqlJoinPredicatesBuilder.append(", ");
            }
            sqlJoinPredicatesBuilder.append(e.toSql());
        }
        if (sqlJoinPredicatesBuilder.length() > 0) {
            msg.hash_join_node.setSql_join_predicates(sqlJoinPredicatesBuilder.toString());
        }
        if (!conjuncts.isEmpty()) {
            StringBuilder sqlPredicatesBuilder = new StringBuilder();
            for (Expr e : conjuncts) {
                if (sqlPredicatesBuilder.length() > 0) {
                    sqlPredicatesBuilder.append(", ");
                }
                sqlPredicatesBuilder.append(e.toSql());
            }
            if (sqlPredicatesBuilder.length() > 0) {
                msg.hash_join_node.setSql_predicates(sqlPredicatesBuilder.toString());
            }
        }
        msg.hash_join_node.setIs_push_down(isPushDown);
        if (innerRef != null) {
            msg.hash_join_node.setIs_rewritten_from_not_in(innerRef.isJoinRewrittenFromNotIn());
        }
        if (!buildRuntimeFilters.isEmpty()) {
            msg.hash_join_node.setBuild_runtime_filters(
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters));
        }
        msg.hash_join_node.setBuild_runtime_filters_from_planner(
                ConnectContext.get().getSessionVariable().getEnableGlobalRuntimeFilter());
        if (partitionExprs != null) {
            msg.hash_join_node.setPartition_exprs(Expr.treesToThrift(partitionExprs));
        }
        msg.setFilter_null_value_columns(filter_null_value_columns);

        if (outputSlots != null) {
            msg.hash_join_node.setOutput_columns(outputSlots);
        }
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String distrModeStr =
                (distrMode != DistributionMode.NONE) ? (" (" + distrMode.toString() + ")") : "";
        StringBuilder output = new StringBuilder().append(
                detailPrefix + "join op: " + joinOp.toString() + distrModeStr + "\n").append(
                detailPrefix + "hash predicates:\n");

        output.append(detailPrefix).append("colocate: ").append(isColocate)
                .append(isColocate ? "" : ", reason: " + colocateReason).append("\n");

        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            output.append(detailPrefix).append("equal join conjunct: ").append(eqJoinPredicate.toSql() + "\n");
        }
        if (!otherJoinConjuncts.isEmpty()) {
            output.append(detailPrefix + "other join predicates: ").append(
                    getExplainString(otherJoinConjuncts) + "\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix + "other predicates: ").append(
                    getExplainString(conjuncts) + "\n");
        }
        return output.toString();
    }

    @Override
    protected String getNodeVerboseExplain(String detailPrefix) {
        String distrModeStr =
                (distrMode != DistributionMode.NONE) ? (" (" + distrMode.toString() + ")") : "";
        StringBuilder output = new StringBuilder().append(detailPrefix)
                .append("join op: ").append(joinOp.toString()).append(distrModeStr).append("\n");

        if (isColocate) {
            output.append(detailPrefix).append("colocate: ").append(isColocate).append("\n");
        }

        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            output.append(detailPrefix).append("equal join conjunct: ").
                    append(eqJoinPredicate.explain()).append("\n");
        }
        if (!otherJoinConjuncts.isEmpty()) {
            output.append(detailPrefix).append("other join predicates: ").
                    append(getVerboseExplain(otherJoinConjuncts)).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("other predicates: ").
                    append(getVerboseExplain(conjuncts)).append("\n");
        }
        if (!buildRuntimeFilters.isEmpty()) {
            output.append(detailPrefix).append("build runtime filters:\n");
            for (RuntimeFilterDescription rf : buildRuntimeFilters) {
                output.append(detailPrefix).append("- ").append(rf.toExplainString(-1)).append("\n");
            }
        }

        if (outputSlots != null) {
            output.append(detailPrefix).append("output columns: ");
            output.append(outputSlots.stream().map(Object::toString).collect(Collectors.joining(", ")));
            output.append("\n");
        }

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return Math.max(children.get(0).getNumInstances(), children.get(1).getNumInstances());
    }

    public enum DistributionMode {
        NONE("NONE"),
        BROADCAST("BROADCAST"),
        PARTITIONED("PARTITIONED"),
        // The hash algorithms of the two bucket shuffle ways are different
        LOCAL_HASH_BUCKET("BUCKET_SHUFFLE"),
        SHUFFLE_HASH_BUCKET("BUCKET_SHUFFLE(S)"),
        COLOCATE("COLOCATE"),
        REPLICATED("REPLICATED");

        private final String description;

        DistributionMode(String desc) {
            this.description = desc;
        }

        @Override
        public String toString() {
            return description;
        }

        public TJoinDistributionMode toThrift() {
            switch (this) {
                case BROADCAST:
                    return TJoinDistributionMode.BROADCAST;
                case PARTITIONED:
                    return TJoinDistributionMode.PARTITIONED;
                case LOCAL_HASH_BUCKET:
                    return TJoinDistributionMode.LOCAL_HASH_BUCKET;
                case SHUFFLE_HASH_BUCKET:
                    return TJoinDistributionMode.SHUFFLE_HASH_BUCKET;
                case COLOCATE:
                    return TJoinDistributionMode.COLOCATE;
                case REPLICATED:
                    return TJoinDistributionMode.REPLICATED;
                default:
                    return TJoinDistributionMode.NONE;
            }
        }
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public void checkRuntimeFilterOnNullValue(RuntimeFilterDescription description, Expr probeExpr) {
        // note(yan): outer join may generate null values, and if runtime filter does not accept null value
        // we have opportunity to filter those values out.
        boolean slotRefWithNullValue = false;
        SlotId slotId = null;
        if (probeExpr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) probeExpr;
            if (slotRef.isNullable()) {
                slotRefWithNullValue = true;
                slotId = slotRef.getSlotId();
            }
        }

        if (joinOp.isOuterJoin() && !description.getEqualForNull() && slotRefWithNullValue) {
            filter_null_value_columns.add(slotId.asInt());
        }
    }

    public void setOutputSlots(List<Integer> outputSlots) {
        this.outputSlots = outputSlots;
    }
}
