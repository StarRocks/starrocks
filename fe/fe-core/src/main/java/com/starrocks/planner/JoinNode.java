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
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TupleId;
import com.starrocks.common.FeConstants;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TJoinDistributionMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 */
public abstract class JoinNode extends PlanNode implements RuntimeFilterBuildNode {
    private static final Logger LOG = LogManager.getLogger(JoinNode.class);

    protected final TableRef innerRef;
    protected final JoinOperator joinOp;
    // predicates of the form 'a=b' or 'a<=>b'
    protected List<BinaryPredicate> eqJoinConjuncts = Lists.newArrayList();
    // join conjuncts from the JOIN clause that aren't equi-join predicates
    protected List<Expr> otherJoinConjuncts;
    protected boolean isPushDown;
    protected DistributionMode distrMode;
    protected String colocateReason = ""; // if can not do colocate join, set reason here
    // the flag for local bucket shuffle join
    protected boolean isLocalHashBucket = false;
    protected boolean isFKRight = false;

    protected final List<RuntimeFilterDescription> buildRuntimeFilters = Lists.newArrayList();
    protected final List<Integer> filter_null_value_columns = Lists.newArrayList();
    protected List<Expr> partitionExprs;

    // contains both the cols required by parent node and cols required by
    // other join conjuncts and predicates
    protected List<Integer> outputSlots;

    // The partitionByExprs which need to check the probe side for partition join.
    protected List<Expr> probePartitionByExprs;
    protected boolean canLocalShuffle = false;

    public List<RuntimeFilterDescription> getBuildRuntimeFilters() {
        return buildRuntimeFilters;
    }

    public void clearBuildRuntimeFilters() {
        buildRuntimeFilters.removeIf(RuntimeFilterDescription::isHasRemoteTargets);
    }

    public JoinNode(String planNodename, PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                    List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super(id, planNodename);
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

    public JoinNode(String planNodename, PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
                    List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super(id, planNodename);
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

    public void setProbePartitionByExprs(List<Expr> probePartitionByExprs) {
        this.probePartitionByExprs = probePartitionByExprs;
    }

    public List<Expr> getProbePartitionByExprs() {
        return this.probePartitionByExprs;
    }

    @Override
    public void buildRuntimeFilters(IdGenerator<RuntimeFilterId> runtimeFilterIdIdGenerator, DescriptorTable descTbl) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        JoinOperator joinOp = getJoinOp();
        PlanNode inner = getChild(1);
        if (!joinOp.isInnerJoin() && !joinOp.isLeftSemiJoin() && !joinOp.isRightJoin() && !joinOp.isCrossJoin()) {
            return;
        }

        if (distrMode.equals(DistributionMode.PARTITIONED) || distrMode.equals(DistributionMode.SHUFFLE_HASH_BUCKET)) {
            // If it's partitioned join, and we can not get correct ndv
            // then it's hard to estimate right bloom filter size, or it's too big.
            // so we'd better to skip this global runtime filter.
            // If buildMaxSize == 0, the filter must be used
            // Otherwise would decide based on cardinality
            long card = inner.getCardinality();
            long buildMaxSize = sessionVariable.getGlobalRuntimeFilterBuildMaxSize();
            if (buildMaxSize > 0 && (card <= 0 || card > buildMaxSize)) {
                return;
            }
        }

        for (int i = 0; i < eqJoinConjuncts.size(); ++i) {
            BinaryPredicate joinConjunct = eqJoinConjuncts.get(i);
            Preconditions.checkArgument(BinaryPredicate.IS_EQ_NULL_PREDICATE.apply(joinConjunct) ||
                    BinaryPredicate.IS_EQ_PREDICATE.apply(joinConjunct));

            RuntimeFilterDescription rf = new RuntimeFilterDescription(sessionVariable);
            rf.setBuildPlanNodeId(this.id.asInt());
            rf.setExprOrder(i);
            rf.setJoinMode(distrMode);
            rf.setEqualCount(eqJoinConjuncts.size());
            rf.setBuildCardinality(inner.getCardinality());
            rf.setEqualForNull(BinaryPredicate.IS_EQ_NULL_PREDICATE.apply(joinConjunct));

            Expr left = joinConjunct.getChild(0);
            Expr right = joinConjunct.getChild(1);
            if (!joinOp.isCrossJoin()) {
                rf.setFilterId(runtimeFilterIdIdGenerator.getNextId().asInt());
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
                if (getChild(0).pushDownRuntimeFilters(descTbl, rf, right, probePartitionByExprs)) {
                    buildRuntimeFilters.add(rf);
                }
            } else {
                // For cross-join, the filter could only be pushed down to left side when
                // left expr is slot ref.
                if (!(left instanceof SlotRef)) {
                    continue;
                }
                if (!right.isBoundByTupleIds(getChild(1).getTupleIds())) {
                    continue;
                }

                rf.setFilterId(runtimeFilterIdIdGenerator.getNextId().asInt());
                rf.setBuildExpr(right);
                rf.setOnlyLocal(true);
                if (getChild(0).pushDownRuntimeFilters(descTbl, rf, left, probePartitionByExprs)) {
                    this.getBuildRuntimeFilters().add(rf);
                }
            }
        }
    }

    /**
     * Each slotExpr can deduce many slotExprs which is adjective because each join's conjunct can deduce left/right exprs.
     */
    public Optional<List<Expr>> candidatesOfSlotExprForChild(Expr expr, int childIdx) {
        if (!(expr instanceof SlotRef)) {
            return Optional.empty();
        }
        List<Expr> newSlotExprs = Lists.newArrayList();
        for (BinaryPredicate eqConjunct : eqJoinConjuncts) {
            Expr lhs = eqConjunct.getChild(0);
            Expr rhs = eqConjunct.getChild(1);
            // distinguish lhs/rhs belongs to left child or right child to decrease iterative times.
            if ((lhs instanceof SlotRef) && expr.isBound(((SlotRef) lhs).getSlotId()) ||
                    (rhs instanceof SlotRef) && expr.isBound(((SlotRef) rhs).getSlotId())) {
                if (lhs.isBoundByTupleIds(getChild(childIdx).getTupleIds())) {
                    newSlotExprs.add(lhs);
                }
                if (rhs.isBoundByTupleIds(getChild(childIdx).getTupleIds())) {
                    newSlotExprs.add(rhs);
                }
            }
        }
        return newSlotExprs.size() > 0 ? Optional.of(newSlotExprs) : Optional.empty();
    }

    public Optional<List<List<Expr>>> candidatesOfSlotExprsForChild(List<Expr> exprs, int childIdx) {
        if (!exprs.stream().allMatch(expr -> candidatesOfSlotExprForChild(expr, childIdx).isPresent())) {
            return Optional.empty();
        }
        List<List<Expr>> candidatesOfSlotExprs =
                exprs.stream().map(expr -> candidatesOfSlotExprForChild(expr, childIdx).get())
                        .collect(Collectors.toList());
        return Optional.of(candidateOfPartitionByExprs(candidatesOfSlotExprs));
    }

    public boolean pushDownRuntimeFiltersForChild(DescriptorTable descTbl, RuntimeFilterDescription description,
                                                  Expr probeExpr,
                                                  List<Expr> partitionByExprs, int childIdx) {
        return pushdownRuntimeFilterForChildOrAccept(descTbl, description, probeExpr,
                candidatesOfSlotExprForChild(probeExpr, childIdx),
                partitionByExprs, candidatesOfSlotExprsForChild(partitionByExprs, childIdx), childIdx, false);
    }

    @Override
    public boolean pushDownRuntimeFilters(DescriptorTable descTbl, RuntimeFilterDescription description, Expr probeExpr,
                                          List<Expr> partitionByExprs) {
        if (!canPushDownRuntimeFilter()) {
            return false;
        }

        if (probeExpr.isBoundByTupleIds(getTupleIds())) {
            boolean hasPushedDown = false;
            // If probeExpr is SlotRef(a) and an equalJoinConjunct SlotRef(a)=SlotRef(b) exists in SemiJoin
            // or InnerJoin, then the rf also can be pushed down to both sides of HashJoin because SlotRef(a) and
            // SlotRef(b) are equivalent.
            boolean isInnerOrSemiJoin = joinOp.isSemiJoin() || joinOp.isInnerJoin();
            if ((probeExpr instanceof SlotRef) && isInnerOrSemiJoin) {
                hasPushedDown |= pushDownRuntimeFiltersForChild(descTbl, description, probeExpr, partitionByExprs, 0);
                hasPushedDown |= pushDownRuntimeFiltersForChild(descTbl, description, probeExpr, partitionByExprs, 1);
            }
            // fall back to PlanNode.pushDownRuntimeFilters for HJ if rf cannot be pushed down via equivalent
            // equalJoinConjuncts
            if (hasPushedDown || super.pushDownRuntimeFilters(descTbl, description, probeExpr, partitionByExprs)) {
                return true;
            }

            // use runtime filter at this level if rf can not be pushed down to children.
            if (description.canProbeUse(this)) {
                description.addProbeExpr(id.asInt(), probeExpr);
                description.addPartitionByExprsIfNeeded(id.asInt(), probeExpr, partitionByExprs);
                probeRuntimeFilters.add(description);
                return true;
            }
        }
        return false;
    }

    public JoinOperator getJoinOp() {
        return joinOp;
    }

    public List<BinaryPredicate> getEqJoinConjuncts() {
        return eqJoinConjuncts;
    }

    public DistributionMode getDistrMode() {
        return distrMode;
    }

    public void setDistributionMode(DistributionMode distrMode) {
        this.distrMode = distrMode;
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
    }

    @Override
    public void computeStats(Analyzer analyzer) {
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

    public void setIsPushDown(boolean isPushDown) {
        this.isPushDown = isPushDown;
    }

    public boolean getCanLocalShuffle() {
        return canLocalShuffle;
    }

    public void setCanLocalShuffle(boolean v) {
        canLocalShuffle = v;
    }

    public void setFKRight(boolean fkRight) {
        this.isFKRight = fkRight;
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String distrModeStr =
                (distrMode != DistributionMode.NONE) ? (" (" + distrMode.toString() + ")") : "";
        StringBuilder output = new StringBuilder().append(
                detailPrefix + "join op: " + joinOp.toString() + distrModeStr + "\n");

        if (detailLevel == TExplainLevel.VERBOSE) {
            if (isColocate) {
                output.append(detailPrefix).append("colocate: ").append(isColocate).append("\n");
            }
        } else {
            output.append(detailPrefix).append("colocate: ").append(isColocate)
                    .append(isColocate ? "" : ", reason: " + colocateReason).append("\n");
        }

        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            output.append(detailPrefix).append("equal join conjunct: ");
            if (detailLevel.equals(TExplainLevel.VERBOSE)) {
                output.append(eqJoinPredicate.explain());
            } else {
                output.append(eqJoinPredicate.toSql());
            }
            output.append("\n");
        }
        if (!otherJoinConjuncts.isEmpty()) {
            output.append(detailPrefix + "other join predicates: ").append(
                    getVerboseExplain(otherJoinConjuncts, detailLevel) + "\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("other predicates: ")
                    .append(getVerboseExplain(conjuncts, detailLevel))
                    .append("\n");
        }

        if (detailLevel == TExplainLevel.VERBOSE) {

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

            if (FeConstants.showJoinLocalShuffleInExplain) {
                output.append(detailPrefix).append("can local shuffle: " + canLocalShuffle + "\n");
            }
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

        public boolean areBothSidesShuffled() {
            return this == SHUFFLE_HASH_BUCKET || this == PARTITIONED;
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
