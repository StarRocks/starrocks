// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/SetOperationNode.java

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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleId;
import com.starrocks.thrift.TExceptNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TIntersectNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TUnionNode;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Node that merges the results of its child plans, Normally, this is done by
 * materializing the corresponding result exprs into a new tuple. However, if
 * a child has an identical tuple layout as the output of the set operation node, and
 * the child only has naked SlotRefs as result exprs, then the child is marked
 * as 'passthrough'. The rows of passthrough children are directly returned by
 * the set operation node, instead of materializing the child's result exprs into new
 * tuples.
 */
public abstract class SetOperationNode extends PlanNode {
    private static final Logger LOG = LoggerFactory.getLogger(SetOperationNode.class);

    // List of set operation result exprs of the originating SetOperationStmt. Used for
    // determining passthrough-compatibility of children.
    protected List<Expr> setOpResultExprs_;

    // Expr lists corresponding to the input query stmts.
    // The ith resultExprList belongs to the ith child.
    // All exprs are resolved to base tables.
    protected List<List<Expr>> resultExprLists_ = Lists.newArrayList();

    // Expr lists that originate from constant select stmts.
    // We keep them separate from the regular expr lists to avoid null children.
    protected List<List<Expr>> constExprLists_ = Lists.newArrayList();

    // Materialized result/const exprs corresponding to materialized slots.
    // Set in init() and substituted against the corresponding child's output smap.
    protected List<List<Expr>> materializedResultExprLists_ = Lists.newArrayList();
    protected List<List<Expr>> materializedConstExprLists_ = Lists.newArrayList();

    // Indicates if this UnionNode is inside a subplan.
    protected boolean isInSubplan_;

    // Index of the first non-passthrough child.
    protected int firstMaterializedChildIdx_;

    protected final TupleId tupleId_;

    protected List<Map<Integer, Integer>> outputSlotIdToChildSlotIdMaps = Lists.newArrayList();

    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName) {
        super(id, tupleId.asList(), planNodeName);
        setOpResultExprs_ = Lists.newArrayList();
        tupleId_ = tupleId;
        isInSubplan_ = false;
    }

    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName,
                               List<Expr> setOpResultExprs,
                               boolean isInSubplan) {
        super(id, tupleId.asList(), planNodeName);
        setOpResultExprs_ = setOpResultExprs;
        tupleId_ = tupleId;
        isInSubplan_ = isInSubplan;
    }

    public void addConstExprList(List<Expr> exprs) {
        constExprLists_.add(exprs);
    }

    /**
     * Add a child tree plus its corresponding unresolved resultExprs.
     */
    public void addChild(PlanNode node, List<Expr> resultExprs) {
        super.addChild(node);
        resultExprLists_.add(resultExprs);
    }

    public void setMaterializedResultExprLists_(List<List<Expr>> materializedResultExprLists_) {
        this.materializedResultExprLists_ = materializedResultExprLists_;
    }

    public void setMaterializedConstExprLists_(List<List<Expr>> materializedConstExprLists_) {
        this.materializedConstExprLists_ = materializedConstExprLists_;
    }

    public void setFirstMaterializedChildIdx_(int firstMaterializedChildIdx_) {
        this.firstMaterializedChildIdx_ = firstMaterializedChildIdx_;
    }

    public void setOutputSlotIdToChildSlotIdMaps(List<Map<Integer, Integer>> outputSlotIdToChildSlotIdMaps) {
        this.outputSlotIdToChildSlotIdMaps = outputSlotIdToChildSlotIdMaps;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
    }

    /**
     * Must be called after addChild()/addConstExprList(). Computes the materialized
     * result/const expr lists based on the materialized slots of this UnionNode's
     * produced tuple. The UnionNode doesn't need an smap: like a ScanNode, it
     * materializes an original tuple.
     * There is no need to call assignConjuncts() because all non-constant conjuncts
     * have already been assigned to the set operation operands, and all constant conjuncts have
     * been evaluated during registration to set analyzer.hasEmptyResultSet_.
     */
    @Override
    public void init(Analyzer analyzer) {
    }

    protected void toThrift(TPlanNode msg, TPlanNodeType nodeType) {
        Preconditions.checkState(materializedResultExprLists_.size() == children.size());
        List<List<TExpr>> texprLists = Lists.newArrayList();
        for (List<Expr> exprList : materializedResultExprLists_) {
            texprLists.add(Expr.treesToThrift(exprList));
        }
        List<List<TExpr>> constTexprLists = Lists.newArrayList();
        for (List<Expr> constTexprList : materializedConstExprLists_) {
            constTexprLists.add(Expr.treesToThrift(constTexprList));
        }
        Preconditions.checkState(firstMaterializedChildIdx_ <= children.size());
        switch (nodeType) {
            case UNION_NODE:
                msg.union_node = new TUnionNode(
                        tupleId_.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx_);
                msg.union_node.setPass_through_slot_maps(outputSlotIdToChildSlotIdMaps);
                msg.node_type = TPlanNodeType.UNION_NODE;
                break;
            case INTERSECT_NODE:
                msg.intersect_node = new TIntersectNode(
                        tupleId_.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx_);
                msg.node_type = TPlanNodeType.INTERSECT_NODE;
                break;
            case EXCEPT_NODE:
                msg.except_node = new TExceptNode(
                        tupleId_.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx_);
                msg.node_type = TPlanNodeType.EXCEPT_NODE;
                break;
            default:
                LOG.error("Node type: " + nodeType.toString() + " is invalid.");
                break;
        }
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        // A SetOperationNode may have predicates if a union is set operation inside an inline view,
        // and the enclosing select stmt has predicates referring to the inline view.
        if (CollectionUtils.isNotEmpty(conjuncts)) {
            output.append(prefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (CollectionUtils.isNotEmpty(constExprLists_)) {
            output.append(prefix).append("constant exprs: ").append("\n");
            for (List<Expr> exprs : constExprLists_) {
                output.append(prefix).append("    ").append(exprs.stream().map(Expr::toSql)
                        .collect(Collectors.joining(" | "))).append("\n");
            }
        }
        if (detailLevel == TExplainLevel.VERBOSE) {
            if (CollectionUtils.isNotEmpty(materializedResultExprLists_)) {
                output.append(prefix).append("child exprs:").append("\n");
                for (List<Expr> exprs : materializedResultExprLists_) {
                    output.append(prefix).append("    ").append(exprs.stream().map(Expr::explain)
                            .collect(Collectors.joining(" | "))).append("\n");
                }
            }
            List<String> passThroughNodeIds = Lists.newArrayList();
            for (int i = 0; i < firstMaterializedChildIdx_; ++i) {
                passThroughNodeIds.add(children.get(i).getId().toString());
            }
            if (!passThroughNodeIds.isEmpty()) {
                String result = prefix + "pass-through-operands: ";
                if (passThroughNodeIds.size() == children.size()) {
                    output.append(result).append("all\n");
                } else {
                    output.append(result).append(Joiner.on(",").join(passThroughNodeIds)).append("\n");
                }
            }
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        int numInstances = 0;
        for (PlanNode child : children) {
            numInstances += child.getNumInstances();
        }
        numInstances = Math.max(1, numInstances);
        return numInstances;
    }

    @Override
    public boolean canDoReplicatedJoin() {
        return false;
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterDescription description, Expr probeExpr) {
        if (!canPushDownRuntimeFilter()) {
            return false;
        }
        if (!probeExpr.isBoundByTupleIds(getTupleIds())) {
            return false;
        }

        if (probeExpr instanceof SlotRef) {
            boolean pushDown = false;
            int probeSlotId = ((SlotRef) probeExpr).getSlotId().asInt();
            Set<Integer> mappedProbeSlotIds = new HashSet<>();
            for (Map<Integer, Integer> map : outputSlotIdToChildSlotIdMaps) {
                if (map.containsKey(probeSlotId)) {
                    mappedProbeSlotIds.add(map.get(probeSlotId));
                }
            }

            for (int i = 0; i < materializedResultExprLists_.size(); i++) {
                // try to push all children if any expr of a child can match `probeExpr`
                for (Expr mexpr : materializedResultExprLists_.get(i)) {
                    if ((mexpr instanceof SlotRef) &&
                            mappedProbeSlotIds.contains(((SlotRef) mexpr).getSlotId().asInt())) {
                        if (children.get(i).pushDownRuntimeFilters(description, mexpr)) {
                            pushDown = true;
                        }
                        break;
                    }
                }
            }
            if (pushDown) {
                return true;
            }
        }

        if (description.canProbeUse(this)) {
            // can not push down to children.
            // use runtime filter at this level.
            description.addProbeExpr(id.asInt(), probeExpr);
            probeRuntimeFilters.add(description);
            return true;
        }
        return false;
    }
}
