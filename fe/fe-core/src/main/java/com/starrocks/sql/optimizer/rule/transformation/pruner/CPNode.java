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

package com.starrocks.sql.optimizer.rule.transformation.pruner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

// CPNode is used to construct cardinality-preserving tree.
public class CPNode {
    OptExpression value;
    CPNode parent;
    Set<CPNode> children = Collections.emptySet();

    Map<OptExpression, Map<ColumnRefOperator, ColumnRefOperator>> equivColumnRefs = Collections.emptyMap();
    boolean hubFlag;

    Set<CPNode> nonCPChildren = Collections.emptySet();

    public CPNode(OptExpression value, CPNode parent, boolean hubFlag) {
        this.value = value;
        this.parent = parent;
        this.hubFlag = hubFlag;
    }

    public static CPNode createNode(OptExpression value) {
        return new CPNode(value, null, false);
    }

    public static CPNode createHubNode(CPNode... children) {
        CPNode hubNode = new CPNode(null, null, true);
        Arrays.stream(children).forEach(hubNode::addChild);
        return hubNode;
    }

    public void addEqColumnRefs(OptExpression optExpr, Map<ColumnRefOperator, ColumnRefOperator> eqColumnRefs) {
        if (equivColumnRefs.isEmpty()) {
            equivColumnRefs = Maps.newHashMap();
        }
        equivColumnRefs.put(optExpr, Collections.unmodifiableMap(eqColumnRefs));
    }

    public CPNode getParent() {
        return parent;
    }

    public void setParent(CPNode parent) {
        this.parent = parent;
    }

    public boolean isHub() {
        return hubFlag;
    }

    public boolean isRoot() {
        return parent == null;
    }

    public boolean isLeaf() {
        return children == null || children.isEmpty();
    }

    public void addChild(CPNode child) {
        if (children.isEmpty()) {
            children = Sets.newHashSet();
        }
        children.add(child);
        child.setParent(this);
    }

    public void addNonCPChild(CPNode child) {
        Preconditions.checkArgument(hubFlag);
        if (nonCPChildren.isEmpty()) {
            nonCPChildren = Sets.newHashSet();
        }
        nonCPChildren.add(child);
        child.setParent(this);
    }

    public static Optional<CPNode> mergeHubNode(CPNode lhsNode, CPNode rhsNode) {
        if (lhsNode == rhsNode) {
            return Optional.empty();
        }
        rhsNode.children.forEach(lhsNode::addChild);
        rhsNode.nonCPChildren.forEach(lhsNode::addNonCPChild);
        return Optional.of(rhsNode);
    }

    public Set<CPNode> getChildren() {
        return children;
    }

    public Set<CPNode> getNonCPChildren() {
        return nonCPChildren;
    }

    public OptExpression getValue() {
        return value;
    }

    public Map<OptExpression, Map<ColumnRefOperator, ColumnRefOperator>> getEquivColumnRefs() {
        return equivColumnRefs;
    }

    public boolean intersect(Set<OptExpression> optExpressions) {
        if (hubFlag) {
            return children.stream().anyMatch(node -> optExpressions.contains(node.getValue())) ||
                    nonCPChildren.stream().anyMatch(node -> optExpressions.contains(node.getValue()));
        } else {
            return optExpressions.contains(value);
        }
    }
}
