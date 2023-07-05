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

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.CPBiRel;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JoinReorderCardinalityPreserving extends JoinOrder {
    List<OptExpression> atomOptExprs;
    List<ScalarOperator> predicates;
    Map<ColumnRefOperator, ScalarOperator> expressionMap;

    Optional<OptExpression> bestPlanRoot = Optional.empty();

    public JoinReorderCardinalityPreserving(OptimizerContext context) {
        super(context);
    }

    @Override
    void init(List<OptExpression> atoms, List<ScalarOperator> predicates,
              Map<ColumnRefOperator, ScalarOperator> expressionMap) {
        super.init(atoms, predicates, expressionMap);
        this.atomOptExprs = atoms;
        this.predicates = predicates;
        this.expressionMap = expressionMap;
    }

    private static class Graph {
        private Map<OptExpression, Integer> nodes;
        private List<Set<OptExpression>> inEdges;
        private List<Set<OptExpression>> outEdges;

        public Graph(List<OptExpression> nodes) {
            Preconditions.checkArgument(!nodes.isEmpty());
            this.nodes = Maps.newHashMap();
            Iterator<OptExpression> nodeIter = nodes.iterator();
            inEdges = new ArrayList<>(nodes.size());
            outEdges = new ArrayList<>(nodes.size());
            for (int i = 0; i < nodes.size(); ++i) {
                this.nodes.put(nodeIter.next(), i);
                inEdges.add(Sets.newHashSet());
                outEdges.add(Sets.newHashSet());
            }
        }

        public void addEdge(OptExpression from, OptExpression to) {
            outEdges.get(nodes.get(from)).add(to);
            inEdges.get(nodes.get(to)).add(from);
        }

        public List<OptExpression> getRoot() {
            return nodes.entrySet().stream().filter(e -> inEdges.get(e.getValue()).isEmpty()).map(e -> e.getKey())
                    .collect(Collectors.toList());
        }

        public List<OptExpression> chooseOrder() {
            List<OptExpression> roots = getRoot();
            Set<OptExpression> visited = Sets.newHashSet();
            // graph has source nodes.
            if (!roots.isEmpty()) {
                // travel each source node.
                List<List<OptExpression>> components = Lists.newArrayList();
                for (int i = 0; i < roots.size(); ++i) {
                    List<OptExpression> component = Lists.newArrayList();
                    travel(roots.get(i), visited, component);
                    components.add(component);
                }
                // put the longest chain in the front.
                components.sort(Comparator.comparingInt(List::size));
                Collections.reverse(components);
                return components.stream().flatMap(Collection::stream).collect(Collectors.toList());
            } else {
                // no source nodes, travel all node, find the longest chain.
                List<OptExpression> longestOrders = Collections.emptyList();
                for (OptExpression node : nodes.keySet()) {
                    visited.clear();
                    List<OptExpression> orders = Lists.newArrayList();
                    travel(node, visited, orders);
                    if (orders.size() > longestOrders.size()) {
                        longestOrders = orders;
                    }
                    if (longestOrders.size() == nodes.size()) {
                        break;
                    }
                }
                return longestOrders;
            }
        }

        // BFS travel
        void travel(OptExpression root, Set<OptExpression> visited, List<OptExpression> component) {
            Queue<OptExpression> q0 = new LinkedList<OptExpression>();
            Queue<OptExpression> q1 = new LinkedList<OptExpression>();
            Function<OptExpression, Long> getTableId =
                    (optExpr) -> ((LogicalScanOperator) optExpr.getOp()).getTable().getId();
            q0.add(root);
            while (!q0.isEmpty()) {
                while (!q0.isEmpty()) {
                    OptExpression curr = q0.remove();
                    if (visited.contains(curr)) {
                        continue;
                    }
                    visited.add(curr);
                    component.add(curr);
                    Long currTableId = getTableId.apply(curr);
                    // permute LogicalScanOperator originates from the same table together will give
                    // CboTablePruneRule more chance to prune tables.
                    List<OptExpression> children =
                            outEdges.get(nodes.get(curr)).stream().filter(child -> !visited.contains(child))
                                    .sorted((lhs, rhs) -> Boolean.compare(
                                            !getTableId.apply(lhs).equals(currTableId),
                                            !getTableId.apply(rhs).equals(currTableId)))
                                    .collect(Collectors.toList());
                    q1.addAll(children);
                }
                Queue<OptExpression> tmp = q0;
                q0 = q1;
                q1 = tmp;
            }
        }
    }

    @Override
    protected void enumerate() {
        // We only try to reorder atoms which are LogicalScanOperator, since at present,
        // we can extract cardinality-preserving relation from a pair of OlapTable.
        List<OptExpression> scanOps = atomOptExprs.stream().filter(opt -> {
            if (!(opt.getOp() instanceof LogicalScanOperator)) {
                return false;
            }
            LogicalScanOperator scanOp = opt.getOp().cast();
            if (!(scanOp.getTable() instanceof OlapTable)) {
                return false;
            }
            OlapTable table = ((OlapTable) scanOp.getTable());
            return table.hasUniqueConstraints() || table.hasForeignKeyConstraints();
        }).collect(Collectors.toList());

        if (scanOps.isEmpty()) {
            return;
        }

        // Construct a mapping from ColumnRefOperator to OptExpression, later we
        // only try matches equality predicate that references ColumnRefOperators backed
        // by real columns of OlapTables to pairs of ColumnRefOperator of cardinality-preserving
        // relation of two OptExpression.
        Map<ColumnRefOperator, OptExpression> colRefToScanNodes = Maps.newHashMap();
        for (OptExpression scanOp : scanOps) {
            LogicalScanOperator scan = scanOp.getOp().cast();
            scan.getColRefToColumnMetaMap().forEach((k, v) -> colRefToScanNodes.put(k, scanOp));
        }

        Map<Pair<OptExpression, OptExpression>, Set<Pair<ColumnRefOperator, ColumnRefOperator>>> biRelToColRefPairs =
                Maps.newHashMap();
        for (ScalarOperator predicate : predicates) {
            if (!(predicate instanceof BinaryPredicateOperator)) {
                continue;
            }
            BinaryPredicateOperator binPredicate = (BinaryPredicateOperator) predicate;
            if (binPredicate.getBinaryType() != BinaryType.EQ) {
                continue;
            }
            ScalarOperator lhs = binPredicate.getChild(0);
            ScalarOperator rhs = binPredicate.getChild(1);
            if (!(lhs instanceof ColumnRefOperator) && (rhs instanceof ColumnRefOperator)) {
                continue;
            }

            ColumnRefOperator lhsColRef = lhs.cast();
            ColumnRefOperator rhsColRef = rhs.cast();
            // column ref references real column
            if (!(colRefToScanNodes.containsKey(lhsColRef) && colRefToScanNodes.containsKey(rhsColRef))) {
                continue;
            }

            OptExpression lhsOptExpr = colRefToScanNodes.get(lhsColRef);
            OptExpression rhsOptExpr = colRefToScanNodes.get(rhsColRef);
            // Pair(OptExprA, OptExprB) and Pair(OptExprB, OptExprA) are the same, so normalize them into
            // one form.
            if (lhsOptExpr.hashCode() < rhsOptExpr.hashCode()) {
                OptExpression tmpOptExpr = lhsOptExpr;
                lhsOptExpr = rhsOptExpr;
                rhsOptExpr = tmpOptExpr;
                ColumnRefOperator tmpColRef = lhsColRef;
                lhsColRef = rhsColRef;
                rhsColRef = tmpColRef;
            }

            biRelToColRefPairs.computeIfAbsent(Pair.create(lhsOptExpr, rhsOptExpr),
                    (k) -> Sets.newHashSet()).add(Pair.create(lhsColRef, rhsColRef));
        }

        Graph graph = new Graph(scanOps);
        // Try to extract cardinality-preserving relation from a pair of OptExpressions
        // Construct a graph from cardinality-preserving relations.
        for (Map.Entry<Pair<OptExpression, OptExpression>, Set<Pair<ColumnRefOperator, ColumnRefOperator>>>
                e : biRelToColRefPairs.entrySet()) {
            OptExpression lhsOptExpr = e.getKey().first;
            OptExpression rhsOptExpr = e.getKey().second;
            Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairs = e.getValue();
            Set<Pair<ColumnRefOperator, ColumnRefOperator>> inversePairs = pairs.stream().map(Pair::inverse).collect(
                    Collectors.toSet());
            List<CPBiRel> biRels = Lists.newArrayList();
            biRels.addAll(CPBiRel.getCPBiRels(lhsOptExpr, rhsOptExpr, true));
            biRels.addAll(CPBiRel.getCPBiRels(rhsOptExpr, lhsOptExpr, false));
            for (CPBiRel biRel : biRels) {
                if (biRel.isLeftToRight() && biRel.getPairs().equals(pairs)) {
                    graph.addEdge(lhsOptExpr, rhsOptExpr);
                } else if (!biRel.isLeftToRight() && biRel.getPairs().equals(inversePairs)) {
                    graph.addEdge(rhsOptExpr, lhsOptExpr);
                }
            }
        }
        // choose the longest cardinality-preserving chains.
        List<OptExpression> order = graph.chooseOrder();
        Set<OptExpression> orderedScanNodes = new HashSet<>(order);
        Set<OptExpression> allScanNodes = new HashSet<>(scanOps);
        Set<OptExpression> allNodes = new HashSet<>(atomOptExprs);
        // add remaining OptExpressions to the order.
        order.addAll(Sets.difference(allScanNodes, orderedScanNodes));
        order.addAll(Sets.difference(allNodes, allScanNodes));
        List<GroupInfo> atoms = joinLevels.get(1).groups;
        Preconditions.checkArgument(order.size() == atoms.size());
        Map<OptExpression, GroupInfo> optExprToAtoms =
                atoms.stream().collect(Collectors.toMap(g -> g.bestExprInfo.expr, Function.identity()));
        // construct left-deep join tree, the longest cardinality-preserving chain in the bottom
        // of the left-deep tree. because our cardinality-preserving relation infer algorithm is
        // bottom-up style, it can benefit from left-deep tree.
        atoms.clear();
        for (OptExpression optExpr : order) {
            atoms.add(optExprToAtoms.get(optExpr));
        }
        boolean[] used = new boolean[atomSize];
        GroupInfo leftGroup = atoms.get(0);
        used[0] = true;
        int next = 1;
        while (next < atomSize) {
            if (used[next]) {
                next++;
                continue;
            }
            int index = next;
            used[index] = true;

            GroupInfo rightGroup = atoms.get(index);
            ExpressionInfo joinExpr = buildJoinExpr(leftGroup, atoms.get(index));
            joinExpr.expr.deriveLogicalPropertyItself();

            BitSet joinBitSet = new BitSet();
            joinBitSet.or(leftGroup.atoms);
            joinBitSet.or(rightGroup.atoms);

            leftGroup = new GroupInfo(joinBitSet);
            leftGroup.bestExprInfo = joinExpr;
        }
        bestPlanRoot = Optional.of(leftGroup.bestExprInfo.expr);
    }

    @Override
    public List<OptExpression> getResult() {
        return bestPlanRoot.map(Collections::singletonList).orElse(Collections.emptyList());
    }
}
