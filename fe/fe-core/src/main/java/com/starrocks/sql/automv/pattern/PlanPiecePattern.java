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

package com.starrocks.sql.automv.pattern;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// PlanPiecePattern is a pattern language used to extract sub-plans that matches
// the user-defined pattern from LogicalPlan. For an example, user can define a
// SPJG pattern, then extract SPJG sub-plans to create MV to accelerated it.
//
// PlanPiecePattern support capture groups alike regexp, so it can be used to
// extract nested sub-plans to help AutoMV to recommend nested MV in future.
public abstract class PlanPiecePattern {
    private static final OperatorVisitor<Optional<NodeDesc>, Void> VISITOR =
            new OperatorVisitor<Optional<NodeDesc>, Void>() {
                @Override
                public Optional<NodeDesc> visitOperator(Operator node, Void context) {
                    return Optional.empty();
                }

                @Override
                public Optional<NodeDesc> visitLogicalTableScan(LogicalScanOperator node, Void context) {
                    NodeDesc desc = new NodeDesc();
                    desc.name = NodeName.Scan;
                    Table table = node.getTable();
                    desc.attributes.put("tableName", ConstantOperator.createVarchar(table.getName()));
                    desc.attributes.put("tableType", ConstantOperator.createVarchar(table.getType().toString()));
                    desc.attributes.put("tableEngine", ConstantOperator.createVarchar(table.getEngine()));
                    desc.attributes.put("catalog", ConstantOperator.createVarchar(table.getCatalogName()));
                    /*
                    boolean isPartitioned = table.isPartitioned() || !table.isUnPartitioned();
                    desc.attributes.put("isPartitioned", ConstantOperator.createBoolean(isPartitioned));
                    int partitionColumnCount = table.getPartitionColumns().size();
                    desc.attributes.put("partitionColumnCount",
                            ConstantOperator.createInt(partitionColumnCount));

                    ConstantOperator partitionColumnType = partitionColumnCount == 0 ? ConstantOperator.NULL :
                            ConstantOperator.createVarchar(
                                    table.getPartitionColumns().get(0).getType().getPrimitiveType().toString());
                    desc.attributes.put("partitionColumnType", partitionColumnType);

                     */
                    return Optional.of(desc);
                }

                @Override
                public Optional<NodeDesc> visitLogicalJoin(LogicalJoinOperator node, Void context) {
                    NodeDesc desc = new NodeDesc();
                    desc.name = NodeName.Join;
                    desc.attributes.put("", ConstantOperator.createVarchar(node.getJoinType().toThrift().name()));
                    return Optional.of(desc);
                }

                @Override
                public Optional<NodeDesc> visitLogicalAggregation(LogicalAggregationOperator node, Void context) {
                    NodeDesc desc = new NodeDesc();
                    desc.name = NodeName.Aggregate;
                    desc.attributes.put("distinct", ConstantOperator.createBoolean(!node.getAggregations().isEmpty()));
                    long numDistinctAggs = node.getAggregations().values().stream().filter(
                            agg -> agg.isDistinct() ||
                                    agg.getFnName().equals(FunctionSet.MULTI_DISTINCT_COUNT) ||
                                    agg.getFnName().equals(FunctionSet.MULTI_DISTINCT_SUM)
                    ).count();
                    desc.attributes.put("hasDistinct", ConstantOperator.createBoolean(numDistinctAggs > 0));
                    desc.attributes.put("multiDistinct", ConstantOperator.createBoolean(numDistinctAggs > 1));
                    desc.attributes.put("noGroupBy", ConstantOperator.createBoolean(!node.getGroupingKeys().isEmpty()));
                    return Optional.of(desc);
                }
            };

    public static Node node(NodeName nodeName) {
        return new Node(nodeName, ConstantOperator.TRUE);
    }

    public static PlanPiecePattern repeat(Node pat, int atLeast, int atMost) {
        return new RepeatPattern(atLeast, atMost, pat);
    }

    public static PlanPiecePattern consistOf(PlanPiecePattern... pats) {
        return new TreeConsistOfPattern(Arrays.asList(pats));
    }

    public static PlanPiecePattern tree(Node node, List<PlanPiecePattern> children, PlanPiecePattern variadicChild) {
        return new TreePattern(false, node, children, variadicChild);
    }

    public static PlanPiecePattern treeCapture(Node node, List<PlanPiecePattern> children,
                                               PlanPiecePattern variadicChild) {
        return new TreePattern(true, node, children, variadicChild);
    }

    public static boolean match(OptExpression root, PlanPiecePattern pat) {
        PlanPiecePattern.MatchContext context = new PlanPiecePattern.MatchContext();
        return pat.match(root, context);
    }

    public static List<OptExpression> extract(OptExpression root, PlanPiecePattern pat) {
        List<OptExpression> subPlans = Lists.newArrayList();
        if (match(root, pat)) {
            subPlans.add(root);
        } else {
            for (OptExpression input : root.getInputs()) {
                subPlans.addAll(extract(input, pat));
            }
        }
        return subPlans;
    }

    public static Optional<OptExpression> getAggRoot(OptExpression root) {
        OptExpression newRoot = root;
        while (true) {
            OperatorType opType = newRoot.getOp().getOpType();
            if (opType.equals(OperatorType.LOGICAL) || opType.equals(OperatorType.LOGICAL_PROJECT)) {
                newRoot = root.inputAt(0);
            } else if (opType.equals(OperatorType.LOGICAL_CTE_ANCHOR)) {
                newRoot = root.inputAt(1);
            } else if (opType.equals(OperatorType.LOGICAL_AGGR)) {
                return Optional.of(newRoot);
            } else {
                return Optional.empty();
            }
        }
    }

    public static Optional<OptExpression> extractEntire(OptExpression root, PlanPiecePattern pat) {
        return getAggRoot(root).flatMap(newRoot ->
                extract(root, pat).stream().filter(subPlan -> subPlan == newRoot).findFirst());
    }

    public abstract boolean matchRoot(OptExpression op, MatchContext context);

    public boolean match(OptExpression op, MatchContext context) {
        int opChildCount = op.getInputs().size();
        if (!matchRoot(op, context)) {
            return false;
        }
        return IntStream.range(0, opChildCount).allMatch(i -> this.getChild(i).match(op.inputAt(i), context));
    }

    public PlanPiecePattern getChild(int i) {
        return this;
    }

    enum NodeName {
        UNKNOWN,
        Scan,
        Join,
        Aggregate,
        Union,
        UnionAll,
    }

    public static class MatchContext {
        private final Map<RepeatPattern, Integer> matchTimes = Maps.newHashMap();
        private final Map<PlanPiecePattern, Integer> groupIds = Maps.newHashMap();
        private final List<List<OptExpression>> groups = Lists.newArrayList();

        int getMatchTimes(RepeatPattern pat) {
            return matchTimes.getOrDefault(pat, 0);
        }

        void incrMatchTimes(RepeatPattern pat) {
            matchTimes.merge(pat, 1, Integer::sum);
        }

        void addCapturedGroup(PlanPiecePattern pat, OptExpression op) {
            int ordinal;
            if (!groupIds.containsKey(pat)) {
                ordinal = groupIds.size();
                groupIds.put(pat, ordinal);
                groups.add(Lists.newArrayList());
            } else {
                ordinal = groupIds.size() - 1;
            }
            groups.get(ordinal).add(op);
        }
    }

    public abstract static class SimplePattern extends PlanPiecePattern {
    }

    public abstract static class DecoratedPattern extends PlanPiecePattern {

    }

    public abstract static class ComposedPattern extends PlanPiecePattern {
    }

    static final class NodeDesc {
        NodeName name = NodeName.UNKNOWN;
        Map<String, ConstantOperator> attributes = Maps.newHashMap();

        Pair<ColumnRefOperator, ScalarOperator> getAttribute(ColumnRefOperator attr) {
            String attrName = attr.getName();
            ConstantOperator value = Optional.ofNullable(attributes).map(attrs -> attrs.get(attrName))
                    .orElse(ConstantOperator.NULL);
            return Pair.create(attr, value);
        }
    }

    public static final class Node extends SimplePattern {
        NodeName name;
        ScalarOperator predicate;

        public Node(NodeName name, ScalarOperator predicate) {
            this.name = name;
            this.predicate = predicate;
        }

        private boolean matchImpl(NodeDesc desc) {
            if (!desc.name.equals(name)) {
                return false;
            }

            List<ColumnRefOperator> colRefs = predicate.getColumnRefs();
            Map<ColumnRefOperator, ScalarOperator> args =
                    colRefs.stream().map(desc::getAttribute)
                            .collect(Collectors.toMap(p -> p.first, p -> p.second));

            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(args, false);
            ScalarOperatorRewriter constFoldRewriter = new ScalarOperatorRewriter();
            ScalarOperator result = constFoldRewriter.rewrite(rewriter.rewrite(predicate),
                    Collections.singletonList(new FoldConstantsRule()));
            return result.isConstantTrue();
        }

        @Override
        public boolean matchRoot(OptExpression op, MatchContext context) {
            return op.getOp().accept(VISITOR, null).map(this::matchImpl).orElse(false);
        }
    }

    public static final class RepeatPattern extends DecoratedPattern {
        final int atLeast;
        final int atMost;
        final Node node;

        public RepeatPattern(int atLeast, int atMost, Node node) {
            this.atLeast = atLeast;
            this.atMost = atMost;
            this.node = node;
        }

        public int getAtLeast() {
            return atLeast;
        }

        public int getAtMost() {
            return atMost;
        }

        @Override
        public boolean matchRoot(OptExpression op, MatchContext context) {
            int hits = context.getMatchTimes(this);
            if (hits >= getAtMost()) {
                return false;
            }
            if (OperatorType.LOGICAL_PROJECT.equals(op.getOp().getOpType())) {
                op = op.inputAt(0);
            }
            if (node.matchRoot(op, context)) {
                context.incrMatchTimes(this);
                return true;
            } else {
                return false;
            }
        }
    }

    public static final class TreeConsistOfPattern extends ComposedPattern {
        final List<PlanPiecePattern> occurrences;

        public TreeConsistOfPattern(List<PlanPiecePattern> occurrences) {
            Preconditions.checkArgument(!occurrences.isEmpty());
            this.occurrences = occurrences;
        }

        @Override
        public boolean matchRoot(OptExpression op, MatchContext context) {
            for (PlanPiecePattern pat : occurrences) {
                if (pat.matchRoot(op, context)) {
                    return true;
                }
            }
            return false;
        }
    }

    public static final class TreePattern extends ComposedPattern {
        final boolean isCapture;
        final Node root;
        final List<PlanPiecePattern> children;

        final PlanPiecePattern variadicLastChild;

        public TreePattern(boolean isCapture, Node root, List<PlanPiecePattern> children,
                           PlanPiecePattern variadicLastChild) {
            Preconditions.checkArgument(!children.isEmpty());
            this.isCapture = isCapture;
            this.root = root;
            this.children = children;
            this.variadicLastChild = Optional.ofNullable(variadicLastChild).orElse(MATCH_NONE);
        }

        @Override
        public boolean matchRoot(OptExpression op, MatchContext context) {
            return root.matchRoot(op, context);
        }

        @Override
        public boolean match(OptExpression op, MatchContext context) {
            if (!super.match(op, context)) {
                return false;
            }
            if (op.getInputs().size() < this.children.size()) {
                return false;
            }
            boolean repeatMatch = context.matchTimes.entrySet().stream()
                    .allMatch(e -> e.getValue() >= e.getKey().atLeast && e.getValue() <= e.getKey().getAtMost());
            if (!repeatMatch) {
                return false;
            }
            if (isCapture) {
                context.addCapturedGroup(this, op);
            }
            return true;
        }

        @Override
        public PlanPiecePattern getChild(int i) {
            if (i < this.children.size()) {
                return this.children.get(i);
            } else {
                return variadicLastChild;
            }
        }
    }

    public static PlanPiecePattern MATCH_NONE = new PlanPiecePattern() {
        @Override
        public boolean matchRoot(OptExpression op, MatchContext context) {
            return false;
        }

        @Override
        public PlanPiecePattern getChild(int i) {
            return MATCH_NONE;
        }
    };

    public static PlanPiecePattern MATCH_ANY = new PlanPiecePattern() {
        @Override
        public boolean matchRoot(OptExpression op, MatchContext context) {
            return true;
        }

        @Override
        public PlanPiecePattern getChild(int i) {
            return MATCH_ANY;
        }
    };

}
