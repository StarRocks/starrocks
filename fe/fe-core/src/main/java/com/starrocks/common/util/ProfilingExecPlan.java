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

package com.starrocks.common.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SortInfo;
import com.starrocks.common.Pair;
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.AnalyticEvalNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DataStreamSink;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.MultiCastDataSink;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SelectNode;
import com.starrocks.planner.SortNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

// This class is designed to maintain only the necessary information of ExecPlan for profile analysis
// Because, the frontend may cache at least 500 queries, it may lead to great memory cost if we simply cache the
// ExecPlan instance.
public class ProfilingExecPlan {

    private static final int MAX_EXPR_LENGTH = 64;
    private static final Gson GSON = new Gson();

    public static final class ProfilingFragment {
        private final ProfilingElement sink;
        private final ProfilingElement root;

        public ProfilingFragment(ProfilingElement sink, ProfilingElement root) {
            this.sink = sink;
            this.root = root;
        }

        public ProfilingElement getSink() {
            return sink;
        }

        public ProfilingElement getRoot() {
            return root;
        }
    }

    public static final class ProfilingElement {
        private final int id;
        private final Class<?> clazz;
        private final List<ProfilingElement> children = Lists.newArrayList();
        private final List<Integer> multiSinkIds = Lists.newArrayList();
        private final TreeSet<String> titleAttributes = Sets.newTreeSet();
        private final Map<String, String> uniqueInfos = Maps.newLinkedHashMap();

        private String displayName;
        private Statistics statistics;
        private CostEstimate costEstimate;
        private double totalCost;

        public ProfilingElement(int id, Class<?> clazz) {
            this.id = id;
            this.clazz = clazz;
        }

        public int getId() {
            return id;
        }

        public Class<?> getClazz() {
            return clazz;
        }

        public boolean instanceOf(Class<?> parent) {
            return parent.isAssignableFrom(clazz);
        }

        public boolean isFinalSink() {
            return instanceOf(DataSink.class) &&
                    !instanceOf(DataStreamSink.class) && !instanceOf(MultiCastDataSink.class);
        }

        public boolean hasChild(int i) {
            return i < children.size();
        }

        public ProfilingElement getChild(int i) {
            return children.get(i);
        }

        public List<ProfilingElement> getChildren() {
            return children;
        }

        public List<Integer> getMultiSinkIds() {
            return multiSinkIds;
        }

        public Set<String> getTitleAttributes() {
            return titleAttributes;
        }

        public void addTitleAttribute(String attribute) {
            titleAttributes.add(attribute);
        }

        public Map<String, String> getUniqueInfos() {
            return uniqueInfos;
        }

        public String getDisplayName() {
            return displayName;
        }

        public Statistics getStatistics() {
            return statistics;
        }

        public CostEstimate getCostEstimate() {
            return costEstimate;
        }

        public double getTotalCost() {
            return totalCost;
        }

        public boolean isMemoryConsumingOperator() {
            return instanceOf(AggregationNode.class) || instanceOf(JoinNode.class)
                    || instanceOf(SortNode.class) || instanceOf(AnalyticEvalNode.class)
                    || instanceOf(ExchangeNode.class) || instanceOf(ScanNode.class);
        }

        private void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        private void addChild(ProfilingElement node) {
            children.add(node);
        }

        private void addMultiSinkIds(List<Integer> ids) {
            multiSinkIds.addAll(ids);
        }

        private void addInfo(String name, String content) {
            uniqueInfos.put(name, content);
        }

        private void setEstimation(Statistics statistics, CostEstimate costEstimate, double totalCost) {
            this.statistics = statistics;
            this.costEstimate = costEstimate;
            this.totalCost = totalCost;
        }
    }

    public static ProfilingExecPlan buildFrom(ExecPlan execPlan) {
        if (execPlan == null) {
            return null;
        }
        ProfilingExecPlan profilingPlan = new ProfilingExecPlan();
        ConnectContext connectContext = execPlan.getConnectContext();
        if (connectContext != null) {
            SessionVariable sessionVariable = connectContext.getSessionVariable();
            profilingPlan.profileLevel = sessionVariable.getPipelineProfileLevel();
        }

        Map<Integer, ProfilingElement> visitedElements = Maps.newHashMap();
        for (PlanFragment fragment : execPlan.getFragments()) {
            DataSink sink = fragment.getSink();
            PlanNode planRoot = fragment.getPlanRoot();

            ProfilingFragment lightFragment = new ProfilingFragment(visitSink(sink),
                    visitNode(execPlan, planRoot, visitedElements));
            profilingPlan.addFragment(lightFragment);
        }

        return profilingPlan;
    }

    private static ProfilingElement visitSink(DataSink sink) {
        ProfilingElement element;
        if (sink instanceof MultiCastDataSink || sink instanceof ResultSink) {
            element = new ProfilingElement(-1, sink.getClass());
        } else {
            element = new ProfilingElement(sink.getExchNodeId() == null ? -1 : sink.getExchNodeId().asInt(),
                    sink.getClass());
        }

        DataPartition outputPartition = null;
        if (sink instanceof MultiCastDataSink) {
            List<DataStreamSink> sinks = ((MultiCastDataSink) sink).getDataStreamSinks();
            List<Integer> ids = Lists.newArrayList();
            if (CollectionUtils.isNotEmpty(sinks)) {
                sinks.forEach(s -> ids.add(s.getExchNodeId().asInt()));
                outputPartition = sinks.get(0).getOutputPartition();
            }
            element.addMultiSinkIds(ids);
        } else {
            outputPartition = sink.getOutputPartition();
        }
        if (outputPartition != null) {
            element.addInfo("PartitionType", outputPartition.getType().toString());
            if (CollectionUtils.isNotEmpty(outputPartition.getPartitionExprs())) {
                element.addInfo("PartitionExprs", exprsToString(outputPartition.getPartitionExprs()));
            }
        }

        if (sink instanceof ResultSink) {
            ResultSink resultSink = (ResultSink) sink;
            element.addInfo("SinkType", resultSink.getSinkType().toString());
        } else if (sink instanceof OlapTableSink) {
            OlapTableSink olapTableSink = (OlapTableSink) sink;
            element.addInfo("Table", olapTableSink.getDstTable().getName());
        }

        buildDisplayName(sink, element);

        return element;
    }

    private static ProfilingElement visitNode(ExecPlan execPlan, PlanNode node,
                                              Map<Integer, ProfilingElement> visitedElements) {
        if (visitedElements.containsKey(node.getId().asInt())) {
            return visitedElements.get(node.getId().asInt());
        }

        ProfilingElement element = new ProfilingElement(node.getId().asInt(), node.getClass());
        buildCostEstimation(execPlan, element);
        buildTitleAttribute(node, element);
        buildUniqueInfos(node, element);
        buildDisplayName(node, element);

        for (PlanNode child : node.getChildren()) {
            element.addChild(visitNode(execPlan, child, visitedElements));
        }

        visitedElements.put(element.getId(), element);
        return element;
    }

    private static void buildCostEstimation(ExecPlan execPlan, ProfilingElement element) {
        if (ConnectContext.get() == null) {
            return;
        }
        OptExpression optExpression = execPlan.getOptExpression(element.getId());
        if (optExpression != null) {
            CostEstimate cost = CostModel.calculateCostEstimate(new ExpressionContext(optExpression));
            element.setEstimation(optExpression.getStatistics(), cost, optExpression.getCost());
        }
    }

    private static void buildTitleAttribute(PlanNode node, ProfilingElement element) {
        if (node instanceof AggregationNode) {
            AggregationNode aggNode = (AggregationNode) node;
            element.addTitleAttribute(aggNode.getAggInfo().isMerge() ? "merge" : "update");
            element.addTitleAttribute(aggNode.isNeedsFinalize() ? "finalize" : "serialize");
        } else if (node instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) node;
            element.addTitleAttribute(joinNode.getJoinOp().toString());
            element.addTitleAttribute(joinNode.getDistrMode().toString());
        } else if (node instanceof SortNode) {
            SortNode sortNode = (SortNode) node;
            element.addTitleAttribute(sortNode.isUseTopN() ? "TOP-N" :
                    (sortNode.getSortInfo().getPartitionExprs().isEmpty() ? "SORT" : "PARTITION-TOP-N"));
            element.addTitleAttribute(sortNode.getTopNType().toString());
        } else if (node instanceof ExchangeNode) {
            ExchangeNode exchangeNode = (ExchangeNode) node;
            if (exchangeNode.getDistributionType() != null) {
                element.addTitleAttribute(exchangeNode.getDistributionType().toString());
            }
        }
    }

    private static void buildUniqueInfos(PlanNode node, ProfilingElement element) {
        if (node instanceof AggregationNode) {
            AggregationNode aggregationNode = (AggregationNode) node;
            AggregateInfo aggInfo = aggregationNode.getAggInfo();
            if (aggInfo == null) {
                return;
            }
            if (CollectionUtils.isNotEmpty(aggInfo.getAggregateExprs())) {
                element.addInfo("AggExprs", exprsToString(aggInfo.getAggregateExprs()));
            }
            if (CollectionUtils.isNotEmpty(aggInfo.getGroupingExprs())) {
                element.addInfo("GroupingExprs", exprsToString(aggInfo.getGroupingExprs()));
            }
        } else if (node instanceof AnalyticEvalNode) {
            AnalyticEvalNode window = (AnalyticEvalNode) node;
            if (CollectionUtils.isNotEmpty(window.getAnalyticFnCalls())) {
                element.addInfo("Functions", exprsToString(window.getAnalyticFnCalls()));
            }
            if (CollectionUtils.isNotEmpty(window.getPartitionExprs())) {
                element.addInfo("PartitionExprs", exprsToString(window.getPartitionExprs()));
            }
            if (CollectionUtils.isNotEmpty(window.getOrderByElements())) {
                element.addInfo("OrderByExprs", exprsToString(window.getOrderByElements().stream()
                        .map(OrderByElement::getExpr).collect(Collectors.toList())));
            }
        } else if (node instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) node;
            if (MapUtils.isNotEmpty(projectNode.getSlotMap())) {
                List<Pair<SlotId, Expr>> orderedExprs = new ArrayList<>();
                for (Map.Entry<SlotId, Expr> kv : projectNode.getSlotMap().entrySet()) {
                    orderedExprs.add(new Pair<>(kv.getKey(), kv.getValue()));
                }
                orderedExprs.sort(Comparator.comparingInt(o -> o.first.asInt()));
                element.addInfo("Expression",
                        exprsToString(orderedExprs.stream().map(kv -> kv.second).collect(Collectors.toList())));
            }

            if (MapUtils.isNotEmpty(projectNode.getCommonSlotMap())) {
                List<Pair<SlotId, Expr>> orderedExprs = new ArrayList<>();
                for (Map.Entry<SlotId, Expr> kv : projectNode.getCommonSlotMap().entrySet()) {
                    orderedExprs.add(new Pair<>(kv.getKey(), kv.getValue()));
                }
                orderedExprs.sort(Comparator.comparingInt(o -> o.first.asInt()));
                element.addInfo("CommonExpression",
                        exprsToString(orderedExprs.stream().map(kv -> kv.second).collect(Collectors.toList())));
            }
        } else if (node instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) node;
            if (CollectionUtils.isNotEmpty(joinNode.getEqJoinConjuncts())) {
                element.addInfo("EqJoinConjuncts", exprsToString(joinNode.getEqJoinConjuncts()));
            }
        } else if (node instanceof SelectNode) {
            SelectNode selectNode = (SelectNode) node;
            if (CollectionUtils.isNotEmpty(selectNode.getConjuncts())) {
                element.addInfo("Predicates", exprsToString(selectNode.getConjuncts()));
            }
        } else if (node instanceof SortNode) {
            SortNode sortNode = (SortNode) node;
            SortInfo sortInfo = sortNode.getSortInfo();
            if (CollectionUtils.isNotEmpty(sortInfo.getPartitionExprs())) {
                element.addInfo("PartitionExprs", exprsToString(sortInfo.getPartitionExprs()));
            }
            if (CollectionUtils.isNotEmpty(sortInfo.getOrderingExprs())) {
                element.addInfo("OrderByExprs", exprsToString(sortInfo.getOrderingExprs()));
            }
        } else if (node instanceof ScanNode) {
            ScanNode scanNode = (ScanNode) node;
            if (scanNode instanceof OlapScanNode) {
                OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                element.addInfo("Table: ", olapScanNode.getOlapTable().getName());
            }
        }
    }

    private static void buildDisplayName(Object node, ProfilingElement element) {
        String name = normalizeNodeName(node.getClass());
        if (node instanceof ExchangeNode) {
            ExchangeNode exchangeNode = (ExchangeNode) node;
            if (exchangeNode.isMerge()) {
                name = "MERGE_" + name;
            }
        }
        if (node instanceof SortNode) {
            SortNode sortNode = (SortNode) node;
            SortInfo sortInfo = sortNode.getSortInfo();
            if (sortInfo != null && CollectionUtils.isNotEmpty(sortInfo.getPartitionExprs())) {
                name = "PARTITION_TOP_N";
            } else if (sortNode.isUseTopN()) {
                name = "TOP_N";
            }
        }
        element.setDisplayName(name);
    }

    private static String normalizeNodeName(Class<?> clazz) {
        String name = clazz.getSimpleName();
        String suffixToRemove = "Node";
        if (name.endsWith(suffixToRemove)) {
            name = name.substring(0, name.length() - suffixToRemove.length());
        }

        StringBuilder normalizedName = new StringBuilder();

        for (int i = 0; i < name.length(); i++) {
            char currentChar = name.charAt(i);
            char nextChar = '\0';
            if (i + 1 < name.length()) {
                nextChar = name.charAt(i + 1);
            }

            if (Character.isUpperCase(currentChar) && Character.isLowerCase(nextChar) && i != 0) {
                normalizedName.append("_");
            }

            normalizedName.append(Character.toUpperCase(currentChar));
        }

        return normalizedName.toString();
    }

    private static String exprsToString(List<? extends Expr> exprs) {
        List<String> exprContents = exprs.stream()
                .map(Expr::toSql)
                .collect(Collectors.toList());
        int lastIndex = -1;
        int length = 0;
        int originalSize = exprs.size();
        for (int i = 0; i < originalSize; i++) {
            if (length + exprContents.get(i).length() > MAX_EXPR_LENGTH) {
                break;
            }
            lastIndex = i;
            length += exprContents.get(i).length();
        }
        if (lastIndex >= 0) {
            exprContents = exprContents.subList(0, lastIndex + 1);
        }
        if (lastIndex < originalSize - 1) {
            exprContents.add("...");
        }
        return "[" + String.join(", ", exprContents) + "]";
    }

    private int profileLevel;
    private final List<ProfilingFragment> fragments = new ArrayList<>();
    private volatile String topologyJson;

    public int getProfileLevel() {
        return profileLevel;
    }

    public List<ProfilingFragment> getFragments() {
        return fragments;
    }

    public String toTopologyJson() {
        if (topologyJson == null) {
            synchronized (this) {
                if (topologyJson == null) {
                    topologyJson = GSON.toJson(Topology.buildFrom(this));
                }
            }
        }
        return topologyJson;
    }

    private void addFragment(ProfilingFragment fragment) {
        fragments.add(fragment);
    }

    private static final class Topology {
        private final int rootId;
        private final List<Node> nodes = Lists.newArrayList();

        private Topology(int rootId) {
            this.rootId = rootId;
        }

        private static Topology buildFrom(ProfilingExecPlan plan) {
            ProfilingFragment rootFragment = plan.getFragments().get(0);
            Topology topology = new Topology(rootFragment.root.id);

            Queue<ProfilingElement> queue = Lists.newLinkedList();
            queue.offer(rootFragment.root);

            Map<Integer, ProfilingFragment> fragmentRootIdToFragment = plan.fragments.stream()
                    .collect(Collectors.toMap(fragment -> fragment.getRoot().getId(), Function.identity()));
            Set<Integer> visited = Sets.newHashSet();

            while (!queue.isEmpty()) {
                int cnt = queue.size();
                while (--cnt >= 0) {
                    ProfilingElement peek = queue.poll();
                    Preconditions.checkNotNull(peek);
                    if (!visited.add(peek.getId())) {
                        continue;
                    }
                    Node node = new Node(peek.getId(), peek.getDisplayName());
                    node.children.addAll(peek.getChildren().stream()
                            .map(ProfilingElement::getId).collect(Collectors.toList()));
                    if (fragmentRootIdToFragment.containsKey(node.id)) {
                        ProfilingFragment fragment = fragmentRootIdToFragment.get(node.id);
                        ProfilingElement sink = fragment.getSink();
                        List<Integer> sinkIds = Lists.newArrayList();
                        if (sink.instanceOf(MultiCastDataSink.class)) {
                            sinkIds.addAll(sink.getMultiSinkIds());
                        } else if (sink.getId() >= 0) {
                            sinkIds.add(sink.getId());
                        }
                        node.properties.put("sinkIds", sinkIds);
                    }
                    node.properties.put("displayMem", peek.isMemoryConsumingOperator());
                    topology.nodes.add(node);
                    queue.addAll(peek.getChildren());
                }
            }

            return topology;
        }

        private static final class Node {
            private final int id;
            private final String name;
            private final Map<String, Object> properties = Maps.newHashMap();
            private final List<Integer> children = Lists.newArrayList();

            public Node(int id, String name) {
                this.id = id;
                this.name = name;
            }
        }
    }
}
