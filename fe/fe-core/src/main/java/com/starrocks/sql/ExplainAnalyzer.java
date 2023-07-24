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

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SortInfo;
import com.starrocks.common.Pair;
import com.starrocks.common.util.Counter;
import com.starrocks.common.util.RuntimeProfile;
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
import com.starrocks.planner.UnionNode;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ExplainAnalyzer {
    private static final Logger LOG = LogManager.getLogger(ExplainAnalyzer.class);

    private static final int RESULT_SINK_PSEUDO_PLAN_NODE_ID = Integer.MAX_VALUE;
    private static final Pattern PLAN_NODE_ID = Pattern.compile("^.*?\\(.*?plan_node_id=([-0-9]+)\\)$");
    private static final int MAX_EXPR_LENGTH = 64;

    // Colors
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BOLD = "\u001B[1m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_SALMON = "\u001B[38;2;250;128;114m";
    public static final String ANSI_BLACK_ON_RED = "\u001B[41;30m";

    private static boolean isResultSink(RuntimeProfile operator) {
        return ("RESULT_SINK".equals(operator.getName())
                || "OLAP_TABLE_SINK".equals(operator.getName()));
    }

    private static int getPlanNodeId(RuntimeProfile operator) {
        if (isResultSink(operator)) {
            return RESULT_SINK_PSEUDO_PLAN_NODE_ID;
        }
        Matcher matcher = PLAN_NODE_ID.matcher(operator.getName());
        Preconditions.checkState(matcher.matches());
        return Integer.parseInt(matcher.group(1));
    }

    public static String analyze(ExecPlan execPlan, RuntimeProfile profile) {
        ExplainAnalyzer analyzer = new ExplainAnalyzer(execPlan, profile);
        return analyzer.analyze();
    }

    private enum GraphElement {
        LST_OPERATOR_INDENT("└──"),
        MIDDLE_OPERATOR_INDENT("├──"),

        LEAF_METRIC_INDENT("    "),
        NON_LEAF_METRIC_INDENT("│   "),

        LAST_CHILD_OPERATOR_INDENT("   "),
        MIDDLE_CHILD_OPERATOR_INDENT("│  ");

        GraphElement(String content) {
            this.content = content;
        }

        private final String content;
    }

    private final ExecPlan execPlan;
    private final RuntimeProfile summaryProfile;
    private final RuntimeProfile plannerProfile;
    private final RuntimeProfile executionProfile;
    private final StringBuilder summaryBuffer = new StringBuilder();
    private final StringBuilder detailBuffer = new StringBuilder();
    private final LinkedList<String> indents = Lists.newLinkedList();
    private final Map<Integer, NodeInfo> allNodeInfos = Maps.newHashMap();
    private final List<NodeInfo> allScanNodeInfos = Lists.newArrayList();
    private final List<NodeInfo> allExchangeNodeInfos = Lists.newArrayList();

    private String color = ANSI_RESET;

    private long cumulativeOperatorTime;

    private static String transformNodeName(Class<?> clazz) {
        String name = clazz.getSimpleName();
        String suffixToRemove = "Node";
        if (name.endsWith(suffixToRemove)) {
            name = name.substring(0, name.length() - suffixToRemove.length());
        }

        StringBuilder transformedName = new StringBuilder();

        for (int i = 0; i < name.length(); i++) {
            char currentChar = name.charAt(i);

            if (Character.isUpperCase(currentChar) && i != 0) {
                transformedName.append("_");
            }

            transformedName.append(Character.toUpperCase(currentChar));
        }

        return transformedName.toString();
    }

    public ExplainAnalyzer(ExecPlan execPlan, RuntimeProfile queryProfile) {
        this.execPlan = execPlan;
        this.summaryProfile = queryProfile.getChild("Summary");
        this.plannerProfile = queryProfile.getChild("Planner");
        this.executionProfile = queryProfile.getChild("Execution");
    }

    public String analyze() {
        if (summaryProfile == null || plannerProfile == null || executionProfile == null) {
            return null;
        }

        try {
            parseProfile();
            appendSummaryInfo();
            appendExecutionInfo();
            appendStatisticsSummaryInfo();
        } catch (Exception e) {
            LOG.error("Failed to analyze profiles", e);
            summaryBuffer.setLength(0);
            detailBuffer.setLength(0);
            indents.clear();
            appendSummaryLine("Failed to analyze profiles");
        }

        return summaryBuffer.toString() + detailBuffer;
    }

    private void parseProfile() {
        for (int i = 0; i < executionProfile.getChildList().size(); i++) {
            RuntimeProfile fragmentProfile = executionProfile.getChildList().get(i).first;

            ProfileNodeParser parser = new ProfileNodeParser(fragmentProfile);
            Map<Integer, NodeInfo> nodeInfos = parser.parse();

            for (Map.Entry<Integer, NodeInfo> entry : nodeInfos.entrySet()) {
                // Exchange can be located in different fragments, put them together
                if (allNodeInfos.containsKey(entry.getKey())) {
                    NodeInfo existsNodeInfo = allNodeInfos.get(entry.getKey());
                    existsNodeInfo.merge(entry.getValue());
                } else {
                    allNodeInfos.put(entry.getKey(), entry.getValue());
                }
            }
        }

        // Bind plan element
        for (PlanFragment fragment : execPlan.getFragments()) {
            DataSink sink = fragment.getSink();
            if (sink instanceof ResultSink || sink instanceof OlapTableSink) {
                NodeInfo resultNode = allNodeInfos.get(RESULT_SINK_PSEUDO_PLAN_NODE_ID);
                resultNode.planElement = sink;
            }

            PlanNode planNode = fragment.getPlanRoot();
            Queue<PlanNode> queue = Lists.newLinkedList();
            queue.offer(planNode);
            while (!queue.isEmpty()) {
                int num = queue.size();
                for (int i = 0; i < num; i++) {
                    PlanNode peek = queue.poll();
                    Preconditions.checkNotNull(peek);
                    NodeInfo nodeInfo = allNodeInfos.get(peek.getId().asInt());
                    Preconditions.checkNotNull(nodeInfo);
                    nodeInfo.planElement = peek;

                    for (PlanNode child : peek.getChildren()) {
                        queue.offer(child);
                    }
                }
            }
        }

        for (NodeInfo nodeInfo : allNodeInfos.values()) {
            if (nodeInfo.planElement instanceof ScanNode) {
                allScanNodeInfos.add(nodeInfo);
            } else if (nodeInfo.planElement instanceof ExchangeNode) {
                allExchangeNodeInfos.add(nodeInfo);
            }
        }
    }

    private void appendSummaryInfo() {
        Counter allNetworkTime = new Counter(TUnit.TIME_NS, null, 0);
        for (NodeInfo nodeInfo : allExchangeNodeInfos) {
            Counter networkTime = searchMaxMetric(nodeInfo, "UniqueMetrics", "NetworkTime");
            if (networkTime != null) {
                allNetworkTime.update(networkTime.getValue());
            }
        }

        Counter allScanTime = new Counter(TUnit.TIME_NS, null, 0);
        for (NodeInfo nodeInfo : allScanNodeInfos) {
            Counter scanTime = searchMaxMetric(nodeInfo, "UniqueMetrics", "ScanTime");
            if (scanTime != null) {
                allScanTime.update(scanTime.getValue());
            }
        }

        appendSummaryLine("Summary");

        pushIndent(GraphElement.LEAF_METRIC_INDENT);
        if (execPlan.getFragments().stream()
                .anyMatch(fragment -> fragment.getSink() instanceof OlapTableSink)) {
            appendSummaryLine("Attention: ", ANSI_BOLD + ANSI_BLACK_ON_RED,
                    "The transaction of the statement will be aborted, and no data will be actually inserted!!!",
                    ANSI_RESET);
        }

        appendSummaryLine("Version: ", summaryProfile.getInfoString("StarRocks Version"));
        appendSummaryLine("QueryId: ", summaryProfile.getInfoString("Query ID"));

        appendSummaryLine("TotalTime: ", summaryProfile.getInfoString("Total"));
        pushIndent(GraphElement.LEAF_METRIC_INDENT);
        Counter executionWallTime = executionProfile.getCounter("QueryExecutionWallTime");
        Counter resultDeliverTime = executionProfile.getCounter("ResultDeliverTime");
        appendSummaryLine("ExecutionTime: ", executionWallTime, " [",
                "Scan: ", allScanTime,
                String.format(" (%.2f%%)", (double) allScanTime.getValue() * 100 / executionWallTime.getValue()),
                ", Network: ", allNetworkTime,
                String.format(" (%.2f%%)", (double) allNetworkTime.getValue() * 100 / executionWallTime.getValue()),
                ", ResultDeliverTime: ", resultDeliverTime,
                String.format(" (%.2f%%)", (double) resultDeliverTime.getValue() * 100 / executionWallTime.getValue()),
                "]");

        appendSummaryLine("CollectProfileTime: ", summaryProfile.getInfoString("Collect Profile Time"));
        popIndent(); // metric indent

        appendSummaryLine("QueryPeakMemoryUsage: ", executionProfile.getCounter("QueryPeakMemoryUsage"),
                ", QueryAllocatedMemoryUsage: ", executionProfile.getCounter("QueryAllocatedMemoryUsage"));

        popIndent(); // metric indent

        cumulativeOperatorTime = executionProfile.getCounter("QueryCumulativeOperatorTime").getValue();
    }

    private void appendStatisticsSummaryInfo() {
        pushIndent(GraphElement.LEAF_METRIC_INDENT);

        appendCpuTopNodes();

        popIndent(); // metric indent
    }

    private void appendCpuTopNodes() {
        // TopCpus
        List<NodeInfo> topCpuNodes = Lists.newArrayList(allNodeInfos.values());
        topCpuNodes.sort((info1, info2) -> Long.compare(info2.totalTime.getValue(), info1.totalTime.getValue()));
        appendSummaryLine("Top Most Time-consuming Nodes:");
        pushIndent(GraphElement.LEAF_METRIC_INDENT);
        for (int i = 0; i < topCpuNodes.size(); i++) {
            if (i >= 10) {
                break;
            }
            NodeInfo nodeInfo = topCpuNodes.get(i);
            if (nodeInfo.totalTimePercentage > 30) {
                setRedColor();
            } else if (nodeInfo.totalTimePercentage > 15) {
                setSalmonColor();
            }
            appendSummaryLine(String.format("%d. ", i + 1), nodeInfo.getTitle(),
                    ": ", nodeInfo.totalTime, String.format(" (%.2f%%)", nodeInfo.totalTimePercentage));
            resetColor();
        }
        popIndent(); // metric indent
    }

    private void appendExecutionInfo() {
        for (int i = 0; i < execPlan.getFragments().size(); i++) {
            PlanFragment fragment = execPlan.getFragments().get(i);
            RuntimeProfile fragmentProfile = executionProfile.getChildList().get(i).first;
            appendFragment(fragment, fragmentProfile);
        }
    }

    private void appendFragment(PlanFragment fragment, RuntimeProfile fragmentProfile) {
        appendDetailLine(fragmentProfile.getName());
        pushIndent(GraphElement.NON_LEAF_METRIC_INDENT);
        appendDetailLine("BackendNum: ", fragmentProfile.getCounter("BackendNum"));
        appendDetailLine("InstancePeakMemoryUsage: ",
                fragmentProfile.getCounter("InstancePeakMemoryUsage"),
                ", InstanceAllocatedMemoryUsage: ", fragmentProfile.getCounter("InstanceAllocatedMemoryUsage"));
        appendDetailLine("PrepareTime: ", fragmentProfile.getCounter("FragmentInstancePrepareTime"));
        popIndent(); // metric indent

        DataSink sink = fragment.getSink();
        boolean isResultSink = sink instanceof ResultSink || sink instanceof OlapTableSink;
        if (isResultSink) {
            // Calculate result sink's time info, other sink's type will be properly processed
            // at the receiver side fragment through exchange node
            NodeInfo resultNodeInfo = allNodeInfos.get(RESULT_SINK_PSEUDO_PLAN_NODE_ID);
            resultNodeInfo.computeTimeUsage(cumulativeOperatorTime);
            if (resultNodeInfo.totalTimePercentage >= 30) {
                setRedColor();
            } else if (resultNodeInfo.totalTimePercentage >= 15) {
                setSalmonColor();
            }
        }
        DataPartition outputPartition = null;
        if (sink instanceof MultiCastDataSink) {
            List<DataStreamSink> sinks = ((MultiCastDataSink) sink).getDataStreamSinks();
            List<String> ids = Lists.newArrayList();
            if (CollectionUtils.isNotEmpty(sinks)) {
                sinks.forEach(s -> ids.add(Integer.toString(s.getExchNodeId().asInt())));
                outputPartition = sinks.get(0).getOutputPartition();
            }
            appendDetailLine(GraphElement.LST_OPERATOR_INDENT, transformNodeName(sink.getClass()),
                    String.format(" (ids=[%s])", String.join(", ", ids)));
        } else {
            outputPartition = sink.getOutputPartition();
            appendDetailLine(GraphElement.LST_OPERATOR_INDENT, transformNodeName(sink.getClass()),
                    isResultSink ? "" : String.format(" (id=%d)", sink.getExchNodeId().asInt()));
        }
        pushIndent(GraphElement.LAST_CHILD_OPERATOR_INDENT);
        pushIndent(GraphElement.NON_LEAF_METRIC_INDENT);
        if (isResultSink) {
            // Time Usage
            NodeInfo resultNodeInfo = allNodeInfos.get(RESULT_SINK_PSEUDO_PLAN_NODE_ID);
            List<Object> items = Lists.newArrayList();
            items.addAll(Arrays.asList("TotalTime: ", resultNodeInfo.totalTime,
                    String.format(" (%.2f%%)", resultNodeInfo.totalTimePercentage)));
            items.addAll(Arrays.asList(" [CPUTime: ", resultNodeInfo.cpuTime));
            items.add("]");
            appendDetailLine(items.toArray());

            // Output Rows
            Counter pullRows = searchMetricFromUpperLevel(resultNodeInfo, "CommonMetrics", "PushRowNum");
            Preconditions.checkNotNull(pullRows);
            resultNodeInfo.outputRowNums.setValue(pullRows.getValue());
            appendDetailLine("OutputRows: ", resultNodeInfo.outputRowNums);
        }
        if (outputPartition != null) {
            appendDetailLine("PartitionType: ", outputPartition.getType());
            if (CollectionUtils.isNotEmpty(outputPartition.getPartitionExprs())) {
                appendExprs("PartitionExprs", outputPartition.getPartitionExprs());
            }
        }
        if (sink instanceof ResultSink) {
            ResultSink resultSink = (ResultSink) sink;
            appendDetailLine("SinkType: ", resultSink.getSinkType());
        } else if (sink instanceof OlapTableSink) {
            OlapTableSink olapTableSink = (OlapTableSink) sink;
            appendDetailLine("Table: ", olapTableSink.getDstTable().getName());
        }
        if (isResultSink) {
            resetColor();
        }
        popIndent(); // metric indent

        leftOrderTraverse(fragment.getPlanRoot(), null, -1, null);

        popIndent(); // child operator indent

        appendDetailLine();
    }

    private void leftOrderTraverse(PlanNode cur, PlanNode parent, int index, String preTitleAttribute) {
        int planNodeId = cur.getId().asInt();
        NodeInfo nodeInfo = allNodeInfos.get(planNodeId);
        Preconditions.checkNotNull(nodeInfo);

        nodeInfo.computeTimeUsage(cumulativeOperatorTime);
        if (nodeInfo.totalTimePercentage >= 30) {
            setRedColor();
        } else if (nodeInfo.totalTimePercentage >= 15) {
            setSalmonColor();
        }

        boolean isMiddleChild = (parent != null && index < parent.getChildren().size() - 1);
        appendDetailLine(isMiddleChild ? GraphElement.MIDDLE_OPERATOR_INDENT : GraphElement.LST_OPERATOR_INDENT,
                preTitleAttribute == null ? "" : String.format("<%s> ", preTitleAttribute), nodeInfo.getTitle());

        if (isMiddleChild) {
            pushIndent(GraphElement.MIDDLE_CHILD_OPERATOR_INDENT);
        } else {
            pushIndent(GraphElement.LAST_CHILD_OPERATOR_INDENT);
        }

        boolean shouldTraverseChildren = cur.getChildren() != null
                && cur.getChildren().size() > 0
                && !(cur instanceof ExchangeNode);
        if (!shouldTraverseChildren) {
            pushIndent(GraphElement.LEAF_METRIC_INDENT);
        } else {
            pushIndent(GraphElement.NON_LEAF_METRIC_INDENT);
        }
        appendOperatorInfo(nodeInfo);
        popIndent(); // metric indent

        resetColor();

        if (shouldTraverseChildren) {
            for (int i = 0; i < cur.getChildren().size(); i++) {
                String childTitleAttribute = null;
                if (nodeInfo.planElement instanceof JoinNode) {
                    childTitleAttribute = (i == 0 ? "PROBE" : "BUILD");
                }
                leftOrderTraverse(cur.getChild(i), cur, i, childTitleAttribute);
            }
        }

        popIndent(); // child operator indent
    }

    private void appendOperatorInfo(NodeInfo nodeInfo) {
        // 1. Cost Estimation
        OptExpression optExpression = execPlan.getOptExpression(nodeInfo.planNodeId);
        if (optExpression != null) {
            CostEstimate cost = CostModel.calculateCostEstimate(new ExpressionContext(optExpression));
            if (optExpression.getStatistics().getColumnStatistics().values().stream()
                    .allMatch(ColumnStatistic::isUnknown)) {
                appendDetailLine("Estimates: [",
                        "row: ", (long) optExpression.getStatistics().getOutputRowCount(),
                        ", cpu: ?, memory: ?, network: ?, cost: ", optExpression.getCost(),
                        "]");
            } else {
                appendDetailLine("Estimates: [",
                        "row: ", (long) optExpression.getStatistics().getOutputRowCount(),
                        ", cpu: ", String.format("%.2f", cost.getCpuCost()),
                        ", memory: ", String.format("%.2f", cost.getMemoryCost()),
                        ", network: ", String.format("%.2f", cost.getNetworkCost()),
                        ", cost: ", String.format("%.2f", optExpression.getCost()),
                        "]");
            }
        } else {
            appendDetailLine("Estimates: [row: ?, cpu: ?, memory: ?, network: ?, cost: ?]");
        }

        // 2. Time Usage
        List<Object> items = Lists.newArrayList();
        items.addAll(Arrays.asList("TotalTime: ", nodeInfo.totalTime,
                String.format(" (%.2f%%)", nodeInfo.totalTimePercentage)));
        items.addAll(Arrays.asList(" [CPUTime: ", nodeInfo.cpuTime));
        if (nodeInfo.planElement instanceof ExchangeNode) {
            if (nodeInfo.networkTime != null) {
                items.addAll(Arrays.asList(", NetworkTime: ", nodeInfo.networkTime));
            }
        } else if (nodeInfo.planElement instanceof ScanNode) {
            if (nodeInfo.scanTime != null) {
                items.addAll(Arrays.asList(", ScanTime: ", nodeInfo.scanTime));
            }
        }
        items.add("]");
        appendDetailLine(items.toArray());

        // 3. Output Rows
        Counter pullRows;
        if (nodeInfo.planElement instanceof UnionNode) {
            pullRows = sumUpMetric(nodeInfo, false, false, "CommonMetrics", "PullRowNum");
        } else {
            pullRows = searchMetricFromUpperLevel(nodeInfo, "CommonMetrics", "PullRowNum");
        }
        Preconditions.checkNotNull(pullRows);
        nodeInfo.outputRowNums.setValue(pullRows.getValue());
        appendDetailLine("OutputRows: ", nodeInfo.outputRowNums);

        // 4. Memory Infos
        if (nodeInfo.planElement instanceof AggregationNode || nodeInfo.planElement instanceof JoinNode
                || nodeInfo.planElement instanceof AnalyticEvalNode) {
            appendDetailLine("PeakMemory: ",
                    sumUpMetric(nodeInfo, false, true, "CommonMetrics", "OperatorPeakMemoryUsage"),
                    ", AllocatedMemory: ",
                    sumUpMetric(nodeInfo, false, false, "CommonMetrics", "OperatorAllocatedMemoryUsage"));
        }

        // 5. Runtime Filters
        Counter rfInputRows = searchMetricFromUpperLevel(nodeInfo, "CommonMetrics", "JoinRuntimeFilterInputRows");
        Counter rfOutputRows = searchMetricFromUpperLevel(nodeInfo, "CommonMetrics", "JoinRuntimeFilterOutputRows");
        if (rfInputRows != null && rfOutputRows != null && rfInputRows.getValue() > 0) {
            appendDetailLine("RuntimeFilter: ",
                    rfInputRows, " -> ", rfOutputRows,
                    String.format(" (%.2f%%)", 100 * (rfInputRows.getValue() - rfOutputRows.getValue()) /
                            (double) rfInputRows.getValue()));
        }

        // 6. Unique Infos
        appendOperatorUniqueInfo(nodeInfo);
    }

    private void appendOperatorUniqueInfo(NodeInfo nodeInfo) {
        if (nodeInfo.planElement instanceof AggregationNode) {
            AggregationNode aggregationNode = (AggregationNode) nodeInfo.planElement;
            AggregateInfo aggInfo = aggregationNode.getAggInfo();
            if (aggInfo == null) {
                return;
            }
            if (CollectionUtils.isNotEmpty(aggInfo.getAggregateExprs())) {
                appendExprs("AggExprs", aggInfo.getAggregateExprs());
            }
            if (CollectionUtils.isNotEmpty(aggInfo.getGroupingExprs())) {
                appendExprs("GroupingExprs", aggInfo.getGroupingExprs());
            }
        } else if (nodeInfo.planElement instanceof AnalyticEvalNode) {
            AnalyticEvalNode window = (AnalyticEvalNode) nodeInfo.planElement;
            if (CollectionUtils.isNotEmpty(window.getAnalyticFnCalls())) {
                appendExprs("Functions", window.getAnalyticFnCalls());
            }
            if (CollectionUtils.isNotEmpty(window.getPartitionExprs())) {
                appendExprs("PartitionExprs", window.getPartitionExprs());
            }
            if (CollectionUtils.isNotEmpty(window.getOrderByElements())) {
                appendExprs("OrderByExprs", window.getOrderByElements().stream()
                        .map(OrderByElement::getExpr).collect(Collectors.toList()));
            }
        } else if (nodeInfo.planElement instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) nodeInfo.planElement;
            if (MapUtils.isNotEmpty(projectNode.getSlotMap())) {
                List<Pair<SlotId, Expr>> orderedExprs = new ArrayList<>();
                for (Map.Entry<SlotId, Expr> kv : projectNode.getSlotMap().entrySet()) {
                    orderedExprs.add(new Pair<>(kv.getKey(), kv.getValue()));
                }
                orderedExprs.sort(Comparator.comparingInt(o -> o.first.asInt()));
                appendExprs("Expression",
                        orderedExprs.stream().map(kv -> kv.second).collect(Collectors.toList()));
            }

            if (MapUtils.isNotEmpty(projectNode.getCommonSlotMap())) {
                List<Pair<SlotId, Expr>> orderedExprs = new ArrayList<>();
                for (Map.Entry<SlotId, Expr> kv : projectNode.getCommonSlotMap().entrySet()) {
                    orderedExprs.add(new Pair<>(kv.getKey(), kv.getValue()));
                }
                orderedExprs.sort(Comparator.comparingInt(o -> o.first.asInt()));
                appendExprs("CommonExpression",
                        orderedExprs.stream().map(kv -> kv.second).collect(Collectors.toList()));
            }
        } else if (nodeInfo.planElement instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) nodeInfo.planElement;
            Counter buildTime =
                    searchMaxMetricByNamePattern(nodeInfo, "_JOIN_BUILD (", "CommonMetrics", "OperatorTotalTime");
            Counter probeTime =
                    searchMaxMetricByNamePattern(nodeInfo, "_JOIN_PROBE (", "CommonMetrics", "OperatorTotalTime");
            appendDetailLine("BuildTime: ", buildTime);
            appendDetailLine("ProbeTime: ", probeTime);
            if (CollectionUtils.isNotEmpty(joinNode.getEqJoinConjuncts())) {
                appendExprs("EqJoinConjuncts", joinNode.getEqJoinConjuncts());
            }
        } else if (nodeInfo.planElement instanceof SelectNode) {
            SelectNode selectNode = (SelectNode) nodeInfo.planElement;
            if (CollectionUtils.isNotEmpty(selectNode.getConjuncts())) {
                appendExprs("Predicates", selectNode.getConjuncts());
            }
        } else if (nodeInfo.planElement instanceof SortNode) {
            SortNode sortNode = (SortNode) nodeInfo.planElement;
            SortInfo sortInfo = sortNode.getSortInfo();
            if (CollectionUtils.isNotEmpty(sortInfo.getPartitionExprs())) {
                appendExprs("PartitionExprs", sortInfo.getPartitionExprs());
            }
            if (CollectionUtils.isNotEmpty(sortInfo.getOrderingExprs())) {
                appendExprs("OrderByExprs", sortInfo.getOrderingExprs());
            }
        } else if (nodeInfo.planElement instanceof ScanNode) {
            ScanNode scanNode = (ScanNode) nodeInfo.planElement;
            if (scanNode instanceof OlapScanNode) {
                OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                appendDetailLine("Table: ", olapScanNode.getOlapTable().getName());
            }
        }
    }

    private void appendExprs(String metricName, List<? extends Expr> exprs) {
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
        appendDetailLine(metricName, ": [", String.join(", ", exprContents), "]");
    }

    private static Counter searchMaxMetric(NodeInfo nodeInfo, String... nameLevels) {
        for (RuntimeProfile operatorProfile : nodeInfo.operatorProfiles) {
            RuntimeProfile cur = getLastLevel(operatorProfile, nameLevels);
            int lastIndex = nameLevels.length - 1;
            Counter counter = cur.getCounter(RuntimeProfile.MERGED_INFO_PREFIX_MAX + nameLevels[lastIndex]);
            if (counter == null) {
                counter = cur.getCounter(nameLevels[lastIndex]);
            }

            if (counter != null) {
                return counter;
            }
        }

        return null;
    }

    private static Counter searchMaxMetricByNamePattern(NodeInfo nodeInfo, String pattern, String... nameLevels) {
        for (RuntimeProfile operatorProfile : nodeInfo.operatorProfiles) {
            if (!operatorProfile.getName().contains(pattern)) {
                continue;
            }
            RuntimeProfile cur = getLastLevel(operatorProfile, nameLevels);
            int lastIndex = nameLevels.length - 1;
            Counter counter = cur.getCounter(RuntimeProfile.MERGED_INFO_PREFIX_MAX + nameLevels[lastIndex]);
            if (counter == null) {
                counter = cur.getCounter(nameLevels[lastIndex]);
            }

            if (counter != null) {
                return counter;
            }
        }

        return null;
    }

    // We must get some metrics, such as output rows, from the upper level operator,
    // For example, we have an operator group including AGGREGATE_BLOCKING_SINK and AGGREGATE_BLOCKING_SOURCE
    // And the order between these operators are not stable, and we need to get the output rows information
    // form the AGGREGATE_BLOCKING_SOURCE, rather than AGGREGATE_BLOCKING_SINK
    private static Counter searchMetricFromUpperLevel(NodeInfo nodeInfo, String... nameLevels) {
        for (int i = 0; i < nodeInfo.operatorProfiles.size(); i++) {
            RuntimeProfile operatorProfile = nodeInfo.operatorProfiles.get(i);
            if (i < nodeInfo.operatorProfiles.size() - 1
                    && (operatorProfile.getName().contains("CHUNK_ACCUMULATE (")
                    || operatorProfile.getName().contains("_SINK (")
                    || operatorProfile.getName().contains("_PREPARE ("))) {
                continue;
            }
            RuntimeProfile cur = getLastLevel(operatorProfile, nameLevels);
            int lastIndex = nameLevels.length - 1;
            return cur.getCounter(nameLevels[lastIndex]);
        }
        return null;
    }

    private static Counter sumUpMetric(NodeInfo nodeInfo, boolean searchPseudoOperator, boolean useMaxValue,
                                       String... nameLevels) {
        Counter counterSumUp = null;
        List<RuntimeProfile> operatorProfiles = Lists.newArrayList(nodeInfo.operatorProfiles);
        if (searchPseudoOperator) {
            operatorProfiles.addAll(nodeInfo.pseudoOperatorProfiles);
        }
        for (RuntimeProfile operatorProfile : operatorProfiles) {
            RuntimeProfile cur = getLastLevel(operatorProfile, nameLevels);
            int lastIndex = nameLevels.length - 1;
            Counter counter = null;
            if (useMaxValue) {
                counter = cur.getCounter(RuntimeProfile.MERGED_INFO_PREFIX_MAX + nameLevels[lastIndex]);
            }
            if (counter == null) {
                counter = cur.getCounter(nameLevels[lastIndex]);
            }
            if (counter == null) {
                continue;
            }
            if (counterSumUp == null) {
                counterSumUp = new Counter(counter.getType(), counter.getStrategy(), 0);
            }

            counterSumUp.setValue(counterSumUp.getValue() + counter.getValue());
        }

        return counterSumUp;
    }

    private static RuntimeProfile getLastLevel(RuntimeProfile operatorProfile, String... nameLevels) {
        RuntimeProfile cur = operatorProfile;
        for (int i = 0; i < nameLevels.length - 1; i++) {
            cur = cur.getChild(nameLevels[i]);
        }
        return cur;
    }

    private void appendSummaryLine(Object... contents) {
        appendLine(summaryBuffer, contents);
    }

    private void appendDetailLine(Object... contents) {
        appendLine(detailBuffer, contents);
    }

    private void appendLine(StringBuilder buffer, Object... contents) {
        Iterator<String> iterator = indents.descendingIterator();
        while (iterator.hasNext()) {
            buffer.append(iterator.next());
        }
        boolean isColorAppended = false;
        for (Object content : contents) {
            if (!isColorAppended && !(content instanceof GraphElement)) {
                buffer.append(color);
                isColorAppended = true;
            }

            if (content instanceof GraphElement) {
                buffer.append(((GraphElement) content).content);
            } else if (content instanceof Counter) {
                buffer.append(RuntimeProfile.printCounter((Counter) content));
            } else if (content instanceof Supplier) {
                buffer.append(((Supplier<?>) content).get());
            } else {
                buffer.append(content);
            }
        }
        buffer.append(ANSI_RESET);
        buffer.append('\n');
    }

    private void pushIndent(GraphElement indent) {
        indents.push(indent.content);
    }

    private void popIndent() {
        indents.pop();
    }

    private void setRedColor() {
        color = ANSI_BOLD + ANSI_RED;
    }

    private void setSalmonColor() {
        color = ANSI_BOLD + ANSI_SALMON;
    }

    private void resetColor() {
        color = ANSI_RESET;
    }

    // This structure is designed to hold all the information of a node, including plan stage information
    // as well as runtime stage information.
    private static final class NodeInfo {
        private final int planNodeId;

        // One node in plan may be decomposed into multiply pipeline operators
        // like `aggregate -> (aggregate_sink_operator, aggregate_source_operator)`
        // as well as some pseudo operators, including local_exchange_operator and chunk_accumulate etc.
        private final List<RuntimeProfile> operatorProfiles;
        private final List<RuntimeProfile> pseudoOperatorProfiles;

        private Object planElement;
        private Counter totalTime;
        private Counter cpuTime;
        private Counter networkTime;
        private Counter scanTime;
        private final Counter outputRowNums = new Counter(TUnit.UNIT, null, 0);
        private double totalTimePercentage;

        public NodeInfo(int planNodeId, List<RuntimeProfile> operatorProfiles,
                        List<RuntimeProfile> pseudoOperatorProfiles) {
            this.planNodeId = planNodeId;
            this.operatorProfiles = operatorProfiles;
            this.pseudoOperatorProfiles = pseudoOperatorProfiles;
        }

        public void merge(NodeInfo other) {
            this.operatorProfiles.addAll(other.operatorProfiles);
            this.pseudoOperatorProfiles.addAll(other.operatorProfiles);
        }

        public void computeTimeUsage(long cumulativeOperatorTime) {
            totalTime = new Counter(TUnit.TIME_NS, null, 0);
            cpuTime = sumUpMetric(this, true, true, "CommonMetrics", "OperatorTotalTime");
            Preconditions.checkNotNull(cpuTime);
            totalTime.update(cpuTime.getValue());
            if (planElement instanceof ExchangeNode) {
                networkTime = searchMaxMetric(this, "UniqueMetrics", "NetworkTime");
                if (networkTime != null) {
                    totalTime.update(networkTime.getValue());
                }
            } else if (planElement instanceof ScanNode) {
                scanTime = searchMaxMetric(this, "UniqueMetrics", "ScanTime");
                if (scanTime != null) {
                    totalTime.update(scanTime.getValue());
                }
            }
            totalTimePercentage = (totalTime.getValue() * 100D / cumulativeOperatorTime);
        }

        public String getTitle() {
            StringBuilder titleBuilder = new StringBuilder();
            titleBuilder.append(transformNodeName(planElement.getClass()));
            if (planElement instanceof ResultSink || planElement instanceof OlapTableSink) {
                return titleBuilder.toString();
            }
            titleBuilder.append(String.format(" (id=%d) ", planNodeId));
            // Attributes
            List<String> attributes = Lists.newArrayList();
            if (planElement instanceof AggregationNode) {
                AggregationNode aggNode = (AggregationNode) planElement;
                attributes.add(aggNode.getAggInfo().isMerge() ? "merge" : "update");
                attributes.add(aggNode.isNeedsFinalize() ? "finalize" : "serialize");
            } else if (planElement instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) planElement;
                attributes.add(joinNode.getJoinOp().toString());
                attributes.add(joinNode.getDistrMode().toString());
            } else if (planElement instanceof SortNode) {
                SortNode sortNode = (SortNode) planElement;
                attributes.add(sortNode.isUseTopN() ? "TOP-N" :
                        (sortNode.getSortInfo().getPartitionExprs().isEmpty() ? "SORT" : "PARTITION-TOP-N"));
                attributes.add(sortNode.getTopNType().toString());
            } else if (planElement instanceof ExchangeNode) {
                ExchangeNode exchangeNode = (ExchangeNode) planElement;
                if (exchangeNode.getDistributionType() != null) {
                    attributes.add(exchangeNode.getDistributionType().toString());
                }
            }
            if (CollectionUtils.isNotEmpty(attributes)) {
                titleBuilder.append('[')
                        .append(String.join(", ", attributes))
                        .append(']');
            }
            return titleBuilder.toString();
        }
    }

    // One node in plan may correspond to multiply pipeline operators,
    // This class is designed to collect all the pipeline operators that should belong to one plan node
    private static final class ProfileNodeParser {
        private final List<RuntimeProfile> pipelineProfiles;
        private int pipelineIdx;
        private int operatorIdx;

        public ProfileNodeParser(RuntimeProfile fragmentProfile) {
            this.pipelineProfiles = fragmentProfile.getChildList().stream()
                    .map(pair -> pair.first)
                    .collect(Collectors.toList());
            pipelineIdx = 0;
            operatorIdx = 0;
        }

        public Map<Integer, NodeInfo> parse() {
            Map<Integer, NodeInfo> nodes = Maps.newHashMap();
            while (hasNext()) {
                List<RuntimeProfile> operatorProfiles = Lists.newArrayList();
                List<RuntimeProfile> pseudoOperatorProfiles = Lists.newArrayList();

                RuntimeProfile nextOperator = nextOperator();
                while (hasNext() && getPlanNodeId(nextOperator) < 0) {
                    pseudoOperatorProfiles.add(nextOperator);
                    moveForward();
                    nextOperator = nextOperator();
                }

                // Find next operator with the different plan node id, and all the auxiliary
                // operators along the path belong to the current plan node id.
                int planNodeId = getPlanNodeId(nextOperator);
                Preconditions.checkState(planNodeId >= 0);
                operatorProfiles.add(nextOperator);
                moveForward();

                while (hasNext()) {
                    nextOperator = nextOperator();
                    int nextPlanNodeId = getPlanNodeId(nextOperator);
                    if (nextPlanNodeId >= 0 && (nextPlanNodeId != planNodeId)) {
                        break;
                    }

                    // Now operator must share the same plan node id or is the auxiliary operator
                    if (nextPlanNodeId >= 0) {
                        operatorProfiles.add(nextOperator);
                    } else {
                        pseudoOperatorProfiles.add(nextOperator);
                    }
                    moveForward();
                }

                NodeInfo node = new NodeInfo(planNodeId, operatorProfiles, pseudoOperatorProfiles);
                if (nodes.containsKey(planNodeId)) {
                    NodeInfo existingNode = nodes.get(planNodeId);
                    existingNode.merge(node);
                } else {
                    nodes.put(planNodeId, node);
                }
            }

            return nodes;
        }

        private boolean hasNext() {
            return pipelineIdx < pipelineProfiles.size();
        }

        private RuntimeProfile nextOperator() {
            RuntimeProfile pipeline = pipelineProfiles.get(pipelineIdx);
            return pipeline.getChildList().get(operatorIdx).first;
        }

        private void moveForward() {
            RuntimeProfile pipeline = pipelineProfiles.get(pipelineIdx);
            if (operatorIdx < pipeline.getChildList().size() - 1) {
                operatorIdx++;
            } else {
                pipelineIdx++;
                operatorIdx = 0;
            }
        }
    }
}
