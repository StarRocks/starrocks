// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner.stream;

import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TStreamAggregationNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

public class StreamAggNode extends PlanNode {

    private final AggregateInfo aggInfo;
    private IMTInfo detailImt;
    private IMTInfo aggImt;

    public StreamAggNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
        super(id, aggInfo.getOutputTupleId().asList(), "STREAM_AGG");
        this.aggInfo = aggInfo;
        this.children.add(input);
    }

    // TODO: set IMT for StreamAgg
    public void setDetailImt(IMTInfo imt) {
        this.detailImt = imt;
    }

    public void setAggImt(IMTInfo imt) {
        this.aggImt = imt;
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        if (CollectionUtils.isNotEmpty(aggInfo.getMaterializedAggregateExprs())) {
            output.append(detailPrefix).append("output: ").append(
                    getExplainString(aggInfo.getAggregateExprs())).append("\n");
        }
        output.append(detailPrefix).append("group_by: ").append(
                getExplainString(aggInfo.getGroupingExprs())).append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getExplainString(conjuncts)).append("\n");
        }

        if (detailLevel == TExplainLevel.VERBOSE) {
            if (detailImt != null) {
                output.append(detailPrefix).append("detail_imt: " + detailImt.toString());
            }
            if (aggImt != null) {
                output.append(detailPrefix).append("agg_imt: " + aggImt.toString());
            }
        }
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.STREAM_AGG_NODE;

        List<TExpr> aggregateFunctions = aggInfo.getMaterializedAggregateExprs().stream()
                .map(Expr::treeToThrift).collect(Collectors.toList());
        msg.stream_agg_node =
                new TStreamAggregationNode(
                        aggregateFunctions,
                        aggInfo.getIntermediateTupleId().asInt(),
                        aggInfo.getOutputTupleId().asInt());

        // Aggregate expression
        String sqlAggFunctions = aggInfo.getMaterializedAggregateExprs().stream().map(Expr::toSql)
                .collect(Collectors.joining(","));
        msg.stream_agg_node.setSql_aggregate_functions(sqlAggFunctions);

        // Grouping expression
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (CollectionUtils.isNotEmpty(groupingExprs)) {
            msg.stream_agg_node.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
        }
        String groupingStr = groupingExprs.stream().map(Expr::toSql).collect(Collectors.joining(", "));
        msg.stream_agg_node.setSql_grouping_keys(groupingStr);

        // TODO: add more functionalities

        msg.stream_agg_node.setAgg_func_set_version(3);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }
}
