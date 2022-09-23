// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner.stream;

import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
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
    protected String getNodeVerboseExplain(String detailPrefix) {
        StringBuilder output = new StringBuilder();
        if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
            output.append(detailPrefix).append("aggregate: ").append(
                    getVerboseExplain(aggInfo.getAggregateExprs())).append("\n");
        }
        if (aggInfo.getGroupingExprs() != null && aggInfo.getGroupingExprs().size() > 0) {
            output.append(detailPrefix).append("group by: ").append(
                    getVerboseExplain(aggInfo.getGroupingExprs())).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getVerboseExplain(conjuncts)).append("\n");
        }
        return output.toString();
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }
}
