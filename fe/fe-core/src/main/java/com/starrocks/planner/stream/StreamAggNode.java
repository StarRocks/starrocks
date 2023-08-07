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


package com.starrocks.planner.stream;

import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.FragmentNormalizer;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.sql.optimizer.operator.stream.IMTInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TNormalSortAggregationNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TStreamAggregationNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class StreamAggNode extends PlanNode {
    private final AggregateInfo aggInfo;
    private IMTInfo detailImt;
    private IMTInfo aggImt;

    public StreamAggNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
        super(id, aggInfo.getOutputTupleId().asList(), "StreamAgg");
        this.aggInfo = aggInfo;
        this.children.add(input);
    }

    // TODO: set IMT for StreamAgg
    public void setDetailImt(IMTInfo imt) { this.detailImt = imt; }

    public void setAggImt(IMTInfo imt) { this.aggImt = imt; }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        if (CollectionUtils.isNotEmpty(aggInfo.getMaterializedAggregateExprs())) {
            output.append(detailPrefix)
                    .append("output: ")
                    .append(getExplainString(aggInfo.getAggregateExprs()))
                    .append("\n");
        }
        output.append(detailPrefix)
                .append("group_by: ")
                .append(getExplainString(aggInfo.getGroupingExprs()))
                .append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getExplainString(conjuncts)).append("\n");
        }

        if (detailLevel == TExplainLevel.VERBOSE) {
            if (detailImt != null) {
                output.append(detailPrefix).append("detail_imt: ").append(detailImt.toString()).append("\n");
            }
            if (aggImt != null) {
                output.append(detailPrefix).append("agg_imt: ").append(aggImt.toString()).append("\n");
            }
        }
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.STREAM_AGG_NODE;

        List<TExpr> aggregateFunctions =
                aggInfo.getMaterializedAggregateExprs().stream().map(Expr::treeToThrift).collect(Collectors.toList());
        msg.stream_agg_node = new TStreamAggregationNode();
        msg.stream_agg_node.setAggregate_functions(aggregateFunctions);

        // Aggregate expression
        String sqlAggFunctions =
                aggInfo.getMaterializedAggregateExprs().stream().map(Expr::toSql).collect(Collectors.joining(","));
        msg.stream_agg_node.setSql_aggregate_functions(sqlAggFunctions);

        // Grouping expression
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (CollectionUtils.isNotEmpty(groupingExprs)) {
            msg.stream_agg_node.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
        }
        String groupingStr = groupingExprs.stream().map(Expr::toSql).collect(Collectors.joining(", "));
        msg.stream_agg_node.setSql_grouping_keys(groupingStr);

        msg.stream_agg_node.setAgg_func_set_version(3);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean extractConjunctsToNormalize(FragmentNormalizer normalizer) {
        List<Expr> conjuncts = normalizer.getConjunctsByPlanNodeId(this);
        normalizer.filterOutPartColRangePredicates(getId(), conjuncts,
                FragmentNormalizer.getSlotIdSet(aggInfo.getGroupingExprs()));
        return true;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalSortAggregationNode sortAggregationNode = new TNormalSortAggregationNode();
        sortAggregationNode.setGrouping_exprs(normalizer.normalizeExprs(aggInfo.getGroupingExprs()));
        sortAggregationNode.setAggregate_functions(
                normalizer.normalizeExprs(new ArrayList<>(aggInfo.getAggregateExprs())));
        sortAggregationNode.setAgg_func_set_version(FeConstants.AGG_FUNC_VERSION);
        planNode.setSort_aggregation_node(sortAggregationNode);
        planNode.setNode_type(TPlanNodeType.STREAM_AGG_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }
}
