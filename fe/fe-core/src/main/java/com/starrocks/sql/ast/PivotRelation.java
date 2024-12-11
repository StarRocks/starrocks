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

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

public class PivotRelation extends Relation {

    @Nullable private Relation query;
    private final List<PivotAggregation> aggregateFunctions;
    private final List<SlotRef> pivotColumns;
    private final List<PivotValue> pivotValues;

    private List<Expr> groupByKeys;
    private List<FunctionCallExpr> rewrittenAggFunctions;

    public PivotRelation(
            Relation query,
            List<PivotAggregation> aggregateFunctions,
            List<SlotRef> pivotColumns,
            List<PivotValue> pivotValues) {
        this(query, aggregateFunctions, pivotColumns, pivotValues, NodePosition.ZERO);
    }

    public PivotRelation(
            Relation query,
            List<PivotAggregation> aggregateFunctions,
            List<SlotRef> pivotColumns,
            List<PivotValue> pivotValues,
            NodePosition pos) {
        super(pos);
        this.query = query;
        this.aggregateFunctions = aggregateFunctions;
        this.pivotColumns = pivotColumns;
        this.pivotValues = pivotValues;
        this.groupByKeys = new ArrayList<>();
        this.rewrittenAggFunctions = new ArrayList<>();
    }

    public List<PivotAggregation> getAggregateFunctions() {
        return aggregateFunctions;
    }

    public List<SlotRef> getPivotColumns() {
        return pivotColumns;
    }

    public List<PivotValue> getPivotValues() {
        return pivotValues;
    }

    @Nullable
    public Relation getQuery() {
        return query;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public void setQuery(@Nullable Relation query) {
        this.query = query;
    }

    public void addGroupByKey(Expr groupByKey) {
        this.groupByKeys.add(groupByKey);
    }

    public List<Expr> getGroupByKeys() {
        return groupByKeys;
    }

    public void addRewrittenAggFunction(FunctionCallExpr function) {
        rewrittenAggFunctions.add(function);
    }

    public List<FunctionCallExpr> getRewrittenAggFunctions() {
        return rewrittenAggFunctions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPivotRelation(this, context);
    }

    public Map<String, SlotRef> getUsedColumns() {
        Map<String, SlotRef> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        AstVisitor<Void, Void> collector = new AstVisitor<Void, Void>() {
            @Override
            public Void visitSlot(SlotRef node, Void context) {
                map.put(node.getColumnName(), node);
                return null;
            }
        };

        for (PivotAggregation agg : aggregateFunctions) {
            agg.getFunctionCallExpr().getChildren().forEach(child -> collector.visit(child, null));
        }

        for (SlotRef slot : pivotColumns) {
            collector.visit(slot, null);
        }

        return map;
    }
}
