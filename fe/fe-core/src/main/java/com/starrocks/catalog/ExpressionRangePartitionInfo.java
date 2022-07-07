// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * ExpressionRangePartitionInfo replace columns with expressions
 * Some Descriptions:
 * 1. no overwrite old serialized method: read、write and readFields, because we use gson now
 */
public class ExpressionRangePartitionInfo extends RangePartitionInfo {

    @SerializedName(value = "partitionExprs")
    private final List<Expr> partitionExprs;

    public ExpressionRangePartitionInfo(List<Expr> partitionExprs, List<Column> columns) {
        super(columns);
        Preconditions.checkState(partitionExprs != null);
        Preconditions.checkState(partitionExprs.size() > 0);
        Preconditions.checkState(columns != null);
        Preconditions.checkState(partitionExprs.size() == columns.size());
        this.partitionExprs = partitionExprs;
        this.isMultiColumnPartition = partitionExprs.size() > 0;
    }


    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY ");
        if (table instanceof MaterializedView) {
            sb.append("(");
            MaterializedView mv = (MaterializedView) table;
            Map<Expr, String> exprPartitionAliasMap = mv.getExprPartitionAliasMap();
            for (Expr expr : partitionExprs) {
                String alias = exprPartitionAliasMap.get(expr);
                if (expr instanceof SlotRef) {
                    sb.append("`").append(alias).append("`").append(",");
                }
                if (expr instanceof FunctionCallExpr) {
                    QueryStatement queryStmt = mv.getQueryStmt();
                    SelectRelation selectRelation = (SelectRelation) queryStmt.getQueryRelation();
                    Map<String, Expr> selectExprMap =
                            selectRelation.getSelectList().getItems().stream().filter(item -> item.getAlias() != null)
                                    .collect(toMap(SelectListItem::getAlias, SelectListItem::getExpr));
                    // find base table expression
                    if (selectExprMap.containsKey(alias)) {
                        Expr baseTableExpr = selectExprMap.get(alias);
                        if (baseTableExpr instanceof SlotRef) {
                            Expr cloneExpr = expr.clone();
                            SlotRef slotRefAlias = new SlotRef(null, alias);
                            for (int i = 0; i < cloneExpr.getChildren().size(); i++) {
                                Expr child = cloneExpr.getChildren().get(i);
                                if (child instanceof SlotRef) {
                                    cloneExpr.setChild(i, slotRefAlias);
                                    break;
                                }
                            }
                            sb.append(cloneExpr.toSql()).append(",");
                        }
                        if (baseTableExpr instanceof FunctionCallExpr) {
                            sb.append("`").append(alias).append("`").append(",");
                        }
                    }
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        }
        sb.append("RANGE(");
        sb.append(Joiner.on(", ").join(partitionExprs.stream().map(Expr::toSql).collect(toList()))).append(")");
        // in the future maybe need range partition info
        return sb.toString();
    }
}

