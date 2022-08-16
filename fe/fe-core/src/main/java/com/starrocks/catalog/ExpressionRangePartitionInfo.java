// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.AstVisitor;

import java.util.List;

import static java.util.stream.Collectors.toList;

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
            for (Expr expr : partitionExprs) {
                if (expr instanceof SlotRef) {
                    SlotRef slotRef = (SlotRef) expr.clone();
                    sb.append("`").append(slotRef.getColumnName()).append("`").append(",");
                }
                if (expr instanceof FunctionCallExpr) {
                    Expr cloneExpr = expr.clone();
                    for (int i = 0; i < cloneExpr.getChildren().size(); i++) {
                        Expr child = cloneExpr.getChildren().get(i);
                        if (child instanceof SlotRef) {
                            cloneExpr.setChild(i, new SlotRef(null, ((SlotRef) child).getColumnName()));
                            break;
                        }
                    }
                    sb.append(cloneExpr.toSql()).append(",");
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

    public void renameTableName(String newTableName) {
        AstVisitor<Void, Void> renameVisitor = new AstVisitor<Void, Void>() {
            @Override
            public Void visitExpression(Expr expr, Void context) {
                for (Expr child : expr.getChildren()) {
                    child.accept(this, context);
                }
                return null;
            }

            @Override
            public Void visitSlot(SlotRef node, Void context) {
                TableName tableName = node.getTblNameWithoutAnalyzed();
                if (tableName != null) {
                    tableName.setTbl(newTableName);
                }
                return null;
            }
        };
        for (Expr expr : partitionExprs) {
            expr.accept(renameVisitor, null);
        }
    }
}

