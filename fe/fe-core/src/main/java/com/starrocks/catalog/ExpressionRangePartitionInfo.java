// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PartitionExprAnalyzer;
import com.starrocks.sql.ast.AstVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * ExpressionRangePartitionInfo replace columns with expressions
 * Some Descriptions:
 * 1. no overwrite old serialized method: read、write and readFields, because we use gson now
 */
@Deprecated
public class ExpressionRangePartitionInfo extends RangePartitionInfo {

    @SerializedName(value = "partitionExprs")
    private List<Expr> partitionExprs;

    public ExpressionRangePartitionInfo() {
        this.type = PartitionType.EXPR_RANGE;
    }

    public ExpressionRangePartitionInfo(List<Expr> partitionExprs, List<Column> columns, PartitionType type) {
        super(columns);
        Preconditions.checkState(partitionExprs != null);
        Preconditions.checkState(partitionExprs.size() > 0);
        Preconditions.checkState(columns != null);
        Preconditions.checkState(partitionExprs.size() == columns.size());
        this.partitionExprs = partitionExprs;
        this.isMultiColumnPartition = partitionExprs.size() > 0;
        this.type = type;
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

    public static PartitionInfo read(DataInput in) throws IOException {
        ExpressionRangePartitionInfo info = new ExpressionRangePartitionInfo();
        info.readFields(in);
        String json = Text.readString(in);
        List<Expr> exprs = GsonUtils.GSON.fromJson(json, new TypeToken<List<Expr>>(){}.getType());
        List<Column> partitionColumns = info.getPartitionColumns();
        for (Expr expr : exprs) {
            if (expr instanceof FunctionCallExpr) {
                SlotRef slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
<<<<<<< HEAD
                for (Column partitionColumn : partitionColumns) {
                    if (slotRef.getColumnName().equalsIgnoreCase(partitionColumn.getName())) {
                        PartitionExprAnalyzer.analyzePartitionExpr(expr, partitionColumn.getType());
                        slotRef.setType(partitionColumn.getType());
=======
                // TODO: Later, for automatically partitioned tables,
                //  partitions of materialized views (also created automatically),
                //  and partition by expr tables will use ExpressionRangePartitionInfoV2
                for (Column partitionColumn : partitionColumns) {
                    if (slotRef.getColumnName().equalsIgnoreCase(partitionColumn.getName())) {
                        slotRef.setType(partitionColumn.getType());
                        PartitionExprAnalyzer.analyzePartitionExpr(expr, slotRef);
>>>>>>> 2.5.18
                    }
                }
            }
        }
        info.setPartitionExprs(exprs);
        return info;
    }

    public void setPartitionExprs(List<Expr> partitionExprs) {
        this.partitionExprs = partitionExprs;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, GsonUtils.GSON.toJson(partitionExprs));
    }
}

