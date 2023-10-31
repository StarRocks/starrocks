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

package com.starrocks.catalog;


import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PartitionExprAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.SqlParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * ExpressionRangePartitionInfo replace columns with expressions
 * Some Descriptions:
 * 1. no overwrite old serialized method: read、write and readFields, because we use gson now
 * 2. As of 2023-09, it's still used to describe auto range using expr like PARTITION BY date_trunc('day', col).
 */
@Deprecated
public class ExpressionRangePartitionInfo extends RangePartitionInfo implements GsonPreProcessable, GsonPostProcessable {
    public static final String AUTOMATIC_SHADOW_PARTITION_NAME = "$shadow_automatic_partition";
    public static final String SHADOW_PARTITION_PREFIX = "$";

    private List<Expr> partitionExprs;

    @SerializedName(value = "partitionExprs")
    private List<GsonUtils.ExpressionSerializedObject> serializedPartitionExprs;

    public ExpressionRangePartitionInfo() {
        this.type = PartitionType.EXPR_RANGE;
    }

    @Override
    public void gsonPreProcess() throws IOException {
        super.gsonPreProcess();
        List<GsonUtils.ExpressionSerializedObject> serializedPartitionExprs = Lists.newArrayList();
        for (Expr partitionExpr : partitionExprs) {
            if (partitionExpr != null) {
                serializedPartitionExprs.add(new GsonUtils.ExpressionSerializedObject(partitionExpr.toSql()));
            }
        }
        this.serializedPartitionExprs = serializedPartitionExprs;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        List<Expr> partitionExprs = Lists.newArrayList();
        for (GsonUtils.ExpressionSerializedObject expressionSql : serializedPartitionExprs) {
            if (expressionSql.expressionSql != null) {
                partitionExprs.add(SqlParser.parseSqlToExpr(expressionSql.expressionSql, SqlModeHelper.MODE_DEFAULT));
            }
        }

        for (Expr expr : partitionExprs) {
            if (expr instanceof FunctionCallExpr) {
<<<<<<< HEAD
                SlotRef slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
                // TODO: Later, for automatically partitioned tables,
                //  partitions of materialized views (also created automatically),
                //  and partition by expr tables will use ExpressionRangePartitionInfoV2
                for (Column partitionColumn : partitionColumns) {
                    if (slotRef.getColumnName().equalsIgnoreCase(partitionColumn.getName())) {
                        slotRef.setType(partitionColumn.getType());
                        slotRef.setNullable(partitionColumn.isAllowNull());
                        PartitionExprAnalyzer.analyzePartitionExpr(expr, slotRef);
                    }
=======
                slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
            } else if (expr instanceof CastExpr) {
                slotRef = AnalyzerUtils.getSlotRefFromCast(expr);
            } else if (expr instanceof SlotRef) {
                slotRef = (SlotRef) expr;
            } else {
                LOG.warn("Unknown expr type: {}", expr.toSql());
                continue;
            }

            // TODO: Later, for automatically partitioned tables,
            //  partitions of materialized views (also created automatically),
            //  and partition by expr tables will use ExpressionRangePartitionInfoV2
            if (partitionNameColumnMap.containsKey(slotRef.getColumnName())) {
                Column partitionColumn = partitionNameColumnMap.get(slotRef.getColumnName());
                slotRef.setType(partitionColumn.getType());
                slotRef.setNullable(partitionColumn.isAllowNull());
                try {
                    PartitionExprAnalyzer.analyzePartitionExpr(expr, slotRef);
                } catch (SemanticException ex) {
                    LOG.warn("Failed to analyze partition expr: {}", expr.toSql(), ex);
>>>>>>> a916db1536 ([Enhancement] Add exception handling to avoid FE startup failure (#33999))
                }
            }
        }
        this.partitionExprs = partitionExprs;
    }

    public ExpressionRangePartitionInfo(List<Expr> partitionExprs, List<Column> columns, PartitionType type) {
        super(columns);
        Preconditions.checkState(partitionExprs != null);
        Preconditions.checkState(partitionExprs.size() > 0);
        Preconditions.checkState(partitionExprs.size() == columns.size());
        this.partitionExprs = partitionExprs;
        this.isMultiColumnPartition = partitionExprs.size() > 0;
        this.type = type;
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public void setPartitionExprs(List<Expr> partitionExprs) {
        this.partitionExprs = partitionExprs;
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
        sb.append(Joiner.on(", ").join(partitionExprs.stream().map(Expr::toSql).collect(toList())));
        return sb.toString();
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        ExpressionRangePartitionInfo info = new ExpressionRangePartitionInfo();
        info.readFields(in);
        String json = Text.readString(in);
        List<GsonUtils.ExpressionSerializedObject> expressionSerializedObjects =
                GsonUtils.GSON.fromJson(json, new TypeToken<List<GsonUtils.ExpressionSerializedObject>>() {
                }.getType());
        List<Expr> partitionExprs = Lists.newArrayList();
        for (GsonUtils.ExpressionSerializedObject expressionSql : expressionSerializedObjects) {
            if (expressionSql != null && expressionSql.expressionSql != null) {
                partitionExprs.add(SqlParser.parseSqlToExpr(expressionSql.expressionSql, SqlModeHelper.MODE_DEFAULT));
            }
        }

        List<Column> partitionColumns = info.getPartitionColumns();
        for (Expr expr : partitionExprs) {
            if (expr instanceof FunctionCallExpr) {
                SlotRef slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
                // TODO: Later, for automatically partitioned tables,
                //  partitions of materialized views (also created automatically),
                //  and partition by expr tables will use ExpressionRangePartitionInfoV2
                for (Column partitionColumn : partitionColumns) {
                    if (slotRef.getColumnName().equalsIgnoreCase(partitionColumn.getName())) {
                        slotRef.setType(partitionColumn.getType());
                        slotRef.setNullable(partitionColumn.isAllowNull());
                        PartitionExprAnalyzer.analyzePartitionExpr(expr, slotRef);
                    }
                }
            }
        }
        info.setPartitionExprs(partitionExprs);
        return info;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        List<GsonUtils.ExpressionSerializedObject> serializedPartitionExprs = Lists.newArrayList();
        for (Expr partitionExpr : partitionExprs) {
            if (partitionExpr != null) {
                serializedPartitionExprs.add(new GsonUtils.ExpressionSerializedObject(partitionExpr.toSql()));
            }
        }
        this.serializedPartitionExprs = serializedPartitionExprs;
        Text.writeString(out, GsonUtils.GSON.toJson(serializedPartitionExprs));
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

    @Override
    public boolean isAutomaticPartition() {
        return type == PartitionType.EXPR_RANGE;
    }

}

