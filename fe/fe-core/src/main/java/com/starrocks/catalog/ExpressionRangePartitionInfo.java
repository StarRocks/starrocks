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
import com.google.common.collect.Range;
import com.google.common.reflect.TypeToken;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PartitionExprAnalyzer;
import com.starrocks.sql.ast.AstVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * ExpressionRangePartitionInfo replace columns with expressions
 * Some Descriptions:
 * 1. no overwrite old serialized method: read、write and readFields, because we use gson now
 */
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
        sb.append("\n(");
        // sort range
        List<Map.Entry<Long, Range<PartitionKey>>> entries = new ArrayList<>(getIdToRange(false).entrySet());
        entries.sort(RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);

        int idx = 0;
        PartitionInfo tblPartitionInfo = table.getPartitionInfo();

        String replicationNumStr = table.getTableProperty().getProperties().get("replication_num");
        short replicationNum;
        if (replicationNumStr == null) {
            replicationNum = FeConstants.default_replication_num;
        } else {
            replicationNum = Short.parseShort(replicationNumStr);
        }

        for (Map.Entry<Long, Range<PartitionKey>> entry : entries) {
            Partition partition = table.getPartition(entry.getKey());
            String partitionName = partition.getName();
            Range<PartitionKey> range = entry.getValue();

            // print all partitions' range is fixed range, even if some of them is created by less than range
            sb.append("PARTITION ").append(partitionName).append(" VALUES [");
            sb.append(range.lowerEndpoint().toSql());
            sb.append(", ").append(range.upperEndpoint().toSql()).append(")");

            if (partitionId != null) {
                partitionId.add(entry.getKey());
                break;
            }
            short curPartitionReplicationNum = tblPartitionInfo.getReplicationNum(entry.getKey());
            if (curPartitionReplicationNum != replicationNum) {
                sb.append("(").append("\"replication_num\" = \"").append(curPartitionReplicationNum).append("\")");
            }
            if (idx != entries.size() - 1) {
                sb.append(",\n");
            }
            idx++;
        }
        sb.append(")");
        return sb.toString();
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
                for (Column partitionColumn : partitionColumns) {
                    if (slotRef.getColumnName().equalsIgnoreCase(partitionColumn.getName())) {
                        PartitionExprAnalyzer.analyzePartitionExpr(expr, partitionColumn.getType());
                        slotRef.setType(partitionColumn.getType());
                    }
                }
            }
        }
        info.setPartitionExprs(exprs);
        return info;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, GsonUtils.GSON.toJson(partitionExprs));
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

