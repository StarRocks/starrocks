// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.List;

/**
 * ExpressionRangePartitionInfo replace columns with expressions
 * Some Descriptions:
 * 1. no overwrite old serialized method: read、write and readFields, because we use gson now
 * 2  no overwrite toSql method，because we will redesign range partition grammar in the future
 */
public class ExpressionRangePartitionInfo extends RangePartitionInfo {

    @SerializedName(value = "partitionExprs")
    private final List<Expr> partitionExprs;

    public ExpressionRangePartitionInfo(List<Expr> partitionExprs) {
        super(Lists.newArrayList());
        Preconditions.checkState(partitionExprs != null);
        Preconditions.checkState(partitionExprs.size() > 0);
        this.partitionExprs = partitionExprs;
        this.isMultiColumnPartition = partitionExprs.size() > 0;
        extractPartitionColumns(partitionExprs);
    }

    /**
     * extract partition columns from  partition expressions
     *
     * @param partitionExprs partition expressions
     */
    private void extractPartitionColumns(List<Expr> partitionExprs) {
        List<Column> partitionColumns = super.getPartitionColumns();
        for (Expr expr : partitionExprs) {
            if (expr instanceof SlotRef) {
                partitionColumns.add(((SlotRef) expr).getColumn());
            } else if (expr instanceof FunctionCallExpr) {
                for (Expr child : expr.getChildren()) {
                    if (child instanceof SlotRef) {
                        partitionColumns.add(((SlotRef) child).getColumn());
                        break;
                    }
                }
            } else {
                throw new SemanticException("Unsupported expression type:" + expr.getType());
            }
        }
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

}

