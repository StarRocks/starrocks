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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PartitionDescAnalyzer;
import com.starrocks.sql.analyzer.PartitionExprAnalyzer;
import com.starrocks.sql.analyzer.PartitionFunctionChecker;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExpressionPartitionDesc extends PartitionDesc {

    private Expr expr;
    // If this value is not null, the type of the partition is different from the type of the partition field.
    private Type partitionType = null;
    // range partition desc == null means this must be materialized view
    private RangePartitionDesc rangePartitionDesc = null;

    private static final List<String> AUTO_PARTITION_SUPPORT_FUNCTIONS =
            Lists.newArrayList(FunctionSet.TIME_SLICE, FunctionSet.DATE_TRUNC);

    public ExpressionPartitionDesc(RangePartitionDesc rangePartitionDesc, Expr expr) {
        super(expr.getPos());
        this.rangePartitionDesc = rangePartitionDesc;
        this.expr = expr;
    }

    public ExpressionPartitionDesc(Expr expr) {
        super(expr.getPos());
        this.expr = expr;
    }

    @Override
    public String toString() {
        return "PARTITION BY " + expr.toSql();
    }

    public RangePartitionDesc getRangePartitionDesc() {
        return rangePartitionDesc;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        Preconditions.checkState(expr != null);
        this.expr = expr;
    }

    public SlotRef getSlotRef() {
        if (expr instanceof FunctionCallExpr) {
            ArrayList<Expr> children = expr.getChildren();
            for (Expr child : children) {
                if (child instanceof SlotRef) {
                    return (SlotRef) child;
                }
            }
        }
        return ((SlotRef) expr);
    }

    public boolean isFunction() {
        return expr instanceof FunctionCallExpr;
    }

    public Type getPartitionType() {
        return partitionType;
    }

    @Override
    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        boolean hasExprAnalyze = false;
        SlotRef slotRef;
        if (rangePartitionDesc != null) {
            // for automatic partition table
            if (rangePartitionDesc.isAutoPartitionTable) {
                rangePartitionDesc.setAutoPartitionTable(true);
                slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
                if (expr instanceof FunctionCallExpr) {
                    FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                    if (!AUTO_PARTITION_SUPPORT_FUNCTIONS.contains(functionCallExpr.getFnName().getFunction())) {
                        throw new SemanticException("Only support date_trunc and time_slice as partition expression");
                    }
                }
            } else {
                // for partition by range expr table
                // The type of the partition field may be different from the type after the expression
                if (expr instanceof CastExpr) {
                    slotRef = AnalyzerUtils.getSlotRefFromCast(expr);
                    partitionType = ((CastExpr) expr).getTargetTypeDef().getType();
                } else if (expr instanceof FunctionCallExpr) {
                    slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);

                    Optional<ColumnDef> columnDef = columnDefs.stream()
                            .filter(c -> c.getName().equals(slotRef.getColumnName())).findFirst();
                    Preconditions.checkState(columnDef.isPresent());
                    slotRef.setType(columnDef.get().getType());

                    String functionName = ((FunctionCallExpr) expr).getFnName().getFunction().toLowerCase();
                    if (functionName.equals(FunctionSet.STR2DATE)) {
                        partitionType = Type.DATE;
                        if (!PartitionFunctionChecker.checkStr2date(expr)) {
                            throw new SemanticException("partition function check fail, only supports the result " +
                                    "of the function str2date(VARCHAR str, VARCHAR format) as a strict DATE type");
                        }
                    }
                } else {
                    throw new AnalysisException("Unsupported expr:" + expr.toSql());
                }
            }
            rangePartitionDesc.partitionType = partitionType;
            PartitionDescAnalyzer.analyze(rangePartitionDesc);
            rangePartitionDesc.analyze(columnDefs, otherProperties);
        } else {
            // for materialized view
            slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
        }

        for (ColumnDef columnDef : columnDefs) {
            if (columnDef.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                slotRef.setType(columnDef.getType());
                PartitionExprAnalyzer.analyzePartitionExpr(expr, slotRef);
                partitionType = expr.getType();
                hasExprAnalyze = true;
            }
        }
        if (!hasExprAnalyze) {
            throw new AnalysisException("Partition expr without analyzed.");
        }
    }
}
