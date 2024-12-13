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
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
<<<<<<< HEAD
=======
import com.starrocks.catalog.FunctionSet;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
<<<<<<< HEAD
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PartitionExprAnalyzer;
=======
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PartitionDescAnalyzer;
import com.starrocks.sql.analyzer.PartitionExprAnalyzer;
import com.starrocks.sql.analyzer.PartitionFunctionChecker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.MetaUtils;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
=======
import java.util.Optional;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

public class ExpressionPartitionDesc extends PartitionDesc {

    private Expr expr;
    // If this value is not null, the type of the partition is different from the type of the partition field.
    private Type partitionType = null;
    // range partition desc == null means this must be materialized view
    private RangePartitionDesc rangePartitionDesc = null;

<<<<<<< HEAD
=======
    private static final List<String> AUTO_PARTITION_SUPPORT_FUNCTIONS =
            Lists.newArrayList(FunctionSet.TIME_SLICE, FunctionSet.DATE_TRUNC);

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public ExpressionPartitionDesc(RangePartitionDesc rangePartitionDesc, Expr expr) {
        super(expr.getPos());
        this.rangePartitionDesc = rangePartitionDesc;
        this.expr = expr;
    }

    public ExpressionPartitionDesc(Expr expr) {
        super(expr.getPos());
        this.expr = expr;
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

    @Override
    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        boolean hasExprAnalyze = false;
        SlotRef slotRef;
        if (rangePartitionDesc != null) {
<<<<<<< HEAD
            rangePartitionDesc.analyze(columnDefs, otherProperties);
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            // for automatic partition table
            if (rangePartitionDesc.isAutoPartitionTable) {
                rangePartitionDesc.setAutoPartitionTable(true);
                slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
<<<<<<< HEAD
=======
                if (expr instanceof FunctionCallExpr) {
                    FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                    if (!AUTO_PARTITION_SUPPORT_FUNCTIONS.contains(functionCallExpr.getFnName().getFunction())) {
                        throw new SemanticException("Only support date_trunc and time_slice as partition expression");
                    }
                }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            } else {
                // for partition by range expr table
                // The type of the partition field may be different from the type after the expression
                if (expr instanceof CastExpr) {
                    slotRef = AnalyzerUtils.getSlotRefFromCast(expr);
                    partitionType = ((CastExpr) expr).getTargetTypeDef().getType();
                } else if (expr instanceof FunctionCallExpr) {
                    slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
<<<<<<< HEAD
=======

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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                } else {
                    throw new AnalysisException("Unsupported expr:" + expr.toSql());
                }
            }
<<<<<<< HEAD
=======
            rangePartitionDesc.partitionType = partitionType;
            PartitionDescAnalyzer.analyze(rangePartitionDesc);
            rangePartitionDesc.analyze(columnDefs, otherProperties);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        } else {
            // for materialized view
            slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
        }

        for (ColumnDef columnDef : columnDefs) {
            if (columnDef.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                slotRef.setType(columnDef.getType());
                PartitionExprAnalyzer.analyzePartitionExpr(expr, slotRef);
<<<<<<< HEAD
=======
                partitionType = expr.getType();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                hasExprAnalyze = true;
            }
        }
        if (!hasExprAnalyze) {
            throw new AnalysisException("Partition expr without analyzed.");
        }
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> schema, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException {
        // for materialized view express partition.
        if (rangePartitionDesc == null) {
<<<<<<< HEAD
            return new ExpressionRangePartitionInfo(Collections.singletonList(expr), schema, PartitionType.RANGE);
=======
            return new ExpressionRangePartitionInfo(Collections.singletonList(ColumnIdExpr.create(schema, expr)),
                    schema, PartitionType.RANGE);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
        List<Column> partitionColumns = Lists.newArrayList();
        // check and get partition column
        for (String colName : rangePartitionDesc.getPartitionColNames()) {
            findRangePartitionColumn(schema, partitionColumns, colName);
        }
        // automatic partition / partition expr only support one partition column
        Column sourcePartitionColumn = partitionColumns.get(0);
        if (partitionType != null) {
            Column newTypePartitionColumn = new Column(sourcePartitionColumn);
            newTypePartitionColumn.setType(partitionType);
            partitionColumns = Lists.newArrayList(newTypePartitionColumn);
        }
        for (Column column : partitionColumns) {
            try {
<<<<<<< HEAD
                RangePartitionInfo.checkRangeColumnType(column);
=======
                RangePartitionInfo.checkExpressionRangeColumnType(column, expr);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // Recreate a partition column type bypass check
        RangePartitionInfo partitionInfo;
        if (rangePartitionDesc.isAutoPartitionTable) {
            // for automatic partition table
<<<<<<< HEAD
            partitionInfo = new ExpressionRangePartitionInfo(Collections.singletonList(expr), partitionColumns,
=======
            partitionInfo = new ExpressionRangePartitionInfo(
                    Collections.singletonList(ColumnIdExpr.create(schema, expr)),
                    partitionColumns,
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    PartitionType.EXPR_RANGE);
        } else {
            // for partition by range expr
            ExpressionRangePartitionInfoV2 expressionRangePartitionInfoV2 =
<<<<<<< HEAD
                    new ExpressionRangePartitionInfoV2(Collections.singletonList(expr), partitionColumns);
=======
                    new ExpressionRangePartitionInfoV2(Collections.singletonList(ColumnIdExpr.create(schema, expr)),
                            partitionColumns);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            expressionRangePartitionInfoV2.setSourcePartitionTypes(Collections.singletonList(sourcePartitionColumn.getType()));
            partitionInfo = expressionRangePartitionInfoV2;
        }

        for (SingleRangePartitionDesc desc : getRangePartitionDesc().getSingleRangePartitionDescs()) {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
<<<<<<< HEAD
            partitionInfo.handleNewSinglePartitionDesc(desc, partitionId, isTemp);
=======
            partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(schema), desc, partitionId, isTemp);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }

        return partitionInfo;
    }

    static void findRangePartitionColumn(List<Column> schema, List<Column> partitionColumns, String colName)
            throws DdlException {
        boolean find = false;
        for (Column column : schema) {
            if (column.getName().equalsIgnoreCase(colName)) {
                if (!column.isKey() && column.getAggregationType() != AggregateType.NONE) {
                    throw new DdlException("The partition column could not be aggregated column"
                            + " and unique table's partition column must be key column");
                }

                if (column.getType().isFloatingPointType() || column.getType().isComplexType()) {
                    throw new DdlException(String.format("Invalid partition column '%s': %s",
                            column.getName(), "invalid data type " + column.getType()));
                }

                partitionColumns.add(column);
                find = true;
                break;
            }
        }
        if (!find) {
            throw new DdlException("Partition column[" + colName + "] does not found");
        }
    }

}
