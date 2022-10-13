// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/RoutineLoadDesc.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load;

import com.starrocks.analysis.ColumnSeparator;
import com.starrocks.analysis.ImportColumnDesc;
import com.starrocks.analysis.ImportColumnsStmt;
import com.starrocks.analysis.ImportWhereStmt;
import com.starrocks.analysis.RowDelimiter;
import com.starrocks.sql.analyzer.ViewDefBuilder;
import com.starrocks.sql.ast.PartitionNames;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RoutineLoadDesc {
    private ColumnSeparator columnSeparator;
    private RowDelimiter rowDelimiter;
    private ImportColumnsStmt columnsInfo;
    private ImportWhereStmt wherePredicate;
    // nullable
    private PartitionNames partitionNames;

    public RoutineLoadDesc() {
    }

    public RoutineLoadDesc(ColumnSeparator columnSeparator, RowDelimiter rowDelimiter, ImportColumnsStmt columnsInfo,
                           ImportWhereStmt wherePredicate, PartitionNames partitionNames) {
        this.columnSeparator = columnSeparator;
        this.rowDelimiter = rowDelimiter;
        this.columnsInfo = columnsInfo;
        this.wherePredicate = wherePredicate;
        this.partitionNames = partitionNames;
    }

    public ColumnSeparator getColumnSeparator() {
        return columnSeparator;
    }

    public void setColumnSeparator(ColumnSeparator columnSeparator) {
        this.columnSeparator = columnSeparator;
    }

    public RowDelimiter getRowDelimiter() {
        return rowDelimiter;
    }

    public void setRowDelimiter(RowDelimiter rowDelimiter) {
        this.rowDelimiter = rowDelimiter;
    }

    public ImportColumnsStmt getColumnsInfo() {
        return columnsInfo;
    }

    public void setColumnsInfo(ImportColumnsStmt importColumnsStmt) {
        this.columnsInfo = importColumnsStmt;
    }

    public ImportWhereStmt getWherePredicate() {
        return wherePredicate;
    }

    public void setWherePredicate(ImportWhereStmt wherePredicate) {
        this.wherePredicate = wherePredicate;
    }

    // nullable
    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(PartitionNames partitionNames) {
        this.partitionNames = partitionNames;
    }

    public String toSql() {
        List<String> subSQLs = new ArrayList<>();
        if (columnSeparator != null) {
            subSQLs.add("COLUMNS TERMINATED BY " + columnSeparator.toSql());
        }
        if (rowDelimiter != null) {
            subSQLs.add("ROWS TERMINATED BY " + rowDelimiter.toSql());
        }
        if (columnsInfo != null) {
            String subSQL = "COLUMNS(" +
                    columnsInfo.getColumns().stream().map(this::columnToString)
                            .collect(Collectors.joining(", ")) +
                    ")";
            subSQLs.add(subSQL);
        }
        if (partitionNames != null) {
            String subSQL = null;
            if (partitionNames.isTemp()) {
                subSQL = "TEMPORARY PARTITION";
            } else {
                subSQL = "PARTITION";
            }
            subSQL += "(" + partitionNames.getPartitionNames().stream().map(this::pack)
                    .collect(Collectors.joining(", "))
                    + ")";
            subSQLs.add(subSQL);
        }
        if (wherePredicate != null) {
            subSQLs.add("WHERE " + ViewDefBuilder.build(wherePredicate.getExpr()));
        }
        return String.join(", ", subSQLs);
    }

    private String pack(String str) {
        return "`" + str + "`";
    }

    public String columnToString(ImportColumnDesc desc) {
        String str = pack(desc.getColumnName());
        if (desc.getExpr() != null) {
            str += " = " + desc.getExpr().toSql();
        }
        return str;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
