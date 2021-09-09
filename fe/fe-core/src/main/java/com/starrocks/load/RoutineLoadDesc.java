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
import com.starrocks.analysis.ImportColumnsStmt;
import com.starrocks.analysis.ImportWhereStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.RowDelimiter;

public class RoutineLoadDesc {
    private final ColumnSeparator columnSeparator;
    private final RowDelimiter rowDelimiter;
    private final ImportColumnsStmt columnsInfo;
    private final ImportWhereStmt wherePredicate;
    // nullable
    private final PartitionNames partitionNames;

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

    public RowDelimiter getRowDelimiter() {
        return rowDelimiter;
    }

    public ImportColumnsStmt getColumnsInfo() {
        return columnsInfo;
    }

    public ImportWhereStmt getWherePredicate() {
        return wherePredicate;
    }

    // nullable
    public PartitionNames getPartitionNames() {
        return partitionNames;
    }
}
