// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AddColumnsClause.java

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

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.alter.AlterOpType;
import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;

import java.util.List;
import java.util.Map;

// add some columns to one index.
public class AddColumnsClause extends AlterTableClause {
    private List<ColumnDef> columnDefs;
    private String rollupName;

    private Map<String, String> properties;
    // set in analyze
    private List<Column> columns;

    public List<Column> getColumns() {
        return columns;
    }

    public String getRollupName() {
        return rollupName;
    }

    public AddColumnsClause(List<ColumnDef> columnDefs, String rollupName, Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.columnDefs = columnDefs;
        this.rollupName = rollupName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (columnDefs == null || columnDefs.isEmpty()) {
            throw new AnalysisException("Columns is empty in add columns clause.");
        }
        for (ColumnDef colDef : columnDefs) {
            colDef.analyze(true);

            if (!colDef.isAllowNull() && colDef.defaultValueIsNull()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DEFAULT_FOR_FIELD, colDef.getName());
            }
        }

        // Make sure return null if rollup name is empty.
        rollupName = Strings.emptyToNull(rollupName);

        columns = Lists.newArrayList();
        for (ColumnDef columnDef : columnDefs) {
            Column col = columnDef.toColumn();
            columns.add(col);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD COLUMN (");
        int idx = 0;
        for (ColumnDef columnDef : columnDefs) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append(columnDef.toSql());
            idx++;
        }
        sb.append(")");
        if (rollupName != null) {
            sb.append(" IN `").append(rollupName).append("`");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
