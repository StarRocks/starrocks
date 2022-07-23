// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AddColumnClause.java

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

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

// clause which is used to add one column to
public class AddColumnClause extends AlterTableColumnClause {
    private ColumnDef columnDef;
    // Column position
    private ColumnPosition colPos;

    public ColumnPosition getColPos() {
        return colPos;
    }

    public ColumnDef getColumnDef() {
        return columnDef;
    }

    public AddColumnClause(ColumnDef columnDef, ColumnPosition colPos, String rollupName,
                           Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE, rollupName, properties);
        this.columnDef = columnDef;
        this.colPos = colPos;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD COLUMN ").append(columnDef.toSql());
        if (colPos != null) {
            sb.append(" ").append(colPos.toSql());
        }
        if (rollupName != null) {
            sb.append(" IN `").append(rollupName).append("`");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddColumnClause(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
