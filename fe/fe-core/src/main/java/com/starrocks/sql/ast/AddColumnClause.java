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

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// clause which is used to add one column to
public class AddColumnClause extends AlterTableColumnClause {
    private final ColumnDef columnDef;
    // Column position
    private ColumnPosition colPos;

    public ColumnPosition getColPos() {
        return colPos;
    }

    public void setColPos(ColumnPosition colPos) {
        this.colPos = colPos;
    }

    public ColumnDef getColumnDef() {
        return columnDef;
    }

    public AddColumnClause(ColumnDef columnDef, ColumnPosition colPos, String rollupName,
                           Map<String, String> properties) {
        this(columnDef, colPos, rollupName, properties, NodePosition.ZERO);
    }

    public AddColumnClause(ColumnDef columnDef, ColumnPosition colPos, String rollupName,
                           Map<String, String> properties, NodePosition pos) {
        super(AlterOpType.SCHEMA_CHANGE, rollupName, properties, pos);
        this.columnDef = columnDef;
        this.colPos = colPos;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddColumnClause(this, context);
    }
}
