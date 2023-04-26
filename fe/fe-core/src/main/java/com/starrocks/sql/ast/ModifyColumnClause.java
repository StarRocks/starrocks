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

// modify one column
public class ModifyColumnClause extends AlterTableColumnClause {
    private final ColumnDef columnDef;
    private final ColumnPosition colPos;

    public ColumnDef getColumnDef() {
        return columnDef;
    }

    public ColumnPosition getColPos() {
        return colPos;
    }

    public ModifyColumnClause(ColumnDef columnDef, ColumnPosition colPos, String rollup,
                              Map<String, String> properties) {
        this(columnDef, colPos, rollup, properties, NodePosition.ZERO);
    }

    public ModifyColumnClause(ColumnDef columnDef, ColumnPosition colPos, String rollup,
                              Map<String, String> properties, NodePosition pos) {
        super(AlterOpType.SCHEMA_CHANGE, rollup, properties, pos);
        this.columnDef = columnDef;
        this.colPos = colPos;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyColumnClause(this, context);
    }
}
