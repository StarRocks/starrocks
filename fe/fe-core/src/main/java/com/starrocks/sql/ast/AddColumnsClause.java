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
import com.starrocks.analysis.ColumnDef;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

// add some columns to one index.
public class AddColumnsClause extends AlterTableColumnClause {
    private final List<ColumnDef> columnDefs;

    public List<ColumnDef> getColumnDefs() {
        return columnDefs;
    }

    public AddColumnsClause(List<ColumnDef> columnDefs, String rollupName, Map<String, String> properties) {
        this(columnDefs, rollupName, properties, NodePosition.ZERO);
    }

    public AddColumnsClause(List<ColumnDef> columnDefs, String rollupName, Map<String, String> properties,
                            NodePosition pos) {
        super(AlterOpType.SCHEMA_CHANGE, rollupName, properties, pos);
        this.columnDefs = columnDefs;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddColumnsClause(this, context);
    }
}
