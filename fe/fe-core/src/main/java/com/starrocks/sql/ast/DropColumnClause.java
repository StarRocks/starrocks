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
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// Drop one column
public class DropColumnClause extends AlterTableColumnClause {
    private final String colName;

    public String getColName() {
        return colName;
    }

    public DropColumnClause(String colName, String rollupName, Map<String, String> properties) {
        this(colName, rollupName, properties, NodePosition.ZERO);
    }

    public DropColumnClause(String colName, String rollupName, Map<String, String> properties, NodePosition pos) {
        super(AlterOpType.SCHEMA_CHANGE, rollupName, properties, pos);
        this.colName = colName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropColumnClause(this, context);
    }
}
