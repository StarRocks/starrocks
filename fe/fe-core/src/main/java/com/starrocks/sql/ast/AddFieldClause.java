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
import com.starrocks.catalog.Type;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

// clause which is used to add one field to
public class AddFieldClause extends AlterTableColumnClause {
    private final String colName;
    private final StructFieldDesc fieldDesc;

    public String getColName() {
        return colName;
    }

    public StructFieldDesc getFieldDesc() {
        return fieldDesc;
    }

    public List<String> getNestedParentFieldNames() {
        return fieldDesc.getNestedParentFieldNames();
    }

    public String getFieldName() {
        return fieldDesc.getFieldName();
    }

    public Type getType() {
        return fieldDesc.getType();
    }

    public ColumnPosition getFieldPos() {
        return fieldDesc.getFieldPos();
    }

    public AddFieldClause(String colName, StructFieldDesc fieldDesc, Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE, null, properties, NodePosition.ZERO);
        this.colName = colName;
        this.fieldDesc = fieldDesc;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddFieldClause(this, context);
    }
}