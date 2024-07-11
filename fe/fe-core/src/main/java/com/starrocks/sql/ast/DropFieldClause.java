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

import java.util.List;
import java.util.Map;

public class DropFieldClause extends AlterTableColumnClause {
    private final String colName;
    private final String fieldName;
    private final List<String> nestedParentFieldNames;

    public String getColName() {
        return colName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public List<String> getNestedParentFieldNames() {
        return nestedParentFieldNames;
    }

    public DropFieldClause(String colName, String fieldName, List<String> nestedParentFieldNames, 
                          Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE, null, properties, NodePosition.ZERO);
        this.colName = colName;
        this.fieldName = fieldName;
        this.nestedParentFieldNames = nestedParentFieldNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropFieldClause(this, context);
    }
}