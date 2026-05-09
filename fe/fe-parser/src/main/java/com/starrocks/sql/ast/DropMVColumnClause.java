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

import com.starrocks.sql.parser.NodePosition;

import java.util.Collections;

/**
 * Clause which is used to drop a column from a materialized view.
 * Syntax: ALTER MATERIALIZED VIEW mv_name DROP COLUMN column_name
 */
public class DropMVColumnClause extends AlterTableColumnClause {
    private final String columnName;

    public DropMVColumnClause(String columnName) {
        this(columnName, NodePosition.ZERO);
    }

    public DropMVColumnClause(String columnName, NodePosition pos) {
        super(null, pos);
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropMVColumnClause(this, context);
    }

    public DropColumnClause toDropColumnClause() {
        return new DropColumnClause(columnName, null, Collections.emptyMap(), getPos());
    }
}
