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

import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;

// SHOW CREATE TABLE statement.
public class ShowCreateTableStmt extends ShowStmt {
    public enum CreateTableType {
        TABLE("TABLE"),
        VIEW("VIEW"),
        MATERIALIZED_VIEW("MATERIALIZED VIEW");
        private final String value;

        CreateTableType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private final TableName tbl;
    private final CreateTableType type;

    public ShowCreateTableStmt(TableName tbl, CreateTableType type) {
        this(tbl, type, NodePosition.ZERO);
    }

    public ShowCreateTableStmt(TableName tbl, CreateTableType type, NodePosition pos) {
        super(pos);
        this.tbl = tbl;
        this.type = type;
    }

    public TableName getTbl() {
        return tbl;
    }

    public String getDb() {
        return tbl.getDb();
    }

    public String getTable() {
        return tbl.getTbl();
    }

    public CreateTableType getType() {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowCreateTableStatement(this, context);
    }
}
