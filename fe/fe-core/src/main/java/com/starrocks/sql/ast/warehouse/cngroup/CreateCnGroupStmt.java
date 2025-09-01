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

package com.starrocks.sql.ast.warehouse.cngroup;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class CreateCnGroupStmt extends CnGroupStmtBase {
    private final String comment;
    private final Map<String, String> properties;
    private final boolean ifNotExists;

    public CreateCnGroupStmt(boolean ifNotExists, String warehouseName, String cnGroupName, String comment,
                             Map<String, String> properties) {
        this(ifNotExists, warehouseName, cnGroupName, comment, properties, NodePosition.ZERO);
    }

    public CreateCnGroupStmt(boolean ifNotExists, String warehouseName, String cnGroupName, String comment,
                             Map<String, String> properties, NodePosition pos) {
        super(warehouseName, cnGroupName, pos);
        this.properties = properties;
        this.ifNotExists = ifNotExists;
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCreateCNGroupStatement(this, context);
    }

    @Override
    public String toSql() {
        // TODO:
        return "";
    }
}
