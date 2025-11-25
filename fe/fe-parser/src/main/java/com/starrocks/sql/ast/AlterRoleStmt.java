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

import java.util.List;

public class AlterRoleStmt extends DdlStmt {
    private final List<String> roles;
    private final boolean ifExists;
    private final String comment;

    public AlterRoleStmt(List<String> roles, boolean ifExists, String comment) {
        this(roles, ifExists, comment, NodePosition.ZERO);
    }

    public AlterRoleStmt(List<String> roles, boolean ifExists, String comment, NodePosition pos) {
        super(pos);
        this.roles = roles;
        this.ifExists = ifExists;
        this.comment = comment;
    }

    public List<String> getRoles() {
        return roles;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterRoleStatement(this, context);
    }
}
