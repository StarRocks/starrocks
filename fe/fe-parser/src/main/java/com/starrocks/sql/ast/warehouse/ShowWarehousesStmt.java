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

package com.starrocks.sql.ast.warehouse;

import com.google.common.base.Strings;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

// Show warehouse statement.
public class ShowWarehousesStmt extends ShowStmt {
    private final String pattern;

    public ShowWarehousesStmt(String pattern) {
        this(pattern, NodePosition.ZERO);
    }

    public ShowWarehousesStmt(String pattern, NodePosition pos) {
        super(pos);
        this.pattern = Strings.nullToEmpty(pattern);
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW WAREHOUSES");
        if (!pattern.isEmpty()) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowWarehousesStatement(this, context);
    }
}

