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

public class DropWarehouseStmt extends DdlStmt {
    private final boolean ifExists;
    private final String warehouseName;

    public DropWarehouseStmt(boolean ifExists, String warehouseName) {
        this(ifExists, warehouseName, NodePosition.ZERO);
    }

    public DropWarehouseStmt(boolean ifExists, String warehouseName, NodePosition pos) {
        super(pos);
        this.ifExists = ifExists;
        this.warehouseName = warehouseName;
    }

    public String getFullWhName() {
        return warehouseName;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropWarehouseStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP WAREHOUSE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append("\'" + warehouseName + "\'");
        return sb.toString();
    }

}
