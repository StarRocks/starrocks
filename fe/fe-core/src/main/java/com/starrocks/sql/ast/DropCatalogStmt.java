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

// ToDo(zhuodong): to support internal catalog in the future
public class DropCatalogStmt extends DdlStmt {

    private final String name;
<<<<<<< HEAD

    public DropCatalogStmt(String name) {
        this(name, NodePosition.ZERO);
    }

    public DropCatalogStmt(String name, NodePosition pos) {
        super(pos);
        this.name = name;
=======
    private final boolean ifExists;


    public DropCatalogStmt(String name) {
        this(name, false, NodePosition.ZERO);
    }

    public DropCatalogStmt(String name, boolean ifExists) {
        this(name, ifExists, NodePosition.ZERO);
    }

    public DropCatalogStmt(String name, boolean ifExists, NodePosition pos) {
        super(pos);
        this.name = name;
        this.ifExists = ifExists;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    public String getName() {
        return name;
    }

<<<<<<< HEAD
=======
    public boolean isIfExists() {
        return ifExists;
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropCatalogStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP CATALOG ");
<<<<<<< HEAD
=======
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        sb.append("\'" + name + "\'");
        return sb.toString();
    }
}
