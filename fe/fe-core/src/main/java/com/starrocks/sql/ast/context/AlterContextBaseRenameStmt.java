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

package com.starrocks.sql.ast.context;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

/**
 * {@code ALTER CONTEXTBASE <name> RENAME TO <newName>}.
 *
 * <p>An in-place rename is a metadata-only rekey: the contextbase keeps its numeric id, so all
 * physical data (rows in {@code __internal_context} are keyed by {@code contextbase_id}) and all
 * privileges ({@link com.starrocks.authorization.ContextBasePEntryObject} stores the id) survive
 * untouched. Only the FE in-memory name maps and their name-derived collection / workspace keys
 * are re-keyed in {@link com.starrocks.context.ContextMgr#renameContextBase}.
 */
public class AlterContextBaseRenameStmt extends DdlStmt {

    private final ContextBaseName name;
    private final String newName;

    public AlterContextBaseRenameStmt(ContextBaseName name, String newName, NodePosition pos) {
        super(pos);
        this.name = name;
        this.newName = newName;
    }

    public ContextBaseName getName() {
        return name;
    }

    public String getNewName() {
        return newName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitAlterContextBaseRenameStatement(this, context);
    }

    @Override
    public String toSql() {
        return ContextStmtFormatter.alterContextBaseRename(name, newName);
    }
}
