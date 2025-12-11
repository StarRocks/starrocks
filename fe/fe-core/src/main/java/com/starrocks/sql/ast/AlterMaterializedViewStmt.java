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

/**
 * 1.Support for modifying the way of refresh and the cycle of asynchronous refresh;
 * 2.Support for modifying the name of a materialized view;
 * 3.SYNC is not supported and ASYNC is not allow changed to SYNC
 */
public class AlterMaterializedViewStmt extends DdlStmt {
    private TableRef mvTableRef;
    private final AlterTableClause alterTableClause;

    public AlterMaterializedViewStmt(TableRef mvTableRef, AlterTableClause alterTableClause, NodePosition pos) {
        super(pos);
        this.mvTableRef = mvTableRef;
        this.alterTableClause = alterTableClause;
    }

    public TableRef getMvTableRef() {
        return mvTableRef;
    }

    public void setMvTableRef(TableRef mvTableRef) {
        this.mvTableRef = mvTableRef;
    }

    public String getCatalogName() {
        return mvTableRef == null ? null : mvTableRef.getCatalogName();
    }

    public String getDbName() {
        return mvTableRef == null ? null : mvTableRef.getDbName();
    }

    public String getMvName() {
        return mvTableRef == null ? null : mvTableRef.getTableName();
    }

    public AlterTableClause getAlterTableClause() {
        return alterTableClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitAlterMaterializedViewStatement(this, context);
    }
}
