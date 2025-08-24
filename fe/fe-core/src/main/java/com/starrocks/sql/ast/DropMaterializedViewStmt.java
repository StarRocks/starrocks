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

import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

/**
 * DROP MATERIALIZED VIEW [ IF EXISTS ] <mv_name> IN|FROM [db_name].<table_name>
 * <p>
 * Parameters
 * IF EXISTS: Do not throw an error if the materialized view does not exist. A notice is issued in this case.
 * mv_name: The name of the materialized view to remove.
 * db_name: The name of db to which materialized view belongs.
 * table_name: The name of table to which materialized view belongs.
 */
public class DropMaterializedViewStmt extends DdlStmt {

    private final boolean ifExists;
    private final TableName dbMvName;
    private final boolean forceDrop;

    public DropMaterializedViewStmt(boolean ifExists, TableName dbMvName) {
        this(ifExists, dbMvName, false, NodePosition.ZERO);
    }

    public DropMaterializedViewStmt(boolean ifExists, TableName dbMvName, boolean forceDrop) {
        this(ifExists, dbMvName, forceDrop, NodePosition.ZERO);
    }

    public DropMaterializedViewStmt(boolean ifExists, TableName dbMvName, boolean forceDrop, NodePosition pos) {
        super(pos);
        this.ifExists = ifExists;
        this.dbMvName = dbMvName;
        this.forceDrop = forceDrop;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getMvName() {
        return dbMvName.getTbl();
    }

    public String getDbName() {
        return dbMvName.getDb();
    }

    public TableName getDbMvName() {
        return dbMvName;
    }

    public boolean isForceDrop() {
        return this.forceDrop;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropMaterializedViewStatement(this, context);
    }
}
