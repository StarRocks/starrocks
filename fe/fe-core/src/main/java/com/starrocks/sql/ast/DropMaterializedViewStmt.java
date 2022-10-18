// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;

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

    public DropMaterializedViewStmt(boolean ifExists, TableName dbMvName) {
        this.ifExists = ifExists;
        this.dbMvName = dbMvName;
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

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropMaterializedViewStatement(this, context);
    }
}
