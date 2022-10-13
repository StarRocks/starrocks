// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.ShowAlterStmt.AlterType;

import java.util.List;

/*
 * CANCEL ALTER COLUMN|ROLLUP FROM db_name.table_name
 */
public class CancelAlterTableStmt extends CancelStmt {

    private final AlterType alterType;

    private final TableName dbTableName;

    public AlterType getAlterType() {
        return alterType;
    }

    public String getDbName() {
        return dbTableName.getDb();
    }

    public void setDbName(String dbName) {
        this.dbTableName.setDb(dbName);
    }

    public TableName getDbTableName() {
        return this.dbTableName;
    }

    public String getTableName() {
        return dbTableName.getTbl();
    }

    private final List<Long> alterJobIdList;

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName) {
        this(alterType, dbTableName, null);
    }

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName, List<Long> alterJobIdList) {
        this.alterType = alterType;
        this.dbTableName = dbTableName;
        this.alterJobIdList = alterJobIdList;
    }

    public List<Long> getAlterJobIdList() {
        return alterJobIdList;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelAlterTableStatement(this, context);
    }

    @Override
    public String toString() {
        return toSql();
    }

}
