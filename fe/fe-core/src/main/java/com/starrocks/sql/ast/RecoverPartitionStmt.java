// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;

public class RecoverPartitionStmt extends DdlStmt {
    private final TableName dbTblName;
    private final String partitionName;

    public RecoverPartitionStmt(TableName dbTblName, String partitionName) {
        this.dbTblName = dbTblName;
        this.partitionName = partitionName;
    }

    public String getDbName() {
        return dbTblName.getDb();
    }

    public String getTableName() {
        return dbTblName.getTbl();
    }

    public TableName getDbTblName() {
        return dbTblName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRecoverPartitionStmt(this, context);
    }
}
