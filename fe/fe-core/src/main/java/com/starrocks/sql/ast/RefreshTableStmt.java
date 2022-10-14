// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;

import java.util.List;

/**
 * This command used to refresh connector table of external catalog.
 * For example:
 * 'REFRESH EXTERNAL TABLE catalog1.db1.table1'
 * This sql will refresh table1 of db1 in catalog1.
 */
public class RefreshTableStmt extends DdlStmt {
    private final TableName tableName;
    private final List<String> partitionNames;

    public RefreshTableStmt(TableName tableName, List<String> partitionNames) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<String> getPartitions() {
        return partitionNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshTableStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
