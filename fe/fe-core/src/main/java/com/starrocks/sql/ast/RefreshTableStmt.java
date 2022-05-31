// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;

import java.util.List;

public class RefreshTableStmt extends DdlStmt {
    private TableName tableName;
    private List<String> partitionNames;

    public RefreshTableStmt(TableName tableName, List<String> partitionNames) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public TableName getTbl() {
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
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REFRESH EXTERNAL TABLE ");
        sb.append(tableName.toSql()).append(" ");
        sb.append("PARTITION(").append(String.join("", partitionNames)).append(")");
        return sb.toString();
    }
}
