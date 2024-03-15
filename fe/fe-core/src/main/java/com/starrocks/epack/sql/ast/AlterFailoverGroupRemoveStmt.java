// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class AlterFailoverGroupRemoveStmt extends DdlStmt {
    private final boolean ifExists;
    private final String failoverGroupName;
    private final List<String> catalogNames;
    private final List<DatabaseName> databaseNames;
    private final List<TableName> tableNames;
    private final List<String> members;

    public AlterFailoverGroupRemoveStmt(
            boolean ifExists,
            String failoverGroupName,
            List<String> catalogNames,
            List<DatabaseName> databaseNames,
            List<TableName> tableNames,
            List<String> members,
            NodePosition pos) {
        super(pos);
        this.ifExists = ifExists;
        this.failoverGroupName = failoverGroupName;
        this.catalogNames = catalogNames;
        this.databaseNames = databaseNames;
        this.tableNames = tableNames;
        this.members = members;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    public String getFailoverGroupName() {
        return failoverGroupName;
    }

    public List<String> getCatalogNames() {
        return catalogNames;
    }

    public List<DatabaseName> getDatabaseNames() {
        return databaseNames;
    }

    public List<TableName> getTableNames() {
        return tableNames;
    }

    public List<String> getMembers() {
        return members;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitAlterFailoverGroupRemoveStatement(this, context);
        } else {
            return null;
        }
    }
}
