// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

public class AlterFailoverGroupAddStmt extends DdlStmt {
    private final boolean ifExists;
    private final String failoverGroupName;
    private final List<String> catalogNames;
    private final List<DatabaseName> databaseNames;
    private final List<TableName> tableNames;
    private final List<String> members;
    private final Map<String, String> properties;

    public AlterFailoverGroupAddStmt(
            boolean ifExists,
            String failoverGroupName,
            List<String> catalogNames,
            List<DatabaseName> databaseNames,
            List<TableName> tableNames,
            List<String> members,
            Map<String, String> properties,
            NodePosition pos) {
        super(pos);
        this.ifExists = ifExists;
        this.failoverGroupName = failoverGroupName;
        this.catalogNames = catalogNames;
        this.databaseNames = databaseNames;
        this.tableNames = tableNames;
        this.members = members;
        this.properties = properties;
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

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitAlterFailoverGroupAddStatement(this, context);
        } else {
            return null;
        }
    }
}
