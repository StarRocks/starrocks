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
    private final List<String> includeCatalogs;
    private final List<DatabaseName> includeDatabases;
    private final List<TableName> includeTables;
    private final List<String> excludeCatalogs;
    private final List<DatabaseName> excludeDatabases;
    private final List<TableName> excludeTables;
    private final List<String> members;
    private final Map<String, String> properties;

    public AlterFailoverGroupAddStmt(
            boolean ifExists,
            String failoverGroupName,
            List<String> includeCatalogs,
            List<DatabaseName> includeDatabases,
            List<TableName> includeTables,
            List<String> excludeCatalogs,
            List<DatabaseName> excludeDatabases,
            List<TableName> excludeTables,
            List<String> members,
            Map<String, String> properties,
            NodePosition pos) {
        super(pos);
        this.ifExists = ifExists;
        this.failoverGroupName = failoverGroupName;
        this.includeCatalogs = includeCatalogs;
        this.includeDatabases = includeDatabases;
        this.includeTables = includeTables;
        this.excludeCatalogs = excludeCatalogs;
        this.excludeDatabases = excludeDatabases;
        this.excludeTables = excludeTables;
        this.members = members;
        this.properties = properties;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    public String getFailoverGroupName() {
        return failoverGroupName;
    }

    public List<String> getIncludeCatalogs() {
        return includeCatalogs;
    }

    public List<DatabaseName> getIncludeDatabases() {
        return includeDatabases;
    }

    public List<TableName> getIncludeTables() {
        return includeTables;
    }

    public List<String> getExcludeCatalogs() {
        return excludeCatalogs;
    }

    public List<DatabaseName> getExcludeDatabases() {
        return excludeDatabases;
    }

    public List<TableName> getExcludeTables() {
        return excludeTables;
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
