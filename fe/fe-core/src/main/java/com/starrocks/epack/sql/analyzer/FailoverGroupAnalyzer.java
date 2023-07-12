// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DatabaseName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;

import java.util.List;

public class FailoverGroupAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new FailoverGroupAnalyzerVisitor().analyze(statement, session);
    }

    static class FailoverGroupAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreatePrimaryFailoverGroupStatement(
                CreatePrimaryFailoverGroupStmt statement, ConnectContext session) {
            String failoverGroupName = statement.getFailoverGroupName();
            if (Strings.isNullOrEmpty(failoverGroupName)) {
                throw new SemanticException("Failover group name is empty");
            }

            List<String> catalogNames = statement.getCatalogNames();
            if (catalogNames != null) {
                for (String catalogName : catalogNames) {
                    if (Strings.isNullOrEmpty(catalogName)) {
                        throw new SemanticException("Catalog name is empty");
                    }
                }
            }

            List<DatabaseName> databaseNames = statement.getDatabaseNames();
            if (databaseNames != null) {
                for (DatabaseName databaseName : databaseNames) {
                    normalizationDatabaseName(databaseName, session);
                }
            }

            List<TableName> tableNames = statement.getTableNames();
            if (tableNames != null) {
                for (TableName tableName : tableNames) {
                    normalizationTableName(tableName, session);
                }
            }

            List<String> members = statement.getMembers();
            if (members == null || members.size() < 2) {
                throw new SemanticException("No enough members");
            }
            for (String member : members) {
                if (Strings.isNullOrEmpty(member)) {
                    throw new SemanticException("Member is empty");
                }
            }

            String schedule = statement.getSchedule();
            if (Strings.isNullOrEmpty(schedule)) {
                throw new SemanticException("No schedule");
            }

            return null;
        }

        @Override
        public Void visitCreateSecondaryFailoverGroupStatement(
                CreateSecondaryFailoverGroupStmt statement, ConnectContext context) {
            String failoverGroupName = statement.getFailoverGroupName();
            if (Strings.isNullOrEmpty(failoverGroupName)) {
                throw new SemanticException("Failover group name is empty");
            }

            String primaryMember = statement.getPrimaryMember();
            if (Strings.isNullOrEmpty(primaryMember)) {
                throw new SemanticException("Primary member is empty");
            }

            return null;
        }
    }

    public static void normalizationDatabaseName(DatabaseName databaseName, ConnectContext connectContext) {
        if (Strings.isNullOrEmpty(databaseName.getCatalog())) {
            if (Strings.isNullOrEmpty(connectContext.getCurrentCatalog())) {
                throw new SemanticException("No catalog selected");
            }
            databaseName.setCatalog(connectContext.getCurrentCatalog());
        }

        if (Strings.isNullOrEmpty(databaseName.getDatabase())) {
            throw new SemanticException("Database name is null");
        }
    }

    public static void normalizationTableName(TableName tableName, ConnectContext connectContext) {
        if (Strings.isNullOrEmpty(tableName.getCatalog())) {
            if (Strings.isNullOrEmpty(connectContext.getCurrentCatalog())) {
                throw new SemanticException("No catalog selected");
            }
            tableName.setCatalog(connectContext.getCurrentCatalog());
        }

        if (Strings.isNullOrEmpty(tableName.getDb())) {
            if (Strings.isNullOrEmpty(connectContext.getDatabase())) {
                throw new SemanticException("No database selected");
            }
            tableName.setDb(connectContext.getDatabase());
        }

        if (Strings.isNullOrEmpty(tableName.getTbl())) {
            throw new SemanticException("Table name is null");
        }
    }
}
