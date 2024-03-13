// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupPrimaryStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRefreshStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupResumeStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSuspendStmt;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DatabaseName;
import com.starrocks.epack.sql.ast.DescribeFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropFailoverGroupStmt;
import com.starrocks.epack.sql.ast.ShowFailoverGroupsStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;

import java.util.List;
import java.util.Map;

public class FailoverGroupAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext context) {
        new FailoverGroupAnalyzerVisitor().analyze(statement, context);
    }

    static class FailoverGroupAnalyzerVisitor implements AstVisitorEPack<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCreatePrimaryFailoverGroupStatement(
                CreatePrimaryFailoverGroupStmt statement, ConnectContext context) {
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
                    normalizeDatabaseName(databaseName, context);
                }
            }

            List<TableName> tableNames = statement.getTableNames();
            if (tableNames != null) {
                for (TableName tableName : tableNames) {
                    normalizeTableName(tableName, context);
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

            String comment = statement.getComment();
            if (comment != null && comment.isEmpty()) {
                throw new SemanticException("Comment is empty");
            }

            Map<String, String> properties = statement.getProperties();
            if (properties != null) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    if (Strings.isNullOrEmpty(entry.getKey())) {
                        throw new SemanticException("Property key is empty");
                    }
                    if (Strings.isNullOrEmpty(entry.getValue())) {
                        throw new SemanticException("Property value is empty");
                    }
                }
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

        @Override
        public Void visitDropFailoverGroupStatement(DropFailoverGroupStmt statement, ConnectContext context) {
            String failoverGroupName = statement.getFailoverGroupName();
            if (Strings.isNullOrEmpty(failoverGroupName)) {
                throw new SemanticException("Failover group name is empty");
            }
            return null;
        }

        @Override
        public Void visitShowFailoverGroupsStatement(ShowFailoverGroupsStmt statement, ConnectContext context) {
            String pattern = statement.getPattern();
            if (pattern != null && pattern.isEmpty()) {
                throw new SemanticException("Failover group pattern is empty");
            }
            return null;
        }

        @Override
        public Void visitDescribeFailoverGroupStatement(DescribeFailoverGroupStmt statement, ConnectContext context) {
            String failoverGroupName = statement.getFailoverGroupName();
            if (Strings.isNullOrEmpty(failoverGroupName)) {
                throw new SemanticException("Failover group name is empty");
            }
            return null;
        }

        @Override
        public Void visitAlterFailoverGroupSetStatement(AlterFailoverGroupSetStmt statement, ConnectContext context) {
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
                    normalizeDatabaseName(databaseName, context);
                }
            }

            List<TableName> tableNames = statement.getTableNames();
            if (tableNames != null) {
                for (TableName tableName : tableNames) {
                    normalizeTableName(tableName, context);
                }
            }

            List<String> members = statement.getMembers();
            if (members != null) {
                if (members.size() < 2) {
                    throw new SemanticException("No enough members");
                }
                for (String member : members) {
                    if (Strings.isNullOrEmpty(member)) {
                        throw new SemanticException("Member is empty");
                    }
                }
            }

            String schedule = statement.getSchedule();
            if (schedule != null && schedule.isEmpty()) {
                throw new SemanticException("Schedule is empty");
            }

            String comment = statement.getComment();
            if (comment != null && comment.isEmpty()) {
                throw new SemanticException("Comment is empty");
            }

            Map<String, String> properties = statement.getProperties();
            if (properties != null) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    if (Strings.isNullOrEmpty(entry.getKey())) {
                        throw new SemanticException("Property key is empty");
                    }
                    if (Strings.isNullOrEmpty(entry.getValue())) {
                        throw new SemanticException("Property value is empty");
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitAlterFailoverGroupAddStatement(AlterFailoverGroupAddStmt statement, ConnectContext context) {
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
                    normalizeDatabaseName(databaseName, context);
                }
            }

            List<TableName> tableNames = statement.getTableNames();
            if (tableNames != null) {
                for (TableName tableName : tableNames) {
                    normalizeTableName(tableName, context);
                }
            }

            List<String> members = statement.getMembers();
            if (members != null) {
                for (String member : members) {
                    if (Strings.isNullOrEmpty(member)) {
                        throw new SemanticException("Member is empty");
                    }
                }
            }

            Map<String, String> properties = statement.getProperties();
            if (properties != null) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    if (Strings.isNullOrEmpty(entry.getKey())) {
                        throw new SemanticException("Property key is empty");
                    }
                    if (Strings.isNullOrEmpty(entry.getValue())) {
                        throw new SemanticException("Property value is empty");
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitAlterFailoverGroupRemoveStatement(AlterFailoverGroupRemoveStmt statement,
                                                           ConnectContext context) {
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
                    normalizeDatabaseName(databaseName, context);
                }
            }

            List<TableName> tableNames = statement.getTableNames();
            if (tableNames != null) {
                for (TableName tableName : tableNames) {
                    normalizeTableName(tableName, context);
                }
            }

            List<String> members = statement.getMembers();
            if (members != null) {
                for (String member : members) {
                    if (Strings.isNullOrEmpty(member)) {
                        throw new SemanticException("Member is empty");
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitAlterFailoverGroupRefreshStatement(AlterFailoverGroupRefreshStmt statement,
                                                            ConnectContext context) {
            String failoverGroupName = statement.getFailoverGroupName();
            if (Strings.isNullOrEmpty(failoverGroupName)) {
                throw new SemanticException("Failover group name is empty");
            }
            return null;
        }

        @Override
        public Void visitAlterFailoverGroupPrimaryStatement(AlterFailoverGroupPrimaryStmt statement,
                                                            ConnectContext context) {
            String failoverGroupName = statement.getFailoverGroupName();
            if (Strings.isNullOrEmpty(failoverGroupName)) {
                throw new SemanticException("Failover group name is empty");
            }
            return null;
        }

        @Override
        public Void visitAlterFailoverGroupSuspendStatement(AlterFailoverGroupSuspendStmt statement,
                                                            ConnectContext context) {
            String failoverGroupName = statement.getFailoverGroupName();
            if (Strings.isNullOrEmpty(failoverGroupName)) {
                throw new SemanticException("Failover group name is empty");
            }
            return null;
        }

        @Override
        public Void visitAlterFailoverGroupResumeStatement(AlterFailoverGroupResumeStmt statement,
                                                           ConnectContext context) {
            String failoverGroupName = statement.getFailoverGroupName();
            if (Strings.isNullOrEmpty(failoverGroupName)) {
                throw new SemanticException("Failover group name is empty");
            }
            return null;
        }
    }

    private static void normalizeDatabaseName(DatabaseName databaseName, ConnectContext connectContext) {
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

    private static void normalizeTableName(TableName tableName, ConnectContext connectContext) {
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
