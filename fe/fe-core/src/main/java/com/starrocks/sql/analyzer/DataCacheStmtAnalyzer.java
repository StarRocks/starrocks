// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ClearDataCacheRulesStmt;
import com.starrocks.sql.ast.CreateDataCacheRuleStmt;
import com.starrocks.sql.ast.DropDataCacheRuleStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DataCacheStmtAnalyzer {
    private DataCacheStmtAnalyzer() {
    }

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new DataCacheStmtAnalyzerVisitor().analyze(stmt, session);
    }

    static class DataCacheStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        private final DataCacheMgr dataCacheMgr = DataCacheMgr.getInstance();

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateDataCacheRuleStatement(CreateDataCacheRuleStmt statement, ConnectContext context) {
            int priority = statement.getPriority();
            if (priority != -1) {
                throw new SemanticException("DataCache only support priority = -1 (aka BlackList) now");
            }

            Map<String, String> properties = statement.getProperties();
            if (properties != null) {
                throw new SemanticException("DataCache don't support specify properties now");
            }

            List<String> parts = statement.getTarget().getParts();

            // check target existed
            String catalogName = parts.get(0);
            String dbName = parts.get(1);
            String tblName = parts.get(2);

            if (CatalogMgr.isInternalCatalog(catalogName)) {
                throw new SemanticException("DataCache only support external catalog now");
            }

            throwExceptionIfTargetIsInvalid(catalogName, dbName, tblName);

            // If catalog/db/tbl does not exist, it will throw exception
            Optional<Table> optionalTable = getTable(catalogName, dbName, tblName);

            // Check new dataCache rule is conflicted with existed rule
            dataCacheMgr.throwExceptionIfRuleIsConflicted(catalogName, dbName, tblName);

            Expr predicates = statement.getPredicates();
            if (predicates != null) {
                if (!optionalTable.isPresent()) {
                    throw new SemanticException("You must have a specific table when using where clause");
                }
                // Build scope
                ImmutableList.Builder<Field> fields = ImmutableList.builder();
                TableName tableName = new TableName(catalogName, dbName, tblName);
                for (Column column : optionalTable.get().getColumns()) {
                    Field field = new Field(column.getName(), column.getType(), tableName,
                            new SlotRef(tableName, column.getName(), column.getName()), true, column.isAllowNull());
                    fields.add(field);
                }
                Scope scope = new Scope(RelationId.anonymous(), new RelationFields(fields.build()));
                ExpressionAnalyzer.analyzeExpression(predicates, new AnalyzeState(), scope, null);
            }

            return null;
        }

        @Override
        public Void visitDropDataCacheRuleStatement(DropDataCacheRuleStmt statement, ConnectContext context) {
            long cacheRuleId = statement.getCacheRuleId();
            if (!dataCacheMgr.isExistCacheRule(cacheRuleId)) {
                throw new SemanticException(String.format("DataCache rule id = %d does not exist", cacheRuleId));
            }
            return null;
        }

        @Override
        public Void visitClearDataCacheRulesStatement(ClearDataCacheRulesStmt statement, ConnectContext context) {
            return null;
        }
    }

    private static boolean isSelectAll(String s) {
        return s.equals("*");
    }

    // If catalogName is '*', dbName and tblName must use '*' either
    // If dbName is '*', tblName must use '*'
    private static void throwExceptionIfTargetIsInvalid(String catalogName, String dbName, String tblName) throws
            SemanticException {
        // check validity
        if (isSelectAll(catalogName)) {
            if (!isSelectAll(dbName) || !isSelectAll(tblName)) {
                throw new SemanticException("Catalog is *, database and table must use * either");
            }
            // *.*.* will go here, return directly, don't need to check dbName anymore
            return;
        }

        if (isSelectAll(dbName)) {
            if (!isSelectAll(tblName)) {
                throw new SemanticException("Database is *, table must use * either");
            }
        }
    }

    private static Optional<Table> getTable(String catalogName, String dbName, String tblName) throws SemanticException {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        // Check target is existed
        Table table = null;
        if (!isSelectAll(catalogName)) {
            // Check catalog is existed
            if (!metadataMgr.getOptionalMetadata(catalogName).isPresent()) {
                throw new SemanticException(String.format("DataCache target catalog: %s does not exist", catalogName));
            }

            if (!isSelectAll(dbName)) {
                // Check db is existed
                Database db = metadataMgr.getDb(catalogName, dbName);
                if (db == null) {
                    throw new SemanticException(String.format("DataCache target database: %s does not exist " +
                            "in [catalog: %s]", dbName, catalogName));
                }
                if (!isSelectAll(tblName)) {
                    // Check tbl is existed
                    table = metadataMgr.getTable(catalogName, dbName, tblName);
                    if (table == null) {
                        throw new SemanticException(String.format("DataCache target table: %s does not exist in " +
                                "[catalog: %s, database: %s]", tblName, catalogName, dbName));
                    }
                }
            }
        }
        return Optional.ofNullable(table);
    }
}
