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

package com.starrocks.sql.automv.analysis;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.automv.ast.AlterTunespaceClause;
import com.starrocks.sql.automv.ast.AlterTunespaceStmt;
import com.starrocks.sql.automv.ast.CreateTunespaceStmt;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.qe.CollectAstVisitor;
import com.starrocks.sql.automv.qe.QueryStatementPlus;
import com.starrocks.sql.common.MetaUtils;

import java.util.Optional;
import java.util.function.Function;

import static com.starrocks.common.ErrorReport.reportCommon;

public class TunespaceAnalyzer {
    public static final TunespaceAnalyzeVisitor INSTANCE = new TunespaceAnalyzeVisitor();

    public static Void analyze(StatementBase node, ConnectContext context) {
        return INSTANCE.visit(node, context);
    }

    private static final class TunespaceAnalyzeVisitor implements AstVisitor<Void, ConnectContext> {
        private static void analyzeAndCheckFullQualifiedTableName(TableName tableName, ConnectContext context,
                                                                  Function<Optional<Table>, Optional<SemanticException>> check) {
            MetaUtils.normalizationTableName(context, tableName);

            final String simpleTblName = tableName.getTbl();
            FeNameFormat.checkTableName(simpleTblName);

            final String catalogName = tableName.getCatalog();
            MetaUtils.checkCatalogExistAndReport(catalogName);
            Database db = MetaUtils.getDatabase(catalogName, tableName.getDb());
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                Optional<SemanticException> optError = check.apply(Optional.ofNullable(db.getTable(simpleTblName)));
                if (optError.isPresent()) {
                    throw optError.get();
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }

        private static Function<Optional<Table>, Optional<SemanticException>> checkTableExists(String simpleTblName) {
            return optTable -> {
                if (optTable.isPresent()) {
                    return Optional.empty();
                } else {
                    return Optional.of(new SemanticException("Table %s is not found", simpleTblName));
                }
            };
        }

        @Override
        public Void visitCreateTunespaceStmt(CreateTunespaceStmt node, ConnectContext context) {
            Function<Optional<Table>, Optional<SemanticException>> check = optTable -> {
                if (node.isIfNotExists() || !optTable.isPresent()) {
                    return Optional.empty();
                } else {
                    return Optional.of(new SemanticException(
                            reportCommon(null, ErrorCode.ERR_TABLE_EXISTS_ERROR, node.getTableName().getTbl())));
                }
            };
            analyzeAndCheckFullQualifiedTableName(node.getTableName(), context, check);
            return null;
        }

        @Override
        public Void visitAlterTunespaceStmt(AlterTunespaceStmt node, ConnectContext context) {
            TableName tableName = node.getTableName();
            String simpleTblName = tableName.getTbl();
            analyzeAndCheckFullQualifiedTableName(tableName, context, checkTableExists(simpleTblName));
            if (node.getAlterClause() instanceof AlterTunespaceClause.AppendClause) {
                AlterTunespaceClause.AppendClause appendClause =
                        (AlterTunespaceClause.AppendClause) node.getAlterClause();
                QueryStatement queryStmt = appendClause.getQueryStatement().getQueryStatement();
                Analyzer.analyze(queryStmt, context);
                QueryStatementPlus newQueryStmtPlus = CollectAstVisitor.collectFQTables(queryStmt, context);
                appendClause.setQueryStatement(newQueryStmtPlus);
            } else if (node.getAlterClause() instanceof AlterTunespaceClause.PopulateFromLegacyMVClause) {
                AlterTunespaceClause.PopulateFromLegacyMVClause populateFromLegacyMVClause =
                        (AlterTunespaceClause.PopulateFromLegacyMVClause) node.getAlterClause();
                populateFromLegacyMVClause.setDb(analyzeDbName(populateFromLegacyMVClause.getDbName(), context));

            } else if (node.getAlterClause() instanceof AlterTunespaceClause.PopulateFromTunespaceClause) {
                AlterTunespaceClause.PopulateFromTunespaceClause populateFromTunespaceClause =
                        (AlterTunespaceClause.PopulateFromTunespaceClause) node.getAlterClause();
                TableName srcTableName = populateFromTunespaceClause.getSrcTableName();
                analyzeAndCheckFullQualifiedTableName(srcTableName, context, checkTableExists(srcTableName.getTbl()));
                if (tableName.equals(srcTableName)) {
                    throw new SemanticException(
                            "Both destination and source tunespaces are same: name=" + tableName.toSql());
                }
            } else if (node.getAlterClause() instanceof AlterTunespaceClause.PopulateAsQueryClause) {
                AlterTunespaceClause.PopulateAsQueryClause populateAsQueryClause =
                        (AlterTunespaceClause.PopulateAsQueryClause) node.getAlterClause();
                Analyzer.analyze(populateAsQueryClause.getQueryStatement(), context);
            }
            return null;
        }

        private Database analyzeDbName(QualifiedName dbName, ConnectContext context) {
            String catalogName;
            String dbSimpleName;
            if (dbName.getParts().size() == 2) {
                catalogName = dbName.getParts().get(0);
                dbSimpleName = dbName.getParts().get(1);
            } else if (dbName.getParts().size() == 1) {
                catalogName = context.getCurrentCatalog();
                dbSimpleName = dbName.getParts().get(0);
            } else {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
                return null;
            }

            if (CatalogMgr.isInternalCatalog(catalogName)) {
                return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbSimpleName);
            } else {
                return MetaUtils.getDatabase(catalogName, dbSimpleName);
            }
        }

        @Override
        public Void visitShowRecommendationsStmt(ShowRecommendationsStmt node, ConnectContext context) {
            TableName tableName = node.getTableName();
            String simpleTblName = tableName.getTbl();
            analyzeAndCheckFullQualifiedTableName(tableName, context, checkTableExists(simpleTblName));
            node.setTableName(tableName);
            return null;
        }
    }
}
