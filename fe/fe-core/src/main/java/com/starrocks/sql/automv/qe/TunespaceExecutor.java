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

package com.starrocks.sql.automv.qe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.automv.ast.AlterTunespaceClause;
import com.starrocks.sql.automv.ast.AlterTunespaceStmt;
import com.starrocks.sql.automv.ast.CreateTunespaceStmt;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.generator.PropertiesPolicy;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pattern.PlanPiecePattern;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import com.starrocks.sql.automv.tunespace.PlanPieceInfo;
import com.starrocks.sql.optimizer.OptExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TunespaceExecutor {
    private static final TunespaceExecuteVisitor INSTANCE = new TunespaceExecuteVisitor();

    public static boolean isTunespaceStmt(StatementBase stmt) {
        return (stmt instanceof CreateTunespaceStmt) ||
                (stmt instanceof AlterTunespaceStmt) ||
                (stmt instanceof ShowRecommendationsStmt);
    }

    public static ShowResultSet execute(StatementBase stmt, ConnectContext context) {
        return INSTANCE.visit(stmt, context);
    }

    public static final class TunespaceExecuteVisitor implements AstVisitor<ShowResultSet, ConnectContext> {
        public void exec(String sql, Class<?> klass, ConnectContext context) throws Exception {
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, context.getSessionVariable());
            Preconditions.checkArgument(stmts.size() == 1 && stmts.get(0).getClass().equals(klass));
            StmtExecutor executor = new StmtExecutor(context, stmts.get(0));
            executor.execute();
        }

        @Override
        public ShowResultSet visitCreateTunespaceStmt(CreateTunespaceStmt stmt, ConnectContext context) {
            try {
                int replicationNum = PropertiesPolicy.calcReplicationNum();
                String fqName = TableNamePlus.of(stmt.getTableName()).getFqName();
                TablePlus table = PlanPieceInfo.getTable(fqName, 10, replicationNum);
                exec(table.getCreateTableSql(), CreateTableStmt.class, context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public ShowResultSet visitAlterTunespaceStmt(AlterTunespaceStmt stmt, ConnectContext context) {
            String fqTableName = TableNamePlus.of(stmt.getTableName()).getFqName();
            if (stmt.getAlterClause() instanceof AlterTunespaceClause.AppendClause) {
                return handleAppendClause(fqTableName,
                        (AlterTunespaceClause.AppendClause) stmt.getAlterClause(), context);
            } else if (stmt.getAlterClause() instanceof AlterTunespaceClause.PopulateFromLegacyMVClause) {
                return handlePopulateFromLegacyMVClause(fqTableName,
                        (AlterTunespaceClause.PopulateFromLegacyMVClause) stmt.getAlterClause(), context);
            } else if (stmt.getAlterClause() instanceof AlterTunespaceClause.PopulateFromTunespaceClause) {
                return handlePopulateFromTunespaceClause(fqTableName,
                        (AlterTunespaceClause.PopulateFromTunespaceClause) stmt.getAlterClause(), context);
            } else if (stmt.getAlterClause() instanceof AlterTunespaceClause.PopulateAsQueryClause) {
                throw new SemanticException("Not support");
            }
            return null;
        }

        private ShowResultSet handleAppendClause(String fqTableName, AlterTunespaceClause.AppendClause appendClause,
                                                 ConnectContext context) {
            QueryStatement queryStmt = appendClause.getQueryStatement().getQueryStatement();
            Map<String, FQTable> fqTableMap = appendClause.getQueryStatement().getFqTableMap();
            List<OptExpression> subPlans = RboOptimizer.getSubPlans(queryStmt, context, PlanPiecePattern.getSPJG());
            AutoMVOptions options = AutoMVOptions.of();
            List<PlanPieceInfo> pieceInfos = subPlans.stream()
                    .map(subPlan -> PlanPieceInfo.from(options, subPlan, false, fqTableMap))
                    .collect(Collectors.toList());
            if (pieceInfos.isEmpty()) {
                return null;
            }
            String insertSql = PlanPieceInfo.getTable(fqTableName, 1, 1).getInsertSql(pieceInfos);
            try {
                exec(insertSql, InsertStmt.class, context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        private ShowResultSet handlePopulateFromLegacyMVClause(String fqTableName,
                                                               AlterTunespaceClause.PopulateFromLegacyMVClause clause,
                                                               ConnectContext context) {
            Preconditions.checkArgument(clause.getDb() != null);
            Database db = clause.getDb();
            List<MaterializedView> mvLists = Collections.emptyList();
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                mvLists = db.getTables().stream()
                        .filter(Table::isMaterializedView)
                        .map(table -> (MaterializedView) table)
                        .filter(MaterializedView::isActive)
                        .collect(Collectors.toList());
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }

            List<PlanPieceInfo> pieceInfos = Lists.newArrayListWithCapacity(mvLists.size());
            for (MaterializedView mv : mvLists) {
                TableName fqName = new TableName(db.getCatalogName(), db.getFullName(), mv.getName());
                MaterializedViewPlus mvPlus = MaterializedViewPlus.of(mv, fqName);
                String createMvSql = mvPlus.getCreateMaterializedViewSql();
                StatementBase stmt = RboOptimizer.parseAndAnalyze(context, createMvSql);
                Preconditions.checkArgument(stmt instanceof CreateMaterializedViewStatement);
                CreateMaterializedViewStatement createMvStmt = (CreateMaterializedViewStatement) stmt;
                QueryStatement queryStmt = createMvStmt.getQueryStatement();
                QueryStatementPlus queryStmtPlus = RboOptimizer.collectFQTables(queryStmt, context);
                Map<String, FQTable> fqTableMap = queryStmtPlus.getFqTableMap();

                Optional<OptExpression> optEntirePlan =
                        RboOptimizer.getEntirePlan(queryStmt, context, PlanPiecePattern.getSPJG());
                if (!optEntirePlan.isPresent()) {
                    continue;
                }
                OptExpression entirePlan = optEntirePlan.get();
                PlanPieceInfo pieceInfo = PlanPieceInfo.fromLegacyMV(mvPlus, entirePlan, fqTableMap);
                pieceInfos.add(pieceInfo);

            }
            if (!pieceInfos.isEmpty()) {
                String insertSql = PlanPieceInfo.getTable(fqTableName, 1, 1).getInsertSql(pieceInfos);
                try {
                    exec(insertSql, InsertStmt.class, context);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        }

        private ShowResultSet handlePopulateFromTunespaceClause(String fqTableName,
                                                                AlterTunespaceClause.PopulateFromTunespaceClause clause,
                                                                ConnectContext context) {
            Preconditions.checkArgument(clause.getSrcTableName() != null);
            TablePlus dstTable = PlanPieceInfo.getTable(fqTableName, 1, 1);
            String srcFqTableName = TableNamePlus.of(clause.getSrcTableName()).getFqName();
            String insertAsSelectSql = dstTable.getInsertAsSelectSql(srcFqTableName);
            try {
                exec(insertAsSelectSql, InsertStmt.class, context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public ShowResultSet visitShowRecommendationsStmt(ShowRecommendationsStmt node, ConnectContext context) {
            return new ShowResultSet(node.getMetaData(), Collections.emptyList());
        }
    }
}
