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
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.automv.ast.AlterTunespaceClause;
import com.starrocks.sql.automv.ast.AlterTunespaceStmt;
import com.starrocks.sql.automv.ast.CreateTunespaceStmt;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.generator.PropertiesPolicy;
import com.starrocks.sql.automv.lattice.MVRecommendation;
import com.starrocks.sql.automv.lattice.MVRecommender;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pattern.PlanPiecePattern;
import com.starrocks.sql.automv.pattern.PlanPiecePatterns;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import com.starrocks.sql.automv.tunespace.PlanPieceInfo;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.OptExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
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

    public static List<MVRecommendation> recommend(MVRecommender.Type recommendType, String tsFqName,
                                                   ConnectContext context, int startIdx, int endIdx) {
        TablePlus table = PlanPieceInfo.getTable(tsFqName, 1, 1);
        List<String> items = table.getColumnPluses().stream()
                .map(columnPlus -> columnPlus.getColumn().getName())
                .collect(Collectors.toList());

        String selectSql = table.getSelectSql(items, null);
        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        List<PlanPieceInfo> pieceInfos =
                executor.query(PlanPieceInfo.class, PlanPieceInfo.getColumns(), context, selectSql);
        AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), context.getSessionVariable());

        MVRecommender mvRecommender = MVRecommender.createMVRecommender(recommendType, context, options);
        return Objects.requireNonNull(mvRecommender).recommendFromPieceInfos(pieceInfos, startIdx, endIdx);
    }

    public static final class TunespaceExecuteVisitor implements AstVisitorEPack<ShowResultSet, ConnectContext> {
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

        private void appendSPJGSubPlans(String fqTableName, String queryName, OptExpression logicalPlan,
                                        Map<String, FQTable> fqTableMap, ConnectContext context) {
            List<OptExpression> subPlans = PlanPiecePattern.extract(logicalPlan, PlanPiecePatterns.getSPJG());

            Optional<OptExpression> optAggRoot = PlanPiecePattern.getAggRoot(logicalPlan);
            boolean matchEntire = optAggRoot
                    .map(aggRoot -> subPlans.size() == 1 && subPlans.get(0) == aggRoot)
                    .orElse(false);

            if (queryName == null || queryName.isBlank() || queryName.isEmpty()) {
                queryName = "";
            }

            String qName = queryName;
            Supplier<String> nameGenerator = matchEntire || qName.equals("") ?
                    () -> qName :
                    Util.nextStringGenerator(qName + ".part.", "");

            List<Pair<String, OptExpression>> namedSubPlans = subPlans.stream()
                    .map(subPlan -> Pair.create(nameGenerator.get(), subPlan))
                    .collect(Collectors.toList());

            AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), context.getSessionVariable());
            List<PlanPieceInfo> pieceInfos = namedSubPlans.stream()
                    .map(namedSubPlan -> PlanPieceInfo.from(options, namedSubPlan.first, namedSubPlan.second, false,
                            fqTableMap))
                    .collect(Collectors.toList());
            if (pieceInfos.isEmpty()) {
                return;
            }
            String insertSql = PlanPieceInfo.getTable(fqTableName, 1, 1).getInsertSql(pieceInfos);
            try {
                exec(insertSql, InsertStmt.class, context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void append11MVSubPlans(String fqTableName, String queryName, OptExpression logicalPlan,
                                        Map<String, FQTable> fqTableMap, ConnectContext context) {
            List<OptExpression> subPlans = PlanPiecePattern.extract(logicalPlan, PlanPiecePatterns.get11MV());

            if (queryName == null || queryName.isBlank() || queryName.isEmpty()) {
                queryName = "";
            }

            String qName = queryName;
            Supplier<String> nameGenerator = qName.equals("") ?
                    () -> qName :
                    Util.nextStringGenerator(qName + ".11mv.part.", "");

            List<Pair<String, OptExpression>> namedSubPlans = subPlans.stream()
                    .filter(Predicate.not(Util::isSPJG))
                    .map(subPlan -> Pair.create(nameGenerator.get(), subPlan))
                    .collect(Collectors.toList());

            AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), context.getSessionVariable());
            List<PlanPieceInfo> pieceInfos = namedSubPlans.stream()
                    .map(namedSubPlan -> PlanPieceInfo.from11MV(options, namedSubPlan.first, namedSubPlan.second, false,
                            fqTableMap))
                    .collect(Collectors.toList());
            if (pieceInfos.isEmpty()) {
                return;
            }
            String insertSql = PlanPieceInfo.getTable(fqTableName, 1, 1).getInsertSql(pieceInfos);
            try {
                exec(insertSql, InsertStmt.class, context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private ShowResultSet handleAppendClause(String fqTableName, AlterTunespaceClause.AppendClause appendClause,
                                                 ConnectContext context) {
            String queryName = appendClause.getQueryName();
            QueryStatement queryStmt = appendClause.getQueryStatement().getQueryStatement();
            Map<String, FQTable> fqTableMap = appendClause.getQueryStatement().getFqTableMap();
            OptExpression logicalPlan = RboOptimizer.getLogicalPlan(queryStmt, context);
            appendSPJGSubPlans(fqTableName, queryName, logicalPlan, fqTableMap, context);
            append11MVSubPlans(fqTableName, queryName, logicalPlan, fqTableMap, context);
            return null;
        }

        private ShowResultSet handlePopulateFromLegacyMVClause(String fqTableName,
                                                               AlterTunespaceClause.PopulateFromLegacyMVClause clause,
                                                               ConnectContext context) {
            Preconditions.checkArgument(clause.getDb() != null);
            Database db = clause.getDb();
            List<MaterializedView> mvLists = Collections.emptyList();
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                mvLists = db.getTables().stream()
                        .filter(Table::isMaterializedView)
                        .map(table -> (MaterializedView) table)
                        .filter(MaterializedView::isActive)
                        .collect(Collectors.toList());
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }

            List<PlanPieceInfo> pieceInfos = Lists.newArrayListWithCapacity(mvLists.size());
            for (MaterializedView mv : mvLists) {
                TableName fqName = new TableName(db.getCatalogName(), db.getFullName(), mv.getName());
                MaterializedViewPlus mvPlus = MaterializedViewPlus.of(mv, fqName);
                Optional<PlanPiece> optPiece = RboOptimizer.getPlanPieceFromLegacyMV(mvPlus, context);
                if (!optPiece.isPresent()) {
                    continue;
                }
                PlanPiece piece = optPiece.get();
                PlanPieceInfo pieceInfo = PlanPieceInfo.fromLegacyMV(mvPlus, piece);
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
            String fqTableName = TableNamePlus.of(node.getTableName()).getFqName();
            int startIdx = node.getOffset().orElse(0L).intValue();
            int endIdx = node.getLimit().map(limit -> limit + startIdx).orElse((long) Integer.MAX_VALUE).intValue();
            Supplier<Integer> idAssigner = Util.nextIdGenerator();

            MVRecommender.Type recommendType = node.isSingle() ?
                    MVRecommender.Type.ONE_ONE_MV :
                    MVRecommender.Type.SPJG_MV;

            List<List<String>> showResults = recommend(recommendType, fqTableName, context, startIdx, endIdx).stream()
                    .map(rec -> rec.getRow(idAssigner))
                    .collect(Collectors.toList());

            int newStartIdx = Math.min(startIdx, showResults.size());
            int newEndIdx = Math.min(endIdx, showResults.size());
            showResults = showResults.subList(newStartIdx, newEndIdx);
            return new ShowResultSet(node.getMetaData(), showResults);
        }
    }
}
