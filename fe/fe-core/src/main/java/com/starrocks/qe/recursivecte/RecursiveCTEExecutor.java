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

package com.starrocks.qe.recursivecte;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.TableName;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.DropTemporaryTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.type.IntegerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class RecursiveCTEExecutor {
    private static final Logger LOG = LogManager.getLogger(RecursiveCTEExecutor.class);

    private record RecursiveCTEGroup(QueryStatement startStmt, QueryStatement recursiveStmt, boolean isDistinct,
                                     List<CTERelation> nonRecursiveCTEs, CreateTemporaryTableStmt tempTableStmt) {
    }

    private final Map<String, RecursiveCTEGroup> recursiveCTEGroups = Maps.newLinkedHashMap();

    private final Map<String, TableName> cteTempTableMap = Maps.newHashMap();

    private final ConnectContext connectContext;

    public RecursiveCTEExecutor(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    public StatementBase splitOuterStmt(StatementBase stmt) {
        analyze(stmt, connectContext);
        RecursiveCTESplitter splitter = new RecursiveCTESplitter();
        splitter.visit(stmt, null);
        return stmt;
    }

    public String explainCTE(StatementBase stmt) {
        StringBuilder sb = new StringBuilder();
        for (String cteName : recursiveCTEGroups.keySet()) {
            RecursiveCTEGroup group = recursiveCTEGroups.get(cteName);
            sb.append("Recursive CTE Name: ").append(cteName).append("\n");
            sb.append("Temporary Table: ").append(group.tempTableStmt.getTableName()).append("\n");
            sb.append("Start Statement: ").append(AstToSQLBuilder.toSQL(group.startStmt)).append("\n");
            sb.append("Recursive Statement: ").append(AstToSQLBuilder.toSQL(group.recursiveStmt)).append("\n");
            sb.append("--------------------------------------------------\n");
        }

        sb.append("Outer Statement After Rewriting:\n");
        sb.append(AstToSQLBuilder.toSQL(stmt)).append("\n");
        return sb.toString();
    }


    private void analyze(StatementBase stmt, ConnectContext context) {
        try (PlannerMetaLocker locker = new PlannerMetaLocker(context, stmt)) {
            locker.lock();
            Analyzer.analyze(stmt, context);
        }
    }

    public void prepareRecursiveCTE() throws Exception {
        try {
            prepareRecursiveCTEImpl();
        } catch (Exception e) {
            LOG.warn("Error occurred during preparing recursive CTE", e);
            throw e;
        }
    }

    private void prepareRecursiveCTEImpl() throws Exception {
        StmtExecutor executor;
        for (String cteName : recursiveCTEGroups.keySet()) {
            RecursiveCTEGroup group = recursiveCTEGroups.get(cteName);
            TableName tempTableName = cteTempTableMap.get(cteName);
            // temp table creation
            analyze(group.tempTableStmt, connectContext);
            DDLStmtExecutor.execute(group.tempTableStmt, connectContext);

            // start plan
            InsertStmt insert = new InsertStmt(tempTableName, group.startStmt);
            insert.setOrigStmt(new OriginStatement(AstToSQLBuilder.toSQL(insert)));
            executor = StmtExecutor.newInternalExecutor(connectContext, insert);
            connectContext.setQueryId(UUIDUtil.genUUID());
            executor.execute();
            if (connectContext.getState().getErrType() != QueryState.ErrType.UNKNOWN) {
                LOG.warn("Error occurred during executing recursive CTE start statement: {}",
                        connectContext.getState().getErrorMessage());
                return;
            }

            // recursive plan
            RecursiveCTEStager stager = new RecursiveCTEStager(group, tempTableName);
            for (int i = 1; i < connectContext.getSessionVariable().getRecursiveCteMaxDepth(); i++) {
                insert = stager.next();
                executor = StmtExecutor.newInternalExecutor(connectContext, insert);
                connectContext.setQueryId(UUIDUtil.genUUID());
                executor.execute();
                if (connectContext.getState().getAffectedRows() <= 0) {
                    // no more rows inserted, break
                    break;
                }
                if (connectContext.getState().getErrType() != QueryState.ErrType.UNKNOWN) {
                    LOG.warn("Error occurred during executing recursive CTE recursive statement: {}",
                            connectContext.getState().getErrorMessage());
                    return;
                }
            }
        }
    }

    public void finalizeRecursiveCTE() {
        // drop temp tables
        if (!connectContext.getSessionVariable().isRecursiveCteFinalizeTemporalTable()) {
            return;
        }
        for (TableName tempTableName : cteTempTableMap.values()) {
            try {
                DropTemporaryTableStmt dropStmt = new DropTemporaryTableStmt(true, tempTableName, true);
                analyze(dropStmt, connectContext);
                DDLStmtExecutor.execute(dropStmt, connectContext);
            } catch (Exception e) {
                LOG.warn("Error occurred during dropping recursive CTE temporary table: {}", tempTableName, e);
            }
        }
    }

    private static class RecursiveCTEStager extends AstTraverser<Void, Void> {
        private int currentLoops;
        private final TableName tempTableName;
        private final RecursiveCTEGroup group;
        private Expr tempPredicate = null;
        private boolean initPredicate = false;

        public RecursiveCTEStager(RecursiveCTEGroup group, TableName tempTableName) {
            this.currentLoops = 0;
            this.tempTableName = tempTableName;
            this.group = group;
        }

        public InsertStmt next() {
            visit(group.recursiveStmt.getQueryRelation());
            currentLoops++;
            SubqueryRelation subquery = new SubqueryRelation(new QueryStatement(group.recursiveStmt.getQueryRelation()));
            subquery.setAlias(new TableName(null, "loops_" + System.currentTimeMillis()));
            SelectRelation recursiveSelect = new SelectRelation(new SelectList(List.of(
                    new SelectListItem(subquery.getAlias()),
                    new SelectListItem(new IntLiteral(currentLoops, IntegerType.INT), "_cte_level")),
                    group.isDistinct), subquery, null, null, null);
            recursiveSelect.getCteRelations().addAll(group.nonRecursiveCTEs);

            InsertStmt insert = new InsertStmt(tempTableName, new QueryStatement(recursiveSelect));
            insert.setOrigStmt(new OriginStatement(AstToSQLBuilder.toSQL(insert)));
            return insert;
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            super.visitSelect(node, context);
            if (node.getRelation() instanceof TableRelation tableRelation) {
                if (tempTableName.equals(tableRelation.getName())) {
                    // add predicate to filter current level
                    IntLiteral levelLiteral = new IntLiteral(currentLoops);
                    Expr equalsLevel = new BinaryPredicate(BinaryType.EQ,
                            new SlotRef(tableRelation.getName(), "_cte_level"), levelLiteral);
                    if (!this.initPredicate) {
                        this.initPredicate = true;
                        this.tempPredicate = node.getPredicate();
                    }
                    if (this.tempPredicate == null) {
                        node.setPredicate(equalsLevel);
                    } else {
                        Expr newPredicate =
                                new CompoundPredicate(CompoundPredicate.Operator.AND, this.tempPredicate, equalsLevel);
                        node.setPredicate(newPredicate);
                    }
                }
            }
            return null;
        }
    }

    private class RecursiveCTESplitter extends AstTraverser<Void, Void> {
        private final List<CTERelation> nonRecursiveCTEs = Lists.newArrayList();

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.hasWithClause()) {
                List<CTERelation> cteRelations = Lists.newArrayList();
                for (CTERelation cteRelation : node.getCteRelations()) {
                    if (cteRelation.isRecursive()) {
                        visit(cteRelation, context);
                    } else {
                        nonRecursiveCTEs.add(cteRelation);
                        cteRelations.add(cteRelation);
                    }
                }
                node.getCteRelations().clear();
                node.getCteRelations().addAll(cteRelations);
            }

            if (node.getOrderBy() != null) {
                for (OrderByElement orderByElement : node.getOrderBy()) {
                    visit(orderByElement.getExpr(), context);
                }
            }

            if (node.getOutputExpression() != null) {
                node.getOutputExpression().forEach(x -> visit(x, context));
            }

            if (node.getPredicate() != null) {
                visit(node.getPredicate(), context);
            }

            if (node.getGroupBy() != null) {
                node.getGroupBy().forEach(x -> visit(x, context));
            }

            if (node.getAggregate() != null) {
                node.getAggregate().forEach(x -> visit(x, context));
            }

            if (node.getHaving() != null) {
                visit(node.getHaving(), context);
            }

            node.setRelation(rewriteCTERelation(node.getRelation(), context));
            return null;
        }

        private Relation rewriteCTERelation(Relation relation, Void context) {
            if (relation instanceof CTERelation cteRelation && cteRelation.isRecursive()
                    && recursiveCTEGroups.containsKey(cteRelation.getName())) {
                CreateTemporaryTableStmt tempTableStmt = recursiveCTEGroups.get(cteRelation.getName()).tempTableStmt;
                TableName name = tempTableStmt.getDbTbl();

                List<SelectListItem> selectItems = Lists.newArrayList();
                for (int i = 0; i < tempTableStmt.getColumnDefs().size() - 1; i++) {
                    selectItems.add(new SelectListItem(new SlotRef(name, tempTableStmt.getColumnDefs().get(i).getName()), null));
                }
                SelectRelation selectRelation = new SelectRelation(
                        new SelectList(selectItems, false),
                        new TableRelation(name, null, Lists.newArrayList(), Lists.newArrayList()), null, null, null);
                SubqueryRelation subquery = new SubqueryRelation(new QueryStatement(selectRelation));
                if (relation.getAlias() != null) {
                    subquery.setAlias(relation.getAlias());
                } else {
                    subquery.setAlias(new TableName(null, cteRelation.getName()));
                }
                return subquery;
            } else {
                visit(relation, context);
                return relation;
            }
        }

        @Override
        public Void visitJoin(JoinRelation node, Void context) {
            node.setLeft(rewriteCTERelation(node.getLeft(), context));
            node.setRight(rewriteCTERelation(node.getRight(), context));
            return null;
        }

        @Override
        public Void visitCTE(CTERelation node, Void context) {
            if (!node.isRecursive() || !node.isAnchor()) {
                return null;
            }

            if (!(node.getCteQueryStatement().getQueryRelation() instanceof UnionRelation unionRelation)) {
                throw new SemanticException("Recursive CTE must be a UNION ALL of anchor and recursive member.");
            }
            if (unionRelation.getRelations().size() < 2) {
                throw new SemanticException("Recursive CTE must have at least anchor and recursive member.");
            }

            // create temporary table for recursive cte
            List<ColumnDef> columnDefs = Lists.newArrayList();
            for (Field allField : node.getRelationFields().getAllFields()) {
                columnDefs.add(new ColumnDef(allField.getName(), new TypeDef(allField.getType()), true));
            }
            columnDefs.add(new ColumnDef("_cte_level", new TypeDef(IntegerType.INT), true));

            Map<String, String> prop = Maps.newHashMap();
            prop.put("replication_num", "1");
            TableName tempTableName =
                    new TableName(connectContext.getDatabase(), node.getName() + "_" + System.currentTimeMillis());
            CreateTemporaryTableStmt tempTableStmt = new CreateTemporaryTableStmt(false, false,
                    tempTableName, columnDefs, List.of(), "OLAP", "utf8",
                    new KeysDesc(KeysType.DUP_KEYS, Lists.newArrayList(columnDefs.get(0).getName())),
                    null, new RandomDistributionDesc(), prop, Maps.newHashMap(),
                    "Temporary Table for Recursive CTE", Lists.newArrayList(), null);

            // split recursive cte
            boolean isDistinct = SetQualifier.DISTINCT.equals(unionRelation.getQualifier());
            SubqueryRelation subquery = new SubqueryRelation(new QueryStatement(unionRelation.getRelations().get(0)));
            subquery.setAlias(new TableName(null, "anchar_" + System.currentTimeMillis()));
            SelectRelation startSelect = new SelectRelation(
                    new SelectList(List.of(new SelectListItem(subquery.getAlias()),
                            new SelectListItem(new IntLiteral(0, IntegerType.INT), "_cte_level")), isDistinct),
                    subquery, null, null, null);
            startSelect.getCteRelations().addAll(nonRecursiveCTEs);

            QueryStatement start = new QueryStatement(startSelect);
            QueryStatement recursive = new QueryStatement(unionRelation.getRelations().get(1));

            recursiveCTEGroups.put(node.getName(),
                    new RecursiveCTEGroup(start, recursive, isDistinct, List.copyOf(nonRecursiveCTEs), tempTableStmt));
            cteTempTableMap.put(node.getName(), tempTableName);

            visit(start.getQueryRelation());
            visit(recursive.getQueryRelation());
            return null;
        }

    }
}
