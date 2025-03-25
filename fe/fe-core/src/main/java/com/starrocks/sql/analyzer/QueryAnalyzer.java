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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.authorization.SecurityPolicyRewriteRule;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ConnectorView;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PivotAggregation;
import com.starrocks.sql.ast.PivotRelation;
import com.starrocks.sql.ast.PivotValue;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.dump.HiveMetaStoreTableDumpInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.sql.analyzer.AstToStringBuilder.getAliasName;
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;
import static com.starrocks.thrift.PlanNodesConstants.BINLOG_OP_COLUMN_NAME;
import static com.starrocks.thrift.PlanNodesConstants.BINLOG_SEQ_ID_COLUMN_NAME;
import static com.starrocks.thrift.PlanNodesConstants.BINLOG_TIMESTAMP_COLUMN_NAME;
import static com.starrocks.thrift.PlanNodesConstants.BINLOG_VERSION_COLUMN_NAME;

public class QueryAnalyzer {
    private final ConnectContext session;
    private final MetadataMgr metadataMgr;

    public QueryAnalyzer(ConnectContext session) {
        this.session = session;
        this.metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
    }

    public void analyze(StatementBase node) {
        new Visitor().process(node, new Scope(RelationId.anonymous(), new RelationFields()));
    }

    public void analyze(StatementBase node, Scope parent) {
        new Visitor().process(node, parent);
    }

    private class GeneratedColumnExprMappingCollector implements AstVisitor<Void, Scope> {
        public GeneratedColumnExprMappingCollector() {
        }

        public Void process(ParseNode node, Scope scope) {
            if (session.getSessionVariable().isDisableGeneratedColumnRewrite()) {
                return null;
            }
            return node.accept(this, scope);
        }

        private void reAnalyzeExpressionBasedOnCurrentScope(SelectRelation childSelectRelation, Scope scope,
                                                            Map<Expr, SlotRef> resultGeneratedExprToColumnRef) {
            if (childSelectRelation.getGeneratedExprToColumnRef() == null ||
                    childSelectRelation.getGeneratedExprToColumnRef().isEmpty()) {
                return;
            }
            // 1. get all available generated column from child selectRelation
            // available means that:
            // a. generated column output from child selectRelation directly.
            // b. all reference column of generated column output from child selectRelation directly.
            List<SlotRef> outputSlotRef = childSelectRelation.getOutputExpression()
                                            .stream().filter(e -> e instanceof SlotRef)
                                            .map(e -> (SlotRef) e).collect(Collectors.toList());
            boolean hasStar = childSelectRelation.getSelectList()
                                        .getItems().stream().anyMatch(SelectListItem::isStar);
            Map<Expr, SlotRef> generatedExprToColumnRef = new HashMap<>();
            for (Map.Entry<Expr, SlotRef> entry : childSelectRelation.getGeneratedExprToColumnRef().entrySet()) {
                List<SlotRef> allRefColumns = Lists.newArrayList();
                entry.getKey().collect(SlotRef.class, allRefColumns);
                allRefColumns.add(entry.getValue());
                if (hasStar || outputSlotRef.containsAll(allRefColumns)) {
                    generatedExprToColumnRef.put(entry.getKey().clone(), (SlotRef) entry.getValue().clone());
                }
            }

            // 2. rewrite(rename slotRef) generated column expression(unAnalyzed) using alias in current scope
            Map<String, String> slotRefToAlias = new HashMap<>();
            for (SelectListItem item : childSelectRelation.getSelectList().getItems()) {
                if (item.isStar()) {
                    slotRefToAlias.clear();
                    break;
                }

                if (!(item.getExpr() instanceof SlotRef) || (item.getAlias() == null || item.getAlias().isEmpty())) {
                    continue;
                }

                slotRefToAlias.put(((SlotRef) item.getExpr()).toSql(), item.getAlias());
            }
            List<SlotRef> allRefSlotRefs = new ArrayList<>();
            for (Map.Entry<Expr, SlotRef> entry : generatedExprToColumnRef.entrySet()) {
                List<SlotRef> refColumns = Lists.newArrayList();
                entry.getKey().collect(SlotRef.class, refColumns);

                allRefSlotRefs.addAll(refColumns);
                allRefSlotRefs.add(entry.getValue());
            }
            for (SlotRef slotRef : allRefSlotRefs) {
                if (!slotRefToAlias.isEmpty()) {
                    String alias = slotRefToAlias.get(slotRef.toSql());
                    if (alias != null) {
                        slotRef.setColumnName(alias);
                    }
                }
                slotRef.setTblName(null);
            }

            // 3. analyze generated column expression based on current scope
            Map<Expr, SlotRef> analyzedGeneratedExprToColumnRef = new HashMap<>();
            for (Map.Entry<Expr, SlotRef> entry : generatedExprToColumnRef.entrySet()) {
                entry.getKey().reset();
                entry.getValue().reset();

                try {
                    ExpressionAnalyzer.analyzeExpression(entry.getKey(), new AnalyzeState(), scope, session);
                    ExpressionAnalyzer.analyzeExpression(entry.getValue(), new AnalyzeState(), scope, session);
                } catch (Exception ignore) {
                    // skip this generated column rewrite if hit any exception
                    // some exception is reasonable because some of illegal generated column
                    // rewrite will be rejected by ananlyzer exception.
                    continue;
                }
                analyzedGeneratedExprToColumnRef.put(entry.getKey(), entry.getValue());
            }
            resultGeneratedExprToColumnRef.putAll(analyzedGeneratedExprToColumnRef);
        }

        @Override
        public Void visitTable(TableRelation tableRelation, Scope scope) {
            Table table = tableRelation.getTable();
            Map<Expr, SlotRef> generatedExprToColumnRef = new HashMap<>();
            for (Column column : table.getBaseSchema()) {
                Expr generatedColumnExpression = column.getGeneratedColumnExpr(table.getIdToColumn());
                if (generatedColumnExpression != null) {
                    SlotRef slotRef = new SlotRef(null, column.getName());
                    ExpressionAnalyzer.analyzeExpression(generatedColumnExpression, new AnalyzeState(), scope, session);
                    ExpressionAnalyzer.analyzeExpression(slotRef, new AnalyzeState(), scope, session);
                    generatedExprToColumnRef.put(generatedColumnExpression, slotRef);
                }
            }
            tableRelation.setGeneratedExprToColumnRef(generatedExprToColumnRef);
            return null;
        }

        @Override
        public Void visitSelect(SelectRelation selectRelation, Scope scope) {
            selectRelation.setGeneratedExprToColumnRef(selectRelation.getRelation().getGeneratedExprToColumnRef());
            return null;
        }

        @Override
        public Void visitSubqueryRelation(SubqueryRelation subquery, Scope scope) {
            QueryRelation queryRelation = subquery.getQueryStatement().getQueryRelation();
            if (queryRelation instanceof SelectRelation) {
                SelectRelation childSelectRelation = (SelectRelation) queryRelation;
                reAnalyzeExpressionBasedOnCurrentScope(childSelectRelation, scope, subquery.getGeneratedExprToColumnRef());
            }
            return null;
        } 

        @Override
        public Void visitJoin(JoinRelation joinRelation, Scope scope) {
            Relation leftRelation = joinRelation.getLeft();
            Relation rightRelation = joinRelation.getRight();
            joinRelation.getGeneratedExprToColumnRef().putAll(leftRelation.getGeneratedExprToColumnRef());
            joinRelation.getGeneratedExprToColumnRef().putAll(rightRelation.getGeneratedExprToColumnRef());
            return null;
        }

        @Override
        public Void visitView(ViewRelation node, Scope scope) {
            QueryRelation queryRelation = node.getQueryStatement().getQueryRelation();
            if (queryRelation instanceof SubqueryRelation) {
                node.setGeneratedExprToColumnRef(queryRelation.getGeneratedExprToColumnRef());
            } else if (queryRelation instanceof SelectRelation) {
                SelectRelation childSelectRelation = (SelectRelation) queryRelation;
                reAnalyzeExpressionBasedOnCurrentScope(childSelectRelation, scope, node.getGeneratedExprToColumnRef());
            }
            return null;
        }

        @Override
        public Void visitCTE(CTERelation cteRelation, Scope scope) {
            QueryRelation queryRelation = cteRelation.getCteQueryStatement().getQueryRelation();
            if (queryRelation instanceof SubqueryRelation) {
                cteRelation.setGeneratedExprToColumnRef(queryRelation.getGeneratedExprToColumnRef());
            } else if (queryRelation instanceof SelectRelation) {
                SelectRelation childSelectRelation = (SelectRelation) queryRelation;
                reAnalyzeExpressionBasedOnCurrentScope(childSelectRelation, scope, cteRelation.getGeneratedExprToColumnRef());
            }
            return null;
        }
    }

    private class Visitor implements AstVisitor<Scope, Scope> {
        public Visitor() {
        }

        public Scope process(ParseNode node, Scope scope) {
            return node.accept(this, scope);
        }

        @Override
        public Scope visitQueryStatement(QueryStatement node, Scope parent) {
            Scope scope = visitQueryRelation(node.getQueryRelation(), parent);
            if (node.hasOutFileClause()) {
                node.getOutFileClause().analyze(scope);
            }
            return scope;
        }

        @Override
        public Scope visitQueryRelation(QueryRelation node, Scope parent) {
            Scope scope = analyzeCTE(node, parent);
            return process(node, scope);
        }

        private Scope analyzeCTE(QueryRelation stmt, Scope scope) {
            Scope cteScope = new Scope(RelationId.anonymous(), new RelationFields());
            cteScope.setParent(scope);

            if (!stmt.hasWithClause()) {
                return cteScope;
            }

            for (CTERelation withQuery : stmt.getCteRelations()) {
                QueryRelation query = withQuery.getCteQueryStatement().getQueryRelation();
                process(withQuery.getCteQueryStatement(), cteScope);
                String cteName = withQuery.getName();
                if (cteScope.containsCTE(cteName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NONUNIQ_TABLE, cteName);
                }

                if (withQuery.getColumnOutputNames() == null) {
                    withQuery.setColumnOutputNames(new ArrayList<>(query.getColumnOutputNames()));
                } else {
                    if (withQuery.getColumnOutputNames().size() != query.getColumnOutputNames().size()) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_VIEW_WRONG_LIST);
                    }
                }

                /*
                 * use cte column name as output scope of subquery relation fields
                 */
                ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
                for (int fieldIdx = 0; fieldIdx < query.getRelationFields().getAllFields().size(); ++fieldIdx) {
                    Field originField = query.getRelationFields().getFieldByIndex(fieldIdx);

                    String database = originField.getRelationAlias() == null ? session.getDatabase() :
                            originField.getRelationAlias().getDb();
                    TableName tableName = new TableName(database, cteName);
                    outputFields.add(
                            new Field(withQuery.getColumnOutputNames().get(fieldIdx), originField.getType(), tableName,
                                    originField.getOriginExpression()));
                }

                /*
                 *  Because the analysis of CTE is sensitive to order
                 *  the later CTE can call the previous resolved CTE,
                 *  and the previous CTE can rewrite the existing table name.
                 *  So here will save an increasing AnalyzeState to add cte scope
                 */
                withQuery.setScope(new Scope(RelationId.of(withQuery), new RelationFields(outputFields.build())));
                cteScope.addCteQueries(cteName, withQuery);
            }

            return cteScope;
        }

        @Override
        public Scope visitSelect(SelectRelation selectRelation, Scope scope) {
            AnalyzeState analyzeState = new AnalyzeState();
            //Record aliases at this level to prevent alias conflicts
            Set<TableName> aliasSet = new HashSet<>();
            Relation resolvedRelation = resolveTableRef(selectRelation.getRelation(), scope, aliasSet);
            if (resolvedRelation instanceof TableFunctionRelation) {
                throw unsupportedException("Table function must be used with lateral join");
            }
            selectRelation.setRelation(resolvedRelation);
            //for avoid init column meta, try to prune unused columns
            pruneScanColumns(selectRelation, resolvedRelation);
            Scope sourceScope = process(resolvedRelation, scope);
            sourceScope.setParent(scope);

            selectRelation.accept(new RewriteAliasVisitor(sourceScope, session), null);
            SelectAnalyzer selectAnalyzer = new SelectAnalyzer(session);
            selectAnalyzer.analyze(
                    analyzeState,
                    selectRelation.getSelectList(),
                    selectRelation.getRelation(),
                    sourceScope,
                    selectRelation.getGroupByClause(),
                    selectRelation.getHavingClause(),
                    selectRelation.getWhereClause(),
                    selectRelation.getOrderBy(),
                    selectRelation.getLimit());

            selectRelation.fillResolvedAST(analyzeState);
            GeneratedColumnExprMappingCollector collector = new GeneratedColumnExprMappingCollector();
            collector.process(selectRelation, sourceScope);
            return analyzeState.getOutputScope();
        }

        // for reduce meta initialization, only support simple query relation, like: select x1, x2 from t;
        private void pruneScanColumns(SelectRelation selectRelation, Relation fromRelation) {
            if (!session.getSessionVariable().isEnableAnalyzePhasePruneColumns() ||
                    !(fromRelation instanceof TableRelation) ||
                    !Objects.isNull(selectRelation.getGroupByClause()) ||
                    !Objects.isNull(selectRelation.getHavingClause()) ||
                    !Objects.isNull(selectRelation.getWhereClause()) ||
                    (!Objects.isNull(selectRelation.getOrderBy()) && !selectRelation.getOrderBy().isEmpty())) {
                return;
            }

            List<String> scanColumns = new ArrayList<>();
            for (SelectListItem item : selectRelation.getSelectList().getItems()) {
                if (item.isStar() || !(item.getExpr() instanceof SlotRef)) {
                    return;
                }

                if (((SlotRef) item.getExpr()).getTblNameWithoutAnalyzed() != null) {
                    // avoid struct column pruned
                    return;
                }
                scanColumns.add(((SlotRef) item.getExpr()).getColumnName().toLowerCase());
            }
            ((TableRelation) fromRelation).setPruneScanColumns(scanColumns);
        }

        private Relation resolveTableRef(Relation relation, Scope scope, Set<TableName> aliasSet) {
            if (relation instanceof JoinRelation) {
                JoinRelation join = (JoinRelation) relation;
                join.setLeft(resolveTableRef(join.getLeft(), scope, aliasSet));
                Relation rightRelation = resolveTableRef(join.getRight(), scope, aliasSet);
                join.setRight(rightRelation);
                if (rightRelation instanceof TableFunctionRelation) {
                    join.setLateral(true);
                }
                return join;
            } else if (relation instanceof PivotRelation) {
                PivotRelation pivotRelation = (PivotRelation) relation;
                pivotRelation.setQuery(resolveTableRef(pivotRelation.getQuery(), scope, aliasSet));
                return pivotRelation;
            } else if (relation instanceof FileTableFunctionRelation) {
                FileTableFunctionRelation tableFunctionRelation = (FileTableFunctionRelation) relation;
                Table table = resolveTableFunctionTable(
                        tableFunctionRelation.getProperties(), tableFunctionRelation.getPushDownSchemaFunc());
                TableFunctionTable tableFunctionTable = (TableFunctionTable) table;
                if (tableFunctionTable.isListFilesOnly()) {
                    return convertFileTableFunctionRelation(tableFunctionTable);
                } else {
                    tableFunctionRelation.setTable(table);
                    return relation;
                }
            } else if (relation instanceof TableRelation) {
                TableRelation tableRelation = (TableRelation) relation;
                TableName tableName = tableRelation.getName();
                if (tableName != null && Strings.isNullOrEmpty(tableName.getDb())) {
                    Optional<CTERelation> withQuery = scope.getCteQueries(tableName.getTbl());
                    if (withQuery.isPresent()) {
                        CTERelation withRelation = withQuery.get();
                        withRelation.addTableRef();
                        RelationFields withRelationFields = withQuery.get().getRelationFields();
                        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

                        for (int fieldIdx = 0; fieldIdx < withRelationFields.getAllFields().size(); ++fieldIdx) {
                            Field originField = withRelationFields.getAllFields().get(fieldIdx);
                            outputFields.add(new Field(
                                    originField.getName(), originField.getType(), tableRelation.getResolveTableName(),
                                    originField.getOriginExpression()));
                        }

                        // The CTERelation stored in the Scope is not used directly here, but a new Relation is copied.
                        // It is because we hope to obtain a new RelationId to distinguish multiple cte reuses.
                        // Because the reused cte should not be considered the same relation.
                        // eg: with w as (select * from t0) select v1,sum(v2) from w group by v1 " +
                        //                "having v1 in (select v3 from w where v2 = 2)
                        // cte used in outer query and sub-query can't use same relation-id and field
                        CTERelation newCteRelation = new CTERelation(withRelation.getCteMouldId(), tableName.getTbl(),
                                withRelation.getColumnOutputNames(),
                                withRelation.getCteQueryStatement());
                        newCteRelation.setAlias(tableRelation.getAlias());
                        newCteRelation.setResolvedInFromClause(true);
                        newCteRelation.setScope(
                                new Scope(RelationId.of(newCteRelation), new RelationFields(outputFields.build())));
                        return newCteRelation;
                    }
                }

                TableName resolveTableName = relation.getResolveTableName();
                resolveTableName.normalization(session);
                if (aliasSet.contains(resolveTableName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NONUNIQ_TABLE,
                            relation.getResolveTableName().getTbl());
                } else {
                    aliasSet.add(new TableName(resolveTableName.getCatalog(),
                            resolveTableName.getDb(),
                            resolveTableName.getTbl()));
                }

                Table table = resolveTable(tableRelation);
                Relation r;
                if (table instanceof View) {
                    View view = (View) table;
                    QueryStatement queryStatement = view.getQueryStatement();
                    ViewRelation viewRelation = new ViewRelation(tableName, view, queryStatement);

                    // If tableRelation is an object that needs to be rewritten by policy,
                    // then when it is changed to ViewRelation, both the view and the table
                    // after the view is parsed also need to inherit this rewriting logic.
                    if (tableRelation.isNeedRewrittenByPolicy()) {
                        viewRelation.setNeedRewrittenByPolicy(true);

                        new AstTraverser<Void, Void>() {
                            @Override
                            public Void visitRelation(Relation relation, Void context) {
                                relation.setNeedRewrittenByPolicy(true);
                                return null;
                            }
                        }.visit(queryStatement);
                    }
                    viewRelation.setAlias(tableRelation.getAlias());

                    r = viewRelation;
                } else if (table instanceof ConnectorView) {
                    ConnectorView connectorView = (ConnectorView) table;
                    QueryStatement queryStatement = connectorView.getQueryStatement();
                    View view = new View(connectorView.getId(), connectorView.getName(), connectorView.getFullSchema(),
                            connectorView.getType());
                    view.setInlineViewDefWithSqlMode(connectorView.getInlineViewDef(), 0);
                    ViewRelation viewRelation = new ViewRelation(tableName, view, queryStatement);
                    viewRelation.setAlias(tableRelation.getAlias());

                    r = viewRelation;
                } else {
                    if (tableRelation.getQueryPeriodString() != null && !table.isTemporal()) {
                        throw unsupportedException("Unsupported table type for temporal clauses, table type: " +
                                table.getType());
                    }

                    if (table.isSupported()) {
                        tableRelation.setTable(table);
                        r = tableRelation;
                    } else {
                        throw unsupportedException("Unsupported scan table type: " + table.getType());
                    }
                }

                if (!r.isNeedRewrittenByPolicy()) {
                    return r;
                }
                assert tableName != null;
                QueryStatement policyRewriteQuery = SecurityPolicyRewriteRule.buildView(session, r, tableName);
                if (policyRewriteQuery == null) {
                    return r;
                } else {
                    r.setNeedRewrittenByPolicy(false);
                    SubqueryRelation subqueryRelation = new SubqueryRelation(policyRewriteQuery);

                    // If an alias exists, rewrite the alias into subquery to ensure
                    // that the original expression can be resolved correctly.
                    // `select v1 from tbl t` is rewritten as `select t.v1 from (select tbl.v1 from tbl) t`
                    subqueryRelation.setAlias(resolveTableName);
                    return subqueryRelation;
                }
            } else {
                if (relation.getResolveTableName() != null) {
                    if (aliasSet.contains(relation.getResolveTableName())) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_NONUNIQ_TABLE,
                                relation.getResolveTableName().getTbl());
                    } else {
                        aliasSet.add(relation.getResolveTableName());
                    }
                }
                return relation;
            }
        }

        // convert FileTableFunctionRelation to ValuesRelation if only list files
        private ValuesRelation convertFileTableFunctionRelation(TableFunctionTable table) {
            List<Column> columns = table.getFullSchema();
            List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            List<Type> outputColumnTypes = columns.stream().map(Column::getType).collect(Collectors.toList());
            List<List<Expr>> rows = Lists.newArrayList();
            for (List<String> fileInfo : table.listFilesAndDirs()) {
                Preconditions.checkState(columns.size() == fileInfo.size());
                List<Expr> row = Lists.newArrayList();
                for (int i = 0; i < columns.size(); ++i) {
                    try {
                        row.add(LiteralExpr.create(fileInfo.get(i), columns.get(i).getType()));
                    } catch (AnalysisException e) {
                        throw new SemanticException(e.getMessage());
                    }
                }
                rows.add(row);
            }
            return new ValuesRelation(rows, columnNames, outputColumnTypes);
        }

        @Override
        public Scope visitTable(TableRelation node, Scope outerScope) {
            TableName tableName = node.getResolveTableName();
            Table table = node.getTable();

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            ImmutableMap.Builder<Field, Column> columns = ImmutableMap.builder();

            if (node.isSyncMVQuery()) {
                OlapTable olapTable = (OlapTable) table;
                List<Column> mvSchema = olapTable.getSchemaByIndexId(olapTable.getBaseIndexId());
                for (Column column : mvSchema) {
                    Field field = new Field(column.getName(), column.getType(), tableName,
                            new SlotRef(tableName, column.getName(), column.getName()), true, column.isAllowNull());
                    columns.put(field, column);
                    fields.add(field);
                }
            } else {
                List<Column> fullSchema = table.getFullSchema();
                Set<Column> baseSchema = new HashSet<>(table.getBaseSchema());

                List<String> pruneScanColumns = node.getPruneScanColumns();
                boolean needPruneScanColumns = pruneScanColumns != null && !pruneScanColumns.isEmpty();
                if (needPruneScanColumns) {
                    Set<String> fullColumnNames = new HashSet<>(fullSchema.size());
                    for (Column column : fullSchema) {
                        if (column.isGeneratedColumn()) {
                            needPruneScanColumns = false;
                            break;
                        }
                        fullColumnNames.add(column.getName().toLowerCase());
                    }
                    needPruneScanColumns &= fullColumnNames.containsAll(pruneScanColumns);
                }

                Set<String> bucketColumns = table.getDistributionColumnNames();
                List<String> partitionColumns = table.getPartitionColumnNames();
                for (Column column : fullSchema) {
                    // TODO: avoid analyze visible or not each time, cache it in schema
                    if (needPruneScanColumns && !column.isKey() &&
                            bucketColumns.stream().noneMatch(column.getName()::equalsIgnoreCase) &&
                            partitionColumns.stream().noneMatch(column.getName()::equalsIgnoreCase) &&
                            pruneScanColumns.stream().noneMatch(column.getName()::equalsIgnoreCase)) {
                        // reduce unnecessary columns init, but must init key columns/bucket columns/partition columns
                        continue;
                    }
                    boolean visible = baseSchema.contains(column);
                    SlotRef slot = new SlotRef(tableName, column.getName(), column.getName());
                    Field field = new Field(column.getName(), column.getType(), tableName, slot, visible,
                            column.isAllowNull());
                    columns.put(field, column);
                    fields.add(field);
                }

                if (node.isBinlogQuery()) {
                    for (Column column : getBinlogMetaColumns()) {
                        SlotRef slot = new SlotRef(tableName, column.getName(), column.getName());
                        Field field = new Field(column.getName(), column.getType(), tableName, slot, true,
                                column.isAllowNull());
                        columns.put(field, column);
                        fields.add(field);
                    }
                }
            }

            node.setColumns(columns.build());
            String dbName = node.getName().getDb();
            if (session.getDumpInfo() != null) {
                session.getDumpInfo().addTable(dbName, table);

                if (table.isHiveTable()) {
                    HiveTable hiveTable = (HiveTable) table;
                    session.getDumpInfo().addHMSTable(hiveTable.getResourceName(), hiveTable.getCatalogDBName(),
                            hiveTable.getCatalogTableName());
                    HiveMetaStoreTableDumpInfo hiveMetaStoreTableDumpInfo = session.getDumpInfo().getHMSTable(
                            hiveTable.getResourceName(), hiveTable.getCatalogDBName(), hiveTable.getCatalogTableName());
                    hiveMetaStoreTableDumpInfo.setPartColumnNames(hiveTable.getPartitionColumnNames());
                    hiveMetaStoreTableDumpInfo.setDataColumnNames(hiveTable.getDataColumnNames());
                    Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().
                            getResource(hiveTable.getResourceName());
                    if (resource != null) {
                        session.getDumpInfo().addResource(resource);
                    }
                }
            }

            Scope scope = new Scope(RelationId.of(node), new RelationFields(fields.build()));
            node.setScope(scope);

            GeneratedColumnExprMappingCollector collector = new GeneratedColumnExprMappingCollector();
            collector.process(node, scope);

            return scope;
        }

        private List<Column> getBinlogMetaColumns() {
            List<Column> columns = new ArrayList<>();
            columns.add(new Column(BINLOG_OP_COLUMN_NAME, Type.TINYINT));
            columns.add(new Column(BINLOG_VERSION_COLUMN_NAME, Type.BIGINT));
            columns.add(new Column(BINLOG_SEQ_ID_COLUMN_NAME, Type.BIGINT));
            columns.add(new Column(BINLOG_TIMESTAMP_COLUMN_NAME, Type.BIGINT));
            return columns;
        }

        @Override
        public Scope visitFileTableFunction(FileTableFunctionRelation node, Scope outerScope) {
            TableName tableName = node.getResolveTableName();
            Table table = node.getTable();

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            ImmutableMap.Builder<Field, Column> columns = ImmutableMap.builder();

            List<Column> fullSchema = table.getFullSchema();
            for (Column column : fullSchema) {
                Field field = new Field(column.getName(), column.getType(), tableName,
                        new SlotRef(tableName, column.getName(), column.getName()), true);
                columns.put(field, column);
                fields.add(field);
            }

            node.setColumns(columns.build());
            Scope scope = new Scope(RelationId.of(node), new RelationFields(fields.build()));
            node.setScope(scope);
            return scope;
        }

        @Override
        public Scope visitCTE(CTERelation cteRelation, Scope context) {
            QueryRelation query = cteRelation.getCteQueryStatement().getQueryRelation();

            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (int fieldIdx = 0; fieldIdx < query.getRelationFields().getAllFields().size(); ++fieldIdx) {
                Field originField = query.getRelationFields().getFieldByIndex(fieldIdx);
                outputFields.add(new Field(cteRelation.getColumnOutputNames() == null ?
                        originField.getName() : cteRelation.getColumnOutputNames().get(fieldIdx),
                        originField.getType(),
                        cteRelation.getResolveTableName(),
                        originField.getOriginExpression()));
            }
            Scope scope = new Scope(RelationId.of(cteRelation), new RelationFields(outputFields.build()));
            cteRelation.setScope(scope);

            GeneratedColumnExprMappingCollector collector = new GeneratedColumnExprMappingCollector();
            collector.process(cteRelation, scope);

            return scope;
        }

        @Override
        public Scope visitJoin(JoinRelation join, Scope parentScope) {
            Scope leftScope = process(join.getLeft(), parentScope);
            Scope rightScope;
            if (join.getRight() instanceof TableFunctionRelation || join.isLateral()) {
                if (!(join.getRight() instanceof TableFunctionRelation)) {
                    throw new SemanticException("Only support lateral join with UDTF");
                } else if (!join.getJoinOp().isInnerJoin() && !join.getJoinOp().isCrossJoin() &&
                        !((TableFunctionRelation) join.getRight()).getFunctionName().getFunction().
                                equalsIgnoreCase("unnest")) {
                    // not inner join && not cross join && not unnest
                    throw new SemanticException("Not support lateral join except inner or cross");
                } else if (!join.getJoinOp().isInnerJoin() && !join.getJoinOp().isCrossJoin()) {
                    // must be unnest and not inner join and not corss join
                    if (join.getJoinOp().isLeftOuterJoin()) {
                        if (join.getOnPredicate() instanceof BoolLiteral &&
                                ((BoolLiteral) join.getOnPredicate()).getValue()) {
                            // left join on true
                            ((TableFunctionRelation) join.getRight()).setIsLeftJoin(true);
                        } else {
                            throw new SemanticException("left join unnest only support on true");
                        }
                    } else {
                        throw new SemanticException("unnest support inner join, cross join and left join on true");
                    }
                }
                rightScope = process(join.getRight(), leftScope);
            } else {
                rightScope = process(join.getRight(), parentScope);
            }

            Expr joinEqual = join.getOnPredicate();
            if (join.getUsingColNames() != null) {
                Expr resolvedUsing = analyzeJoinUsing(join.getUsingColNames(), leftScope, rightScope);
                if (joinEqual == null) {
                    joinEqual = resolvedUsing;
                } else {
                    joinEqual = new CompoundPredicate(CompoundPredicate.Operator.AND, joinEqual, resolvedUsing);
                }
                join.setOnPredicate(joinEqual);
            }

            if (!join.getJoinHint().isEmpty()) {
                analyzeJoinHints(join);
            }

            if (joinEqual != null) {
                /*
                 * sourceRelation.getRelationFields() is used to represent the column information of output.
                 * To ensure the OnPredicate in semi/anti is correct, the relation needs to be re-assembled here
                 * with left child and right child relationFields
                 */
                analyzeExpression(joinEqual, new AnalyzeState(), new Scope(RelationId.of(join),
                        leftScope.getRelationFields().joinWith(rightScope.getRelationFields())));

                AnalyzerUtils.verifyNoAggregateFunctions(joinEqual, "JOIN");
                AnalyzerUtils.verifyNoWindowFunctions(joinEqual, "JOIN");
                AnalyzerUtils.verifyNoGroupingFunctions(joinEqual, "JOIN");

                if (!joinEqual.getType().matchesType(Type.BOOLEAN) && !joinEqual.getType().matchesType(Type.NULL)) {
                    throw new SemanticException("WHERE clause must evaluate to a boolean: actual type %s",
                            joinEqual.getType());
                }
                // check the join on predicate, example:
                // we have col_json, we can't join on table_a.col_json = table_b.col_json,
                // but we can join on cast(table_a.col_json->"a" as int) = cast(table_b.col_json->"a" as int)
                // similarly, we can join on table_a.col_map['a'] = table_b.col_map['a'],
                // and table_a.col_struct.a = table_b.col_struct.a
                checkJoinEqual(joinEqual);
            } else {
                if (join.getJoinOp().isOuterJoin() || join.getJoinOp().isSemiAntiJoin()) {
                    throw new SemanticException(join.getJoinOp() + " requires an ON or USING clause.");
                }
            }

            /*
             * New Scope needs to be constructed for select in semi/anti join
             */
            Scope scope;
            if (join.getJoinOp().isLeftSemiAntiJoin()) {
                scope = new Scope(RelationId.of(join), leftScope.getRelationFields());
            } else if (join.getJoinOp().isRightSemiAntiJoin()) {
                scope = new Scope(RelationId.of(join), rightScope.getRelationFields());
            } else if (join.getJoinOp().isLeftOuterJoin()) {
                List<Field> rightFields = getFieldsWithNullable(rightScope);
                scope = new Scope(RelationId.of(join),
                        leftScope.getRelationFields().joinWith(new RelationFields(rightFields)));
            } else if (join.getJoinOp().isRightOuterJoin()) {
                List<Field> leftFields = getFieldsWithNullable(leftScope);
                scope = new Scope(RelationId.of(join),
                        new RelationFields(leftFields).joinWith(rightScope.getRelationFields()));
            } else if (join.getJoinOp().isFullOuterJoin()) {
                List<Field> rightFields = getFieldsWithNullable(rightScope);
                List<Field> leftFields = getFieldsWithNullable(leftScope);
                scope = new Scope(RelationId.of(join),
                        new RelationFields(leftFields).joinWith(new RelationFields(rightFields)));
            } else {
                scope = new Scope(RelationId.of(join),
                        leftScope.getRelationFields().joinWith(rightScope.getRelationFields()));
            }
            join.setScope(scope);

            GeneratedColumnExprMappingCollector collector = new GeneratedColumnExprMappingCollector();
            collector.process(join, scope);

            return scope;
        }

        private List<Field> getFieldsWithNullable(Scope scope) {
            List<Field> newFields = new ArrayList<>();
            for (Field field : scope.getRelationFields().getAllFields()) {
                Field newField = new Field(field);
                newField.setNullable(true);
                newFields.add(newField);
            }
            return newFields;
        }

        private Expr analyzeJoinUsing(List<String> usingColNames, Scope left, Scope right) {
            Expr joinEqual = null;
            for (String colName : usingColNames) {
                TableName leftTableName =
                        left.resolveField(new SlotRef(null, colName)).getField().getRelationAlias();
                TableName rightTableName =
                        right.resolveField(new SlotRef(null, colName)).getField().getRelationAlias();

                // create predicate "<left>.colName = <right>.colName"
                BinaryPredicate resolvedUsing = new BinaryPredicate(BinaryType.EQ,
                        new SlotRef(leftTableName, colName), new SlotRef(rightTableName, colName));

                if (joinEqual == null) {
                    joinEqual = resolvedUsing;
                } else {
                    joinEqual = new CompoundPredicate(CompoundPredicate.Operator.AND, joinEqual, resolvedUsing);
                }
            }
            return joinEqual;
        }

        private void analyzeJoinHints(JoinRelation join) {
            if (JoinOperator.HINT_BROADCAST.equals(join.getJoinHint())) {
                if (join.getJoinOp() == JoinOperator.RIGHT_OUTER_JOIN
                        || join.getJoinOp() == JoinOperator.FULL_OUTER_JOIN
                        || join.getJoinOp() == JoinOperator.RIGHT_SEMI_JOIN
                        || join.getJoinOp() == JoinOperator.RIGHT_ANTI_JOIN) {
                    throw new SemanticException(join.getJoinOp().toString() + " does not support BROADCAST.");
                }
            } else if (JoinOperator.HINT_SHUFFLE.equals(join.getJoinHint())) {
                if (join.getJoinOp() == JoinOperator.CROSS_JOIN ||
                        (join.getJoinOp() == JoinOperator.INNER_JOIN && join.getOnPredicate() == null)) {
                    throw new SemanticException("CROSS JOIN does not support SHUFFLE.");
                }
            } else if (JoinOperator.HINT_BUCKET.equals(join.getJoinHint()) ||
                    JoinOperator.HINT_COLOCATE.equals(join.getJoinHint())) {
                if (join.getJoinOp() == JoinOperator.CROSS_JOIN) {
                    throw new SemanticException("CROSS JOIN does not support " + join.getJoinHint() + ".");
                }
            } else if (JoinOperator.HINT_SKEW.equals(join.getJoinHint())) {
                if (join.getJoinOp() == JoinOperator.CROSS_JOIN ||
                        (join.getJoinOp() == JoinOperator.INNER_JOIN && join.getOnPredicate() == null)) {
                    throw new SemanticException("CROSS JOIN does not support SKEW JOIN optimize");
                }
                if (join.getJoinOp().isRightJoin()) {
                    throw new SemanticException("RIGHT JOIN does not support SKEW JOIN optimize");
                }
                if (join.getSkewColumn() != null) {
                    if (!(join.getSkewColumn() instanceof SlotRef)) {
                        throw new SemanticException("Skew join column must be a column reference");
                    }
                    analyzeExpression(join.getSkewColumn(), new AnalyzeState(), join.getLeft().getScope());
                } else {
                    throw new SemanticException("Skew join column must be specified");
                }
                if (join.getSkewValues() != null) {
                    if (join.getSkewValues().stream().anyMatch(expr -> !expr.isConstant())) {
                        throw new SemanticException("skew join values must be constant");
                    }
                    List<Expr> newSkewValues = new ArrayList<>();
                    for (Expr expr : join.getSkewValues()) {
                        newSkewValues.add(TypeManager.addCastExpr(expr, join.getSkewColumn().getType()));
                    }
                    join.setSkewValues(newSkewValues);
                } else {
                    throw new SemanticException("Skew join values must be specified");
                }
            } else if (!JoinOperator.HINT_UNREORDER.equals(join.getJoinHint())) {
                throw new SemanticException("JOIN hint not recognized: " + join.getJoinHint());
            }
        }

        @Override
        public Scope visitSubqueryRelation(SubqueryRelation subquery, Scope context) {
            if (subquery.getResolveTableName() != null && subquery.getResolveTableName().getTbl() == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DERIVED_MUST_HAVE_ALIAS);
            }

            Scope queryOutputScope = process(subquery.getQueryStatement(), context);

            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

            if (subquery.getExplicitColumnNames() != null) {
                if (queryOutputScope.getRelationFields().getAllVisibleFields().size()
                        != subquery.getExplicitColumnNames().size()) {
                    throw new SemanticException("In definition of view, derived table or common table expression, " +
                            "SELECT list and column names list have different column counts");
                }
            }

            int explicitColumnNameIdx = 0;
            for (Field field : queryOutputScope.getRelationFields().getAllFields()) {
                String fieldResolveName;
                if (subquery.getExplicitColumnNames() != null && field.isVisible()) {
                    fieldResolveName = subquery.getExplicitColumnNames().get(explicitColumnNameIdx);
                    explicitColumnNameIdx++;
                } else {
                    fieldResolveName = field.getName();
                }

                outputFields.add(new Field(fieldResolveName, field.getType(), subquery.getResolveTableName(),
                        field.getOriginExpression()));

            }
            Scope scope = new Scope(RelationId.of(subquery), new RelationFields(outputFields.build()));

            analyzeOrderByClause(subquery, scope);
            subquery.setScope(scope);

            GeneratedColumnExprMappingCollector collector = new GeneratedColumnExprMappingCollector();
            collector.process(subquery, scope);

            return scope;
        }

        private void analyzeOrderByClause(QueryRelation query, Scope scope) {
            if (!query.hasOrderByClause()) {
                return;
            }
            List<Expr> outputExpressions = query.getOutputExpression();
            for (OrderByElement orderByElement : query.getOrderBy()) {
                Expr expression = orderByElement.getExpr();
                AnalyzerUtils.verifyNoGroupingFunctions(expression, "ORDER BY");

                if (expression instanceof IntLiteral) {
                    long ordinal = ((IntLiteral) expression).getLongValue();
                    if (ordinal < 1 || ordinal > outputExpressions.size()) {
                        throw new SemanticException("ORDER BY position %s is not in select list", ordinal);
                    }
                    expression = new FieldReference((int) ordinal - 1, null);
                }

                analyzeExpression(expression, new AnalyzeState(), scope);

                if (!expression.getType().canOrderBy()) {
                    throw new SemanticException(Type.NOT_SUPPORT_ORDER_ERROR_MSG);
                }

                orderByElement.setExpr(expression);
            }
        }

        @Override
        public Scope visitView(ViewRelation node, Scope scope) {
            boolean isRelationAliasCaseInSensitive = false;
            if (ConnectContext.get() != null) {
                isRelationAliasCaseInSensitive = ConnectContext.get().isRelationAliasCaseInsensitive();
                // For hive view, relation alias is case-insensitive
                if (node.getView().isHiveView()) {
                    ConnectContext.get().setRelationAliasCaseInSensitive(true);
                }
            }
            Scope queryOutputScope;
            try {
                queryOutputScope = process(node.getQueryStatement(), scope);
            } catch (SemanticException e) {
                throw new SemanticException("View " + node.getName() + " references invalid table(s) or column(s) or " +
                        "function(s) or definer/invoker of view lack rights to use them: " + e.getMessage(), e);
            } finally {
                if (ConnectContext.get() != null && node.getView().isHiveView()) {
                    ConnectContext.get().setRelationAliasCaseInSensitive(isRelationAliasCaseInSensitive);
                }
            }

            View view = node.getView();
            List<Field> fields = Lists.newArrayList();
            for (int i = 0; i < view.getBaseSchema().size(); ++i) {
                Column column = view.getBaseSchema().get(i);
                Field originField = queryOutputScope.getRelationFields().getFieldByIndex(i);
                // A view can specify its column names optionally, if column names are absent,
                // the output names of the queryRelation is used as the names of the view schema,
                // so column names in view's schema are always correct. Using originField.getName
                // here will gives wrong names when user-specified view column names are different
                // from output names of the queryRelation.
                //
                // view created in previous use originField.getOriginExpression().type as column
                // types in its schema, it is incorrect, so use originField.type instead.
                Field field = new Field(column.getName(), originField.getType(), node.getResolveTableName(),
                        originField.getOriginExpression());
                fields.add(field);
            }

            if (session.getDumpInfo() != null) {
                String dbName = node.getName().getDb();
                session.getDumpInfo().addView(dbName, view);
            }

            Scope viewScope = new Scope(RelationId.of(node), new RelationFields(fields));
            node.setScope(viewScope);

            GeneratedColumnExprMappingCollector collector = new GeneratedColumnExprMappingCollector();
            collector.process(node, viewScope);

            return viewScope;
        }

        @Override
        public Scope visitUnion(UnionRelation node, Scope context) {
            return analyzeSetOperation(node, context);
        }

        @Override
        public Scope visitExcept(ExceptRelation node, Scope context) {
            if (node.getQualifier().equals(SetQualifier.ALL)) {
                throw new SemanticException("EXCEPT does not support ALL qualifier");
            }
            return analyzeSetOperation(node, context);
        }

        @Override
        public Scope visitIntersect(IntersectRelation node, Scope context) {
            if (node.getQualifier().equals(SetQualifier.ALL)) {
                throw new SemanticException("INTERSECT does not support ALL qualifier");
            }
            return analyzeSetOperation(node, context);
        }

        private Scope analyzeSetOperation(SetOperationRelation node, Scope context) {
            List<QueryRelation> setOpRelations = node.getRelations();

            Scope leftChildScope = process(setOpRelations.get(0), context);
            Type[] outputTypes = leftChildScope.getRelationFields().getAllFields()
                    .stream().map(Field::getType).toArray(Type[]::new);
            List<Boolean> nullables = leftChildScope.getRelationFields().getAllFields()
                    .stream().map(field -> field.isNullable()).collect(Collectors.toList());
            int outputSize = leftChildScope.getRelationFields().size();

            for (int i = 1; i < setOpRelations.size(); ++i) {
                Scope relation = process(setOpRelations.get(i), context);
                if (relation.getRelationFields().size() != outputSize) {
                    throw new SemanticException("Operands have unequal number of columns");
                }
                for (int fieldIdx = 0; fieldIdx < relation.getRelationFields().size(); ++fieldIdx) {
                    Field field = relation.getRelationFields().getAllFields().get(fieldIdx);
                    Type fieldType = field.getType();
                    if (fieldType.isOnlyMetricType() &&
                            !((node instanceof UnionRelation) &&
                                    (node.getQualifier().equals(SetQualifier.ALL)))) {
                        throw new SemanticException("%s not support set operation", fieldType);
                    }

                    Type commonType = TypeManager.getCommonSuperType(outputTypes[fieldIdx],
                            relation.getRelationFields().getFieldByIndex(fieldIdx).getType());
                    if (!commonType.isValid()) {
                        throw new SemanticException(String.format("Incompatible return types '%s' and '%s'",
                                outputTypes[fieldIdx],
                                relation.getRelationFields().getFieldByIndex(fieldIdx).getType()));
                    }
                    outputTypes[fieldIdx] = commonType;
                    nullables.set(fieldIdx, nullables.get(fieldIdx) | field.isNullable());
                }
            }

            ArrayList<Field> fields = new ArrayList<>();
            for (int fieldIdx = 0; fieldIdx < outputSize; ++fieldIdx) {
                Field oldField = leftChildScope.getRelationFields().getFieldByIndex(fieldIdx);
                fields.add(new Field(oldField.getName(), outputTypes[fieldIdx], oldField.getRelationAlias(),
                        oldField.getOriginExpression(), true, nullables.get(fieldIdx)));
            }

            Scope setOpOutputScope = new Scope(RelationId.of(node), new RelationFields(fields));

            analyzeOrderByClause(node, setOpOutputScope);
            node.setScope(setOpOutputScope);
            return setOpOutputScope;
        }

        @Override
        public Scope visitValues(ValuesRelation node, Scope scope) {
            Type[] outputTypes;
            List<List<Expr>> rows = node.getRows();
            List<Type> outputColumnTypes = node.getOutputColumnTypes();
            if (!outputColumnTypes.isEmpty()) {
                outputTypes = outputColumnTypes.toArray(new Type[0]);
            } else {
                Preconditions.checkState(!rows.isEmpty());
                AnalyzeState analyzeState = new AnalyzeState();

                List<Expr> firstRow = node.getRow(0);
                firstRow.forEach(e -> analyzeExpression(e, analyzeState, scope));
                outputTypes = firstRow.stream().map(Expr::getType).toArray(Type[]::new);
                for (List<Expr> row : rows) {
                    if (row.size() != firstRow.size()) {
                        throw new SemanticException("Values have unequal number of columns");
                    }
                    for (int fieldIdx = 0; fieldIdx < row.size(); ++fieldIdx) {
                        analyzeExpression(row.get(fieldIdx), analyzeState, scope);
                        Type commonType =
                                TypeManager.getCommonSuperType(outputTypes[fieldIdx], row.get(fieldIdx).getType());
                        if (!commonType.isValid()) {
                            throw new SemanticException(String.format("Incompatible return types '%s' and '%s'",
                                    outputTypes[fieldIdx], row.get(fieldIdx).getType()));
                        }
                        outputTypes[fieldIdx] = commonType;
                    }
                }
            }

            List<Field> fields = new ArrayList<>();
            for (int fieldIdx = 0; fieldIdx < outputTypes.length; ++fieldIdx) {
                fields.add(new Field(node.getColumnOutputNames().get(fieldIdx), outputTypes[fieldIdx],
                        node.getResolveTableName(), rows.isEmpty() ? null : rows.get(0).get(fieldIdx)));
            }

            Scope valuesScope = new Scope(RelationId.of(node), new RelationFields(fields));
            node.setScope(valuesScope);
            return valuesScope;
        }

        @Override
        public Scope visitPivotRelation(PivotRelation node, Scope context) {
            AnalyzeState analyzeState = new AnalyzeState();
            Scope queryScope = process(node.getQuery(), context);

            List<Pair<String, Expr>> aggAliasExprs = new ArrayList<>();
            for (PivotAggregation pivotAggregation : node.getAggregateFunctions()) {
                analyzeExpression(pivotAggregation.getFunctionCallExpr(), analyzeState, queryScope);
                if (!pivotAggregation.getFunctionCallExpr().isAggregateFunction()) {
                    throw new SemanticException("Measure expression in PIVOT must use aggregate function");
                }
                String alias = pivotAggregation.getAlias() == null
                        ? getAliasName(pivotAggregation.getFunctionCallExpr(), false, false)
                        : pivotAggregation.getAlias();
                aggAliasExprs.add(new Pair<>(alias, pivotAggregation.getFunctionCallExpr()));
            }

            for (SlotRef column : node.getPivotColumns()) {
                analyzeExpression(column, analyzeState, queryScope);
            }
            List<Type> types = node.getPivotColumns().stream()
                    .map(SlotRef::getType)
                    .collect(Collectors.toList());

            List<String> valueAliases = new ArrayList<>();
            for (PivotValue pivotValue : node.getPivotValues()) {
                for (int i = 0; i < pivotValue.getExprs().size(); i++) {
                    Expr expr = pivotValue.getExprs().get(i);
                    analyzeExpression(expr, analyzeState, queryScope);
                    if (!Type.canCastTo(expr.getType(), types.get(i))) {
                        throw new SemanticException("Pivot value type %s is not compatible with pivot column type %s",
                                expr.getType(), types.get(i));
                    }
                }
                String alias = pivotValue.getAlias();
                if (alias == null) {
                    if (pivotValue.getExprs().size() == 1) {
                        alias = getAliasName(pivotValue.getExprs().get(0), false, false);
                    } else {
                        alias = "{" + Joiner.on(",")
                                .join(pivotValue.getExprs().stream()
                                        .map(e -> getAliasName(e, false, false))
                                        .collect(Collectors.toList())) + "}";
                    }
                }
                valueAliases.add(alias);

                // construct agg(case when)
                for (int i = 0; i < node.getAggregateFunctions().size(); i++) {
                    PivotAggregation pivotAggregation = node.getAggregateFunctions().get(i);
                    List<Expr> predicates = new ArrayList<>();
                    for (int j = 0; j < node.getPivotColumns().size(); j++) {
                        predicates.add(new BinaryPredicate(
                                BinaryType.EQ,
                                node.getPivotColumns().get(j),
                                pivotValue.getExprs().get(j)));
                    }

                    Expr whenExpr;
                    if (predicates.size() > 1) {
                        whenExpr = new CompoundPredicate(
                                CompoundPredicate.Operator.AND, predicates.get(0), predicates.get(1));
                        for (int j = 2; j < predicates.size(); j++) {
                            whenExpr = new CompoundPredicate(
                                    CompoundPredicate.Operator.AND, whenExpr, predicates.get(j));
                        }
                    } else {
                        whenExpr = predicates.get(0);
                    }

                    // special case: count(), count(*)
                    Expr thenExpr = pivotAggregation.getFunctionCallExpr().getChild(0) == null
                            ? new IntLiteral(1) : pivotAggregation.getFunctionCallExpr().getChild(0);
                    CaseWhenClause caseWhenClause =
                            new CaseWhenClause(whenExpr, thenExpr);
                    List<CaseWhenClause> clauses = Lists.newArrayList(caseWhenClause);

                    CaseExpr caseWhen = new CaseExpr(null, clauses, null);
                    FunctionCallExpr functionCallExpr = new FunctionCallExpr(
                            pivotAggregation.getFunctionCallExpr().getFnName(), Lists.newArrayList(caseWhen));
                    analyzeExpression(functionCallExpr, analyzeState, queryScope);
                    node.addRewrittenAggFunction(functionCallExpr);
                }
            }

            List<Field> queryFields = queryScope.getRelationFields().getAllFields();
            Set<String> usedColumns = node.getUsedColumns().keySet();
            ImmutableList.Builder<Expr> outputExpressionBuilder = ImmutableList.builder();
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (Field field : queryFields) {
                if (!usedColumns.contains(field.getName())) {
                    outputExpressionBuilder.add(field.getOriginExpression());
                    outputFields.add(field);
                    Expr expr = field.getOriginExpression().clone();
                    analyzeExpression(expr, analyzeState, queryScope);
                    node.addGroupByKey(expr);
                }
            }

            for (String valueAlias : valueAliases) {
                for (Pair<String, Expr> pair : aggAliasExprs) {
                    Expr aggExpr = pair.second;
                    String name = aggAliasExprs.size() == 1 ? valueAlias : valueAlias + "_" + pair.first;
                    outputFields.add(new Field(name, aggExpr.getType(), null, aggExpr, true, aggExpr.isNullable()));
                }
            }

            Scope scope = new Scope(RelationId.of(node), new RelationFields(outputFields.build()));
            scope.setParent(queryScope);
            node.setScope(scope);
            return scope;
        }

        @Override
        public Scope visitTableFunction(TableFunctionRelation node, Scope scope) {
            AnalyzeState analyzeState = new AnalyzeState();
            List<Expr> args = node.getFunctionParams().exprs();
            Type[] argTypes = new Type[args.size()];
            for (int i = 0; i < args.size(); ++i) {
                analyzeExpression(args.get(i), analyzeState, scope);
                argTypes[i] = args.get(i).getType();

                AnalyzerUtils.verifyNoAggregateFunctions(args.get(i), "Table Function");
                AnalyzerUtils.verifyNoWindowFunctions(args.get(i), "Table Function");
                AnalyzerUtils.verifyNoGroupingFunctions(args.get(i), "Table Function");
            }
            List<String> names = node.getFunctionParams().getExprsNames();
            String[] namesArray = null;
            if (names != null && !names.isEmpty()) {
                namesArray = names.toArray(String[]::new);
            }
            Function fn = Expr.getBuiltinFunction(node.getFunctionName().getFunction(), argTypes, namesArray,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            if (fn == null) {
                fn = AnalyzerUtils.getUdfFunction(session, node.getFunctionName(), argTypes);
            }

            if (fn == null) {
                if (namesArray == null) {
                    throw new SemanticException("Unknown table function '%s(%s)'", node.getFunctionName().getFunction(),
                            Arrays.stream(argTypes).map(Object::toString).collect(Collectors.joining(",")));
                } else {
                    throw new SemanticException("Unknown table function '%s(%s)', the function doesn't support named " +
                            "arguments or has invalid arguments",
                            node.getFunctionName().getFunction(), node.getFunctionParams().getNamedArgStr());
                }
            }

            if (namesArray != null) {
                Preconditions.checkState(fn.hasNamedArg());
                node.getFunctionParams().reorderNamedArgAndAppendDefaults(fn);
            } else if (node.getFunctionParams().exprs().size() < fn.getNumArgs()) {
                node.getFunctionParams().appendPositionalDefaultArgExprs(fn);
            }
            Preconditions.checkState(node.getFunctionParams().exprs().size() == fn.getNumArgs());

            if (!(fn instanceof TableFunction)) {
                throw new SemanticException("'%s(%s)' is not table function", node.getFunctionName().getFunction(),
                        Arrays.stream(argTypes).map(Object::toString).collect(Collectors.joining(",")));
            }

            TableFunction tableFunction = (TableFunction) fn;
            tableFunction.setIsLeftJoin(node.getIsLeftJoin());
            node.setTableFunction(tableFunction);
            node.setChildExpressions(node.getFunctionParams().exprs());

            if (node.getColumnOutputNames() == null) {
                if (tableFunction.getFunctionName().getFunction().equals("unnest")) {
                    // If the unnest variadic function does not explicitly specify column name,
                    // all column names are `unnest`. This refers to the return column name of postgresql.
                    List<String> columnNames = new ArrayList<>();
                    for (int i = 0; i < tableFunction.getTableFnReturnTypes().size(); ++i) {
                        columnNames.add("unnest");
                    }
                    node.setColumnOutputNames(columnNames);
                } else {
                    node.setColumnOutputNames(new ArrayList<>(tableFunction.getDefaultColumnNames()));
                }
            } else {
                if (node.getColumnOutputNames().size() != tableFunction.getTableFnReturnTypes().size()) {
                    throw new SemanticException("table %s has %s columns available but %s columns specified",
                            node.getAlias().getTbl(),
                            tableFunction.getTableFnReturnTypes().size(),
                            node.getColumnOutputNames().size());
                }
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < tableFunction.getTableFnReturnTypes().size(); ++i) {
                String colName = node.getColumnOutputNames().get(i);

                Field field = new Field(colName,
                        tableFunction.getTableFnReturnTypes().get(i),
                        node.getResolveTableName(),
                        new SlotRef(node.getResolveTableName(), colName, colName));
                fields.add(field);
            }

            Scope outputScope = new Scope(RelationId.of(node), new RelationFields(fields.build()));
            node.setScope(outputScope);
            return outputScope;
        }

        @Override
        public Scope visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Scope scope) {
            Scope ignored = visitJoin(node, scope);
            // Only the scope of the table function is visible outside.
            node.setScope(node.getRight().getScope());
            return node.getScope();
        }

    }

    public Table resolveTable(TableRelation tableRelation) {
        TableName tableName = tableRelation.getName();
        try {
            tableName.normalization(session);
            String catalogName = tableName.getCatalog();
            String dbName = tableName.getDb();
            String tbName = tableName.getTbl();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }

            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
            }

            Database db;
            try (Timer ignored = Tracers.watchScope("AnalyzeDatabase")) {
                db = metadataMgr.getDb(session, catalogName, dbName);
            }

            MetaUtils.checkDbNullAndReport(db, dbName);

            Table table = null;
            if (tableRelation.isSyncMVQuery()) {
                try (Timer ignored = Tracers.watchScope("AnalyzeSyncMV")) {
                    Pair<Table, MaterializedIndexMeta> materializedIndex =
                            GlobalStateMgr.getCurrentState().getLocalMetastore().getMaterializedViewIndex(dbName, tbName);
                    if (materializedIndex != null) {
                        Table mvTable = materializedIndex.first;
                        Preconditions.checkState(mvTable != null);
                        Preconditions.checkState(mvTable instanceof OlapTable);
                        // Add read lock to avoid concurrent problems.
                        OlapTable mvOlapTable = new OlapTable(mvTable.getType());
                        ((OlapTable) mvTable).copyOnlyForQuery(mvOlapTable);
                        // Copy the necessary olap table meta to avoid changing original meta;
                        mvOlapTable.setBaseIndexId(materializedIndex.second.getIndexId());
                        table = mvOlapTable;
                    }
                }
            } else {
                // treat it as a temporary table first
                try (Timer ignored = Tracers.watchScope("AnalyzeTemporaryTable")) {
                    table = metadataMgr.getTemporaryTable(session.getSessionId(), catalogName, db.getId(), tbName);
                }
                if (table == null) {
                    try (Timer ignored = Tracers.watchScope("AnalyzeTable")) {
                        table = metadataMgr.getTable(session, catalogName, dbName, tbName);
                    }
                }
            }

            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, dbName + "." + tbName);
            }

            if (table.isNativeTableOrMaterializedView() &&
                    (((OlapTable) table).getState() == OlapTable.OlapTableState.RESTORE
                            || ((OlapTable) table).getState() == OlapTable.OlapTableState.RESTORE_WITH_LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_STATE, "RESTORING");
            }

            PartitionNames partitionNamesObject = tableRelation.getPartitionNames();
            if (table.isExternalTableWithFileSystem() && partitionNamesObject != null) {
                throw unsupportedException("Unsupported table type for partition clause, type: " + table.getType());
            }

            if (partitionNamesObject != null && table.isNativeTable()) {
                List<String> partitionNames = partitionNamesObject.getPartitionNames();
                if (partitionNames != null) {
                    boolean isTemp = partitionNamesObject.isTemp();
                    for (String partitionName : partitionNames) {
                        Partition partition = table.getPartition(partitionName, isTemp);
                        if (partition == null) {
                            throw new SemanticException("Unknown partition '%s' in table '%s'", partitionName,
                                    table.getName());
                        }
                    }
                }
            }

            if (table instanceof OlapTable) {
                ((OlapTable) table).maySetDatabaseName(db.getFullName());
            }

            return table;
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    private Table resolveTableFunctionTable(Map<String, String> properties, Consumer<TableFunctionTable> pushDownSchemaFunc) {
        try {
            return new TableFunctionTable(properties, pushDownSchemaFunc);
        } catch (DdlException e) {
            throw new StorageAccessException(e);
        }
    }

    private void analyzeExpression(Expr expr, AnalyzeState analyzeState, Scope scope) {
        ExpressionAnalyzer.analyzeExpression(expr, analyzeState, scope, session);
    }

    public static void checkJoinEqual(Expr expr) {
        if (expr instanceof BinaryPredicate) {
            for (Expr child : expr.getChildren()) {
                if (!child.getType().canJoinOn()) {
                    throw new SemanticException(Type.NOT_SUPPORT_JOIN_ERROR_MSG);
                }
            }
        } else {
            for (Expr child : expr.getChildren()) {
                checkJoinEqual(child);
            }
        }
    }

    // Support alias referring to select item
    // eg: select trim(c1) as a, trim(c2) as b, concat(a,',', b) from t1
    // Note: alias in where clause is not handled now
    // eg: select trim(c1) as a, trim(c2) as b, concat(a,',', b) from t1 where a != 'x'
    private static class RewriteAliasVisitor implements AstVisitor<Expr, Void> {
        private final Map<String, Expr> aliases = new HashMap<>();
        private final Set<String> aliasesMaybeAmbiguous = new HashSet<>();
        private final Map<String, Expr> resolvedAliases = new HashMap<>();
        private final LinkedList<String> resolvingAlias = new LinkedList<>();
        private final Scope sourceScope;
        private final ConnectContext session;

        public RewriteAliasVisitor(Scope sourceScope, ConnectContext session) {
            this.sourceScope = sourceScope;
            this.session = session;
        }

        @Override
        public Expr visitExpression(Expr expr, Void context) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, visit(expr.getChild(i)));
            }
            return expr;
        }

        @Override
        public Expr visitSlot(SlotRef slotRef, Void context) {
            // We treat it as column name rather than alias when table name is specified
            if (slotRef.getTblNameWithoutAnalyzed() != null) {
                return slotRef;
            }
            // Alias is case-insensitive
            String ref = slotRef.getColumnName().toLowerCase();
            Expr e = aliases.get(ref);
            // Ignore slot rewrite if the `slotRef` is not in alias map
            if (e == null) {
                return slotRef;
            }
            // If alias is same with table column name where this alias is defined, we directly use table column name.
            // eg: DATE(pt) as pt
            if (ref.equals(resolvingAlias.peekLast())) {
                return slotRef;
            }
            // If alias is same with table column name, we directly use table column name.
            // Otherwise, we treat it as alias.
            // It behaves the same as that in `SelectAnalyzer.RewriteAliasVisitor`
            // eg: select trim(t1a) as t1c, concat(t1c,','),t1c from test_all_type
            if (sourceScope.tryResolveField(slotRef).isPresent() &&
                    !session.getSessionVariable().getEnableGroupbyUseOutputAlias()) {
                return slotRef;
            }
            // Referring to a duplicated alias is ambiguous
            if (aliasesMaybeAmbiguous.contains(ref)) {
                throw new SemanticException("Column " + ref + " is ambiguous", slotRef.getPos());
            }
            // Use short circuit to avoid duplicated resolving of same alias
            if (resolvedAliases.containsKey(ref)) {
                return resolvedAliases.get(ref);
            }
            if (resolvingAlias.contains(ref)) {
                throw new SemanticException("Cyclic aliases: " + ref, slotRef.getPos());
            }

            // Use resolvingAliases to detect cyclic aliases
            resolvingAlias.add(ref);
            e = visit(e);
            resolvedAliases.put(ref, e);
            resolvingAlias.removeLast();
            return e;
        }

        @Override
        public Expr visitSelect(SelectRelation selectRelation, Void context) {
            // Collect alias in select relation
            for (SelectListItem item : selectRelation.getSelectList().getItems()) {
                String alias = item.getAlias();
                if (item.getAlias() == null) {
                    continue;
                }

                // Alias is case-insensitive
                Expr lastAssociatedExpr = aliases.putIfAbsent(alias.toLowerCase(), item.getExpr());
                if (lastAssociatedExpr != null) {
                    // Duplicate alias is allowed, eg: select a.v1 as v, a.v2 as v from t0 a,
                    // But it should be ambiguous when the alias is used in the order by expr,
                    // eg: select a.v1 as v, a.v2 as v from t0 a order by v
                    aliasesMaybeAmbiguous.add(alias.toLowerCase());
                }
            }
            if (aliases.isEmpty()) {
                return null;
            }
            for (SelectListItem item : selectRelation.getSelectList().getItems()) {
                if (item.getExpr() == null) {
                    continue;
                }
                if (item.getAlias() == null) {
                    item.setExpr(visit(item.getExpr()));
                    continue;
                }
                // Alias is case-insensitive
                String alias = item.getAlias().toLowerCase();
                resolvingAlias.add(alias);
                // Can't use computeIfAbsent here due to resolvedAliases is also modified in `visitSlot`,
                // otherwise `ConcurrentModificationException` will be thrown
                Expr rewrittenExpr = resolvedAliases.containsKey(alias) && !aliasesMaybeAmbiguous.contains(alias)
                        ? resolvedAliases.get(alias) : visit(item.getExpr());
                resolvedAliases.putIfAbsent(alias, rewrittenExpr);
                item.setExpr(rewrittenExpr);
                resolvingAlias.removeLast();
            }
            return null;
        }
    }
}
