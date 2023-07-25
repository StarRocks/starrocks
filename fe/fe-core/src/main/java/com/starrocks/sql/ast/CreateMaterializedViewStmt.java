// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateMaterializedViewStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.mvpattern.MVColumnBitmapUnionPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnHLLUnionPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnOneChildPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnPercentileUnionPattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Materialized view is performed to materialize the results of query.
 * This clause is used to create a new materialized view for a specified table
 * through a specified query stmt.
 * <p>
 * Syntax:
 * CREATE MATERIALIZED VIEW [MV name] (
 * SELECT select_expr[, select_expr ...]
 * FROM [Base view name]
 * GROUP BY column_name[, column_name ...]
 * ORDER BY column_name[, column_name ...])
 * [PROPERTIES ("key" = "value")]
 */
public class CreateMaterializedViewStmt extends DdlStmt {

    private static final Logger LOG = LogManager.getLogger(CreateMaterializedViewStmt.class);
    public static final String MATERIALIZED_VIEW_NAME_PREFIX = "mv_";
    public static final Map<String, MVColumnPattern> FN_NAME_TO_PATTERN;

    static {
        FN_NAME_TO_PATTERN = Maps.newHashMap();
        FN_NAME_TO_PATTERN.put(AggregateType.SUM.name().toLowerCase(),
                new MVColumnOneChildPattern(AggregateType.SUM.name().toLowerCase()));
        FN_NAME_TO_PATTERN.put(AggregateType.MIN.name().toLowerCase(),
                new MVColumnOneChildPattern(AggregateType.MIN.name().toLowerCase()));
        FN_NAME_TO_PATTERN.put(AggregateType.MAX.name().toLowerCase(),
                new MVColumnOneChildPattern(AggregateType.MAX.name().toLowerCase()));
        FN_NAME_TO_PATTERN.put(FunctionSet.COUNT, new MVColumnOneChildPattern(FunctionSet.COUNT));
        FN_NAME_TO_PATTERN.put(FunctionSet.BITMAP_UNION, new MVColumnBitmapUnionPattern());
        FN_NAME_TO_PATTERN.put(FunctionSet.HLL_UNION, new MVColumnHLLUnionPattern());
        FN_NAME_TO_PATTERN.put(FunctionSet.PERCENTILE_UNION, new MVColumnPercentileUnionPattern());
    }

    private final String mvName;
    private final Map<String, String> properties;

    private final QueryStatement queryStatement;
    /**
     * origin stmt: select k1, k2, v1, sum(v2) from base_table group by k1, k2, v1
     * mvColumnItemList: [k1: {name: k1, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     * k2: {name: k2, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     * v1: {name: v1, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     * v2: {name: v2, isKey: false, aggType: sum, isAggregationTypeImplicit: false}]
     * This order of mvColumnItemList is meaningful.
     */
    private List<MVColumnItem> mvColumnItemList = Lists.newArrayList();
    private String baseIndexName;
    private String dbName;
    private KeysType mvKeysType = KeysType.DUP_KEYS;

    //if process is replaying log, isReplay is true, otherwise is false, avoid replay process error report, only in Rollup or MaterializedIndexMeta is true
    private boolean isReplay = false;

    public CreateMaterializedViewStmt(String mvName, QueryStatement queryStatement, Map<String, String> properties) {
        this.mvName = mvName;
        this.queryStatement = queryStatement;
        this.properties = properties;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setIsReplay(boolean isReplay) {
        this.isReplay = isReplay;
    }

    public boolean isReplay() {
        return isReplay;
    }

    public String getMVName() {
        return mvName;
    }

    public List<MVColumnItem> getMVColumnItemList() {
        return mvColumnItemList;
    }

    public void setMvColumnItemList(List<MVColumnItem> mvColumnItemList) {
        this.mvColumnItemList = mvColumnItemList;
    }

    public String getBaseIndexName() {
        return baseIndexName;
    }

    public void setBaseIndexName(String baseIndexName) {
        this.baseIndexName = baseIndexName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getDBName() {
        return dbName;
    }

    public void setDBName(String dbName) {
        this.dbName = dbName;
    }

    public KeysType getMVKeysType() {
        return mvKeysType;
    }

    public void setMvKeysType(KeysType mvKeysType) {
        this.mvKeysType = mvKeysType;
    }

    public Map<String, Expr> parseDefineExprWithoutAnalyze(String originalSql) throws AnalysisException {
        Map<String, Expr> result = Maps.newHashMap();
        SelectList selectList = null;
        QueryRelation queryRelation = queryStatement.getQueryRelation();
        if (queryRelation instanceof SelectRelation) {
            selectList = ((SelectRelation) queryRelation).getSelectList();
        }
        if (selectList == null) {
            LOG.warn("parse defineExpr may not correctly for sql [{}] ", originalSql);
            return result;
        }
        for (SelectListItem selectListItem : selectList.getItems()) {
            Expr selectListItemExpr = selectListItem.getExpr();
            if (selectListItemExpr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) selectListItemExpr;
                result.put(slotRef.getColumnName(), null);
            } else if (selectListItemExpr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItemExpr;
                List<SlotRef> slots = new ArrayList<>();
                functionCallExpr.collect(SlotRef.class, slots);
                Preconditions.checkArgument(slots.size() == 1);
                String baseColumnName = slots.get(0).getColumnName().toLowerCase();
                String functionName = functionCallExpr.getFnName().getFunction();
                SlotRef baseSlotRef = slots.get(0);
                switch (functionName.toLowerCase()) {
                    case "sum":
                    case "min":
                    case "max":
                        result.put(baseColumnName, null);
                        break;
                    case FunctionSet.BITMAP_UNION:
                        if (functionCallExpr.getChild(0) instanceof FunctionCallExpr) {
                            CastExpr castExpr = new CastExpr(new TypeDef(Type.VARCHAR), baseSlotRef);
                            List<Expr> params = Lists.newArrayList();
                            params.add(castExpr);
                            FunctionCallExpr defineExpr = new FunctionCallExpr(FunctionSet.TO_BITMAP, params);
                            result.put(mvColumnBuilder(functionName, baseColumnName), defineExpr);
                        } else {
                            result.put(baseColumnName, null);
                        }
                        break;
                    case FunctionSet.HLL_UNION:
                        if (functionCallExpr.getChild(0) instanceof FunctionCallExpr) {
                            CastExpr castExpr = new CastExpr(new TypeDef(Type.VARCHAR), baseSlotRef);
                            List<Expr> params = Lists.newArrayList();
                            params.add(castExpr);
                            FunctionCallExpr defineExpr = new FunctionCallExpr(FunctionSet.HLL_HASH, params);
                            result.put(mvColumnBuilder(functionName, baseColumnName), defineExpr);
                        } else {
                            result.put(baseColumnName, null);
                        }
                        break;
                    case FunctionSet.PERCENTILE_UNION:
                        if (functionCallExpr.getChild(0) instanceof FunctionCallExpr) {
                            CastExpr castExpr = new CastExpr(new TypeDef(Type.VARCHAR), baseSlotRef);
                            List<Expr> params = Lists.newArrayList();
                            params.add(castExpr);
                            FunctionCallExpr defineExpr = new FunctionCallExpr(FunctionSet.PERCENTILE_HASH, params);
                            result.put(mvColumnBuilder(functionName, baseColumnName), defineExpr);
                        } else {
                            result.put(baseColumnName, null);
                        }
                        break;
                    case FunctionSet.COUNT:
                        Expr defineExpr = new CaseExpr(null, Lists.newArrayList(
                                new CaseWhenClause(new IsNullPredicate(slots.get(0), false),
                                        new IntLiteral(0, Type.BIGINT))), new IntLiteral(1, Type.BIGINT));
                        result.put(mvColumnBuilder(functionName, baseColumnName), defineExpr);
                        break;
                    default:
                        throw new AnalysisException("Unsupported function:" + functionName);
                }
            } else {
                throw new AnalysisException("Unsupported select item:" + selectListItem.toSql());
            }
        }
        return result;
    }

    public static String mvColumnBuilder(String functionName, String sourceColumnName) {
        return new StringBuilder().append(MATERIALIZED_VIEW_NAME_PREFIX).append(functionName).append("_")
                .append(sourceColumnName).toString();
    }

    @Override
    public String toSql() {
        return null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedViewStmt(this, context);
    }

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new OldMVAnalyzerVisitor().visit(stmt, session);
    }

    public static class OldMVAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitCreateMaterializedViewStmt(CreateMaterializedViewStmt statement,
                                                    ConnectContext context) {
            QueryStatement queryStatement = statement.getQueryStatement();
            long originSelectLimit = context.getSessionVariable().getSqlSelectLimit();
            // ignore limit in creating mv
            context.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
            com.starrocks.sql.analyzer.Analyzer.analyze(statement.getQueryStatement(), context);
            context.getSessionVariable().setSqlSelectLimit(originSelectLimit);

            // forbid explain query
            if (queryStatement.isExplain()) {
                throw new IllegalArgumentException("Materialized view does not support explain query");
            }

            if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
                throw new SemanticException("Materialized view query statement only support select");
            }
            Map<TableName, Table> tables = AnalyzerUtils.collectAllTableAndViewWithAlias(queryStatement);
            if (tables.size() != 1) {
                throw new SemanticException("The materialized view only support one table in from clause.");
            }
            Map.Entry<TableName, Table> entry = tables.entrySet().iterator().next();
            Table table = entry.getValue();
            if (table instanceof View) {
                // Only in order to make the error message keep compatibility
                throw new SemanticException("Do not support alter non-OLAP table[" + table.getName() + "]");
            } else if (!(table instanceof OlapTable)) {
                throw new SemanticException("The materialized view only support olap table.");
            }
            TableName tableName = entry.getKey();
            statement.setBaseIndexName(table.getName());
            statement.setDBName(tableName.getDb());

            SelectRelation selectRelation = ((SelectRelation) queryStatement.getQueryRelation());
            if (!(selectRelation.getRelation() instanceof TableRelation)) {
                throw new SemanticException("Materialized view query statement only support direct query from table.");
            }
            int beginIndexOfAggregation = genColumnAndSetIntoStmt(statement, selectRelation);
            if (selectRelation.isDistinct() || selectRelation.hasAggregation()) {
                statement.setMvKeysType(KeysType.AGG_KEYS);
            }
            if (selectRelation.hasWhereClause()) {
                throw new SemanticException("The where clause is not supported in add materialized view clause, expr:"
                        + selectRelation.getWhereClause().toSql());
            }
            if (selectRelation.hasHavingClause()) {
                throw new SemanticException("The having clause is not supported in add materialized view clause, expr:"
                        + selectRelation.getHavingClause().toSql());
            }
            analyzeOrderByClause(statement, selectRelation, beginIndexOfAggregation);
            if (selectRelation.hasLimit()) {
                throw new SemanticException("The limit clause is not supported in add materialized view clause, expr:"
                        + " limit " + selectRelation.getLimit());
            }
            final String countPrefix = new StringBuilder().append(MATERIALIZED_VIEW_NAME_PREFIX)
                    .append(FunctionSet.COUNT).append("_").toString();
            for (MVColumnItem mvColumnItem : statement.getMVColumnItemList()) {
                if (!statement.isReplay() && mvColumnItem.isKey() && !mvColumnItem.getType().canBeMVKey()) {
                    throw new SemanticException(
                            String.format("Invalid data type of materialized key column '%s': '%s'",
                                    mvColumnItem.getName(), mvColumnItem.getType()));
                }
                if (mvColumnItem.getName().startsWith(countPrefix)
                        && ((OlapTable) table).getKeysType().isAggregationFamily()) {
                    throw new SemanticException("Aggregate type table do not support count function in materialized view");
                }
            }
            return null;
        }
    }

    private static int genColumnAndSetIntoStmt(CreateMaterializedViewStmt statement, SelectRelation selectRelation) {
        List<MVColumnItem> mvColumnItemList = Lists.newArrayList();

        boolean meetAggregate = false;
        Set<String> mvColumnNameSet = Sets.newHashSet();
        int beginIndexOfAggregation = -1;
        StringJoiner joiner = new StringJoiner(", ", "[", "]");

        List<SelectListItem> selectListItems = selectRelation.getSelectList().getItems();
        for (int i = 0; i < selectListItems.size(); ++i) {
            SelectListItem selectListItem = selectListItems.get(i);
            if (selectListItem.isStar()) {
                throw new SemanticException("The materialized view currently does not support * in select statement");
            }

            Expr selectListItemExpr = selectListItem.getExpr();
            if (!(selectListItemExpr instanceof SlotRef) && !(selectListItemExpr instanceof FunctionCallExpr)) {
                throw new SemanticException("The materialized view only support the single column or function expr. "
                        + "Error column: " + selectListItemExpr.toSql());
            }
            if (selectListItemExpr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) selectListItemExpr;
                String columnName = slotRef.getColumnName().toLowerCase();
                joiner.add(columnName);
                if (meetAggregate) {
                    throw new SemanticException("Any single column should be before agg column. " +
                            "Column %s at wrong location", columnName);
                }
                // check duplicate column
                if (!mvColumnNameSet.add(columnName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnName);
                }
                MVColumnItem mvColumnItem = new MVColumnItem(columnName, slotRef.getType());
                mvColumnItemList.add(mvColumnItem);
            } else {
                // Function must match pattern.
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItemExpr;
                String functionName = functionCallExpr.getFnName().getFunction();
                MVColumnPattern mvColumnPattern =
                        CreateMaterializedViewStmt.FN_NAME_TO_PATTERN.get(functionName.toLowerCase());
                if (mvColumnPattern == null) {
                    throw new SemanticException(
                            "Materialized view does not support this function:%s, supported functions are: %s",
                            functionCallExpr.toSqlImpl(), FN_NAME_TO_PATTERN.keySet());
                }
                // current version not support count(distinct) function in creating materialized view
                if (!statement.isReplay() && functionCallExpr.isDistinct()) {
                    throw new SemanticException(
                            "Materialized view does not support distinct function " + functionCallExpr.toSqlImpl());
                }
                if (!mvColumnPattern.match(functionCallExpr)) {
                    throw new SemanticException(
                            "The function " + functionName + " must match pattern:" + mvColumnPattern.toString());
                }
                if (functionCallExpr.getChild(0) instanceof CastExpr) {
                    throw new SemanticException(
                            "The function " + functionName + " disable cast expression");
                }
                // check duplicate column
                List<SlotRef> slots = new ArrayList<>();
                functionCallExpr.collect(SlotRef.class, slots);
                Preconditions.checkArgument(slots.size() == 1);
                String columnName = slots.get(0).getColumnName().toLowerCase();
                if (!mvColumnNameSet.add(columnName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnName);
                }

                if (beginIndexOfAggregation == -1) {
                    beginIndexOfAggregation = i;
                }
                meetAggregate = true;
<<<<<<< HEAD
                mvColumnItemList.add(buildMVColumnItem(functionCallExpr, statement.isReplay()));
                joiner.add(functionCallExpr.toSqlImpl());
=======

                mvColumnItem = buildAggColumnItem(selectListItem, slots);
                if (!mvColumnNameSet.add(mvColumnItem.getName())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, mvColumnItem.getName());
                }
                mvColumnItemList.add(mvColumnItem);
            } else {
                if (meetAggregate) {
                    throw new UnsupportedMVException("Any single column should be before agg column. " +
                            "Column %s at wrong location", selectListItemExpr.toMySql());
                }
                // NOTE: If `selectListItemExpr` contains aggregate function, we can not support it.
                if (selectListItemExpr.containsAggregate()) {
                    throw new UnsupportedMVException("Aggregate function with function expr is not supported yet",
                            selectListItemExpr.toMySql());
                }

                mvColumnItem = buildNonAggColumnItem(selectListItem, slots);
                if (!mvColumnNameSet.add(mvColumnItem.getName())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, mvColumnItem.getName());
                }

                mvColumnItemList.add(mvColumnItem);
                joiner.add(selectListItemExpr.toSql());
            }
            Set<String> fullSchemaColNames = table.getFullSchema().stream().map(Column::getName).collect(Collectors.toSet());
            if (fullSchemaColNames.contains(mvColumnItem.getName())) {
                Expr existedDefinedExpr = table.getColumn(mvColumnItem.getName()).getDefineExpr();
                if (existedDefinedExpr != null && !existedDefinedExpr.toSqlWithoutTbl()
                        .equalsIgnoreCase(mvColumnItem.getDefineExpr().toSqlWithoutTbl())) {
                    throw new UnsupportedMVException(
                            String.format("The mv column %s has already existed in the table's full " +
                                            "schema, old expr: %s, new expr: %s", selectListItem.getAlias(),
                                    existedDefinedExpr.toSqlWithoutTbl(),
                                    mvColumnItem.getDefineExpr().toSqlWithoutTbl()));
                }
>>>>>>> 1f503fe8fc ([BugFix] Support count(1)/count(*)/count(col) rewrite when col is not nullable (#27728))
            }
        }
        if (beginIndexOfAggregation == 0) {
            throw new SemanticException("Only %s found in the select list. " +
                    "Please add group by clause and at least one group by column in the select list", joiner);
        }
        statement.setMvColumnItemList(mvColumnItemList);
        return beginIndexOfAggregation;
    }

    private static MVColumnItem buildMVColumnItem(FunctionCallExpr functionCallExpr, boolean isReplay) {
        String functionName = functionCallExpr.getFnName().getFunction();
        List<SlotRef> slots = new ArrayList<>();
        functionCallExpr.collect(SlotRef.class, slots);
        Preconditions.checkArgument(slots.size() == 1);
        SlotRef baseColumnRef = slots.get(0);
        String baseColumnName = baseColumnRef.getColumnName().toLowerCase();
        Type baseType = baseColumnRef.getType();
        Expr functionChild0 = functionCallExpr.getChild(0);
        String mvColumnName;
        AggregateType mvAggregateType;
        Expr defineExpr = null;
        Type type;
        switch (functionName.toLowerCase()) {
            case "sum":
                mvColumnName = baseColumnName;
                mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                PrimitiveType baseColumnType = baseColumnRef.getType().getPrimitiveType();
                if (baseColumnType == PrimitiveType.TINYINT || baseColumnType == PrimitiveType.SMALLINT
                        || baseColumnType == PrimitiveType.INT) {
                    type = Type.BIGINT;
                } else if (baseColumnType == PrimitiveType.FLOAT) {
                    type = Type.DOUBLE;
                } else {
                    type = baseType;
                }
                break;
            case "min":
            case "max":
                mvColumnName = baseColumnName;
                mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                type = baseType;
                break;
            case FunctionSet.BITMAP_UNION:
                // Compatible aggregation models
                if (baseColumnRef.getType().getPrimitiveType() == PrimitiveType.BITMAP) {
                    mvColumnName = baseColumnName;
                } else {
                    mvColumnName = mvColumnBuilder(functionName, baseColumnName);
                    defineExpr = functionChild0;
                }
                mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                type = Type.BITMAP;
                break;
            case FunctionSet.HLL_UNION:
                // Compatible aggregation models
                if (baseColumnRef.getType().getPrimitiveType() == PrimitiveType.HLL) {
                    mvColumnName = baseColumnName;
                } else {
                    mvColumnName = mvColumnBuilder(functionName, baseColumnName);
                    defineExpr = functionChild0;
                }
                mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                type = Type.HLL;
                break;
            case FunctionSet.PERCENTILE_UNION:
                if (baseColumnRef.getType().getPrimitiveType() == PrimitiveType.PERCENTILE) {
                    mvColumnName = baseColumnName;
                } else {
                    mvColumnName = mvColumnBuilder(functionName, baseColumnName);
                    defineExpr = functionChild0;
                }
                mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                type = Type.PERCENTILE;
                break;
            case FunctionSet.COUNT:
                mvColumnName = mvColumnBuilder(functionName, baseColumnName);
                mvAggregateType = AggregateType.SUM;
                defineExpr = new CaseExpr(null, Lists.newArrayList(new CaseWhenClause(
                        new IsNullPredicate(baseColumnRef, false),
                        new IntLiteral(0, Type.BIGINT))), new IntLiteral(1, Type.BIGINT));
                type = Type.BIGINT;
                break;
            default:
                throw new SemanticException("Unsupported function:" + functionName);
        }

        // If isReplay, don't check compatibility because materialized view maybe already created before.
        if (!isReplay && !mvAggregateType.checkCompatibility(type)) {
            throw new SemanticException(
                    String.format("Invalid aggregate function '%s' for '%s'", mvAggregateType, type));
        }
        return new MVColumnItem(mvColumnName, type, mvAggregateType, functionCallExpr.isNullable(), false, defineExpr,
                baseColumnName);
    }

    private static void analyzeOrderByClause(CreateMaterializedViewStmt statement,
                                             SelectRelation selectRelation,
                                             int beginIndexOfAggregation) {
        if (!selectRelation.hasOrderByClause() || selectRelation.getGroupBy().size() != selectRelation.getOrderBy().size()) {
            supplyOrderColumn(statement);
            return;
        }

        List<OrderByElement> orderByElements = selectRelation.getOrderBy();
        List<MVColumnItem> mvColumnItemList = statement.getMVColumnItemList();

        if (orderByElements.size() > mvColumnItemList.size()) {
            throw new SemanticException("The number of columns in order clause must be less then " + "the number of "
                    + "columns in select clause");
        }
        if (beginIndexOfAggregation != -1 && (orderByElements.size() != (beginIndexOfAggregation))) {
            throw new SemanticException("The order-by columns must be identical to the group-by columns");
        }
        for (int i = 0; i < orderByElements.size(); i++) {
            Expr orderByElement = orderByElements.get(i).getExpr();
            if (!(orderByElement instanceof SlotRef)) {
                throw new SemanticException("The column in order clause must be original column without calculation. "
                        + "Error column: " + orderByElement.toSql());
            }
            MVColumnItem mvColumnItem = mvColumnItemList.get(i);
            SlotRef slotRef = (SlotRef) orderByElement;
            if (!mvColumnItem.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                throw new SemanticException("The order of columns in order by clause must be same as "
                        + "the order of columns in select list");
            }
            Preconditions.checkState(mvColumnItem.getAggregationType() == null);
            mvColumnItem.setIsKey(true);
        }

        // supplement none aggregate type
        for (MVColumnItem mvColumnItem : mvColumnItemList) {
            if (mvColumnItem.isKey()) {
                continue;
            }
            if (mvColumnItem.getAggregationType() != null) {
                break;
            }
            mvColumnItem.setAggregationType(AggregateType.NONE, true);
        }
    }

    /*
    This function is used to supply order by columns and calculate short key count
    */
    private static void supplyOrderColumn(CreateMaterializedViewStmt statement) {
        List<MVColumnItem> mvColumnItemList = statement.getMVColumnItemList();

        /*
         * The keys type of Materialized view is aggregation.
         * All of group by columns are keys of materialized view.
         */
        if (statement.getMVKeysType() == KeysType.AGG_KEYS) {
            for (MVColumnItem mvColumnItem : statement.getMVColumnItemList()) {
                if (mvColumnItem.getAggregationType() != null) {
                    break;
                }
                if (!mvColumnItem.getType().isScalarType()) {
                    throw new SemanticException("Key column must be scalar type: " + mvColumnItem.getName());
                }
                mvColumnItem.setIsKey(true);
            }
        } else if (statement.getMVKeysType() == KeysType.DUP_KEYS) {
            /*
             * There is no aggregation function in materialized view.
             * Supplement key of MV columns
             * The key is same as the short key in duplicate table
             * For example: select k1, k2 ... kn from t1
             * The default key columns are first 36 bytes of the columns in define order.
             * If the number of columns in the first 36 is more than 3, the first 3 columns will be used.
             * column: k1, k2, k3. The key is true.
             * Supplement non-key of MV columns
             * column: k4... kn. The key is false, aggregation type is none, isAggregationTypeImplicit is true.
             */
            int theBeginIndexOfValue = 0;
            // supply key
            int keySizeByte = 0;
            for (; theBeginIndexOfValue < mvColumnItemList.size(); theBeginIndexOfValue++) {
                MVColumnItem column = mvColumnItemList.get(theBeginIndexOfValue);
                keySizeByte += column.getType().getIndexSize();
                if (theBeginIndexOfValue + 1 > FeConstants.shortkey_max_column_count ||
                        keySizeByte > FeConstants.shortkey_maxsize_bytes) {
                    if (theBeginIndexOfValue == 0 && column.getType().getPrimitiveType().isCharFamily()) {
                        column.setIsKey(true);
                        theBeginIndexOfValue++;
                    }
                    break;
                }
                if (!column.getType().canBeMVKey()) {
                    break;
                }
                if (column.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                    column.setIsKey(true);
                    theBeginIndexOfValue++;
                    break;
                }
                column.setIsKey(true);
            }
            if (mvColumnItemList.isEmpty()) {
                throw new SemanticException("Empty schema");
            }
            if (theBeginIndexOfValue == 0) {
                throw new SemanticException("Data type of first column cannot be " + mvColumnItemList.get(0).getType());
            }
            // supply value
            for (; theBeginIndexOfValue < mvColumnItemList.size(); theBeginIndexOfValue++) {
                MVColumnItem mvColumnItem = mvColumnItemList.get(theBeginIndexOfValue);
                mvColumnItem.setAggregationType(AggregateType.NONE, true);
            }
        }
    }
}
