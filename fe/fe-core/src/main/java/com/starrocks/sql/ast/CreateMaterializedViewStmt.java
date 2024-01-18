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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.FeConstants;
import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReport;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.UnsupportedMVException;
import com.starrocks.sql.analyzer.mvpattern.MVColumnBitmapAggPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnBitmapUnionPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnHLLUnionPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnOneChildPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnPercentileUnionPattern;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.mv.MVUtils.MATERIALIZED_VIEW_NAME_PREFIX;

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
        FN_NAME_TO_PATTERN.put(FunctionSet.BITMAP_AGG, new MVColumnBitmapAggPattern());
        FN_NAME_TO_PATTERN.put(FunctionSet.HLL_UNION, new MVColumnHLLUnionPattern());
        FN_NAME_TO_PATTERN.put(FunctionSet.PERCENTILE_UNION, new MVColumnPercentileUnionPattern());
    }

    private final TableName mvTableName;
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

    private Expr whereClause;
    private String baseIndexName;
    private String dbName;
    private KeysType mvKeysType = KeysType.DUP_KEYS;

    // If the process is replaying log, isReplay is true, otherwise is false,
    // avoid throwing error during replay process, only in Rollup or MaterializedIndexMeta is true.
    private boolean isReplay = false;

    public static String WHERE_PREDICATE_COLUMN_NAME = "__WHERE_PREDICATION";

    public CreateMaterializedViewStmt(TableName mvTableName, QueryStatement queryStatement, Map<String, String> properties) {
        super(NodePosition.ZERO);
        this.mvTableName = mvTableName;
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
        return mvTableName.getTbl();
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

    public Expr getWhereClause() {
        return whereClause;
    }

    // NOTE: This method is used to replay persistent MaterializedViewMeta,
    // so need keep the same with `genColumnAndSetIntoStmt` and keep compatible with old version policy.
    public Map<String, Expr> parseDefineExprWithoutAnalyze(String originalSql) throws AnalysisException {
        Map<String, Expr> result = Maps.newHashMap();
        SelectList selectList = null;
        QueryRelation queryRelation = queryStatement.getQueryRelation();
        if (queryRelation instanceof SelectRelation) {
            SelectRelation selectRelation = (SelectRelation) queryRelation;
            selectList = selectRelation.getSelectList();
            if (selectRelation.hasWhereClause()) {
                result.put(WHERE_PREDICATE_COLUMN_NAME, selectRelation.getWhereClause());
            }
        }
        if (selectList == null) {
            LOG.warn("parse defineExpr may not correctly for sql [{}] ", originalSql);
            return result;
        }
        for (SelectListItem selectListItem : selectList.getItems()) {
            Expr selectListItemExpr = selectListItem.getExpr();
            List<SlotRef> slots = Lists.newArrayList();
            selectListItemExpr.collect(SlotRef.class, slots);

            String name;
            Expr expr;
            if (selectListItemExpr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItemExpr;
                String functionName = functionCallExpr.getFnName().getFunction();
                switch (functionName.toLowerCase()) {
                    case "sum":
                    case "min":
                    case "max":
                    case FunctionSet.BITMAP_UNION:
                    case FunctionSet.HLL_UNION:
                    case FunctionSet.PERCENTILE_UNION:
                    case FunctionSet.COUNT: {
                        MVColumnItem item = buildAggColumnItem(selectListItem, slots);
                        expr = item.getDefineExpr();
                        name = item.getName();
                        break;
                    }
                    default: {
                        if (functionCallExpr.isAggregateFunction()) {
                            throw new AnalysisException("Unsupported function:" + functionName);
                        }
                        MVColumnItem item = buildNonAggColumnItem(selectListItem, slots);
                        expr = item.getDefineExpr();
                        name = item.getName();
                    }
                }
            } else {
                MVColumnItem item = buildNonAggColumnItem(selectListItem, slots);
                expr = item.getDefineExpr();
                name = item.getName();
            }
            result.put(name, expr);
        }
        return result;
    }

    @Override
    public String toSql() {
        return null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedViewStmt(this, context);
    }

    public void analyze(ConnectContext context) {
        QueryStatement queryStatement = getQueryStatement();

        long originSelectLimit = context.getSessionVariable().getSqlSelectLimit();
        try {
            // ignore limit in creating mv
            context.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
            Analyzer.analyze(queryStatement, context);
        } finally {
            context.getSessionVariable().setSqlSelectLimit(originSelectLimit);
        }

        // forbid explain query
        if (queryStatement.isExplain()) {
            throw new IllegalArgumentException("Creating materialized view does not support explain query");
        }

        if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
            throw new SemanticException("Materialized view query statement only support select");
        }

        Map<TableName, Table> tables = AnalyzerUtils.collectAllTableAndViewWithAlias(queryStatement);
        if (tables.size() != 1) {
            throw new UnsupportedMVException("The materialized view only support one table in from clause.");
        }
        Map.Entry<TableName, Table> entry = tables.entrySet().iterator().next();
        Table table = entry.getValue();

        // SyncMV doesn't support mv's database is different from table's db.
        if (mvTableName != null && !Strings.isNullOrEmpty(mvTableName.getDb()) &&
                !mvTableName.getDb().equalsIgnoreCase(entry.getKey().getDb())) {
            throw new UnsupportedMVException(
                    String.format("Creating materialized view does not support: MV's db %s is different " +
                            "from table's db %s", mvTableName.getDb(), entry.getKey().getDb()));
        }

        if (table instanceof View) {
            // Only in order to make the error message keep compatibility
            throw new SemanticException("Do not support alter non-OLAP table[" + table.getName() + "]");
        } else if (!(table instanceof OlapTable)) {
            throw new UnsupportedMVException("The materialized view only support olap table.");
        }

        TableName tableName = entry.getKey();
        setBaseIndexName(table.getName());
        setDBName(tableName.getDb());

        SelectRelation selectRelation = ((SelectRelation) queryStatement.getQueryRelation());
        if (!(selectRelation.getRelation() instanceof TableRelation)) {
            throw new UnsupportedMVException("Materialized view query statement only support direct query from table.");
        }
        int beginIndexOfAggregation = genColumnAndSetIntoStmt(table, selectRelation);
        if (selectRelation.isDistinct() || selectRelation.hasAggregation()) {
            setMvKeysType(KeysType.AGG_KEYS);
        }
        if (selectRelation.hasWhereClause()) {
            whereClause = selectRelation.getWhereClause();
        }
        if (selectRelation.hasHavingClause()) {
            throw new UnsupportedMVException("The having clause is not supported in add materialized view clause, expr:"
                    + selectRelation.getHavingClause().toSql());
        }
        analyzeOrderByClause(selectRelation, beginIndexOfAggregation);
        if (selectRelation.hasLimit()) {
            throw new UnsupportedMVException("The limit clause is not supported in add materialized view clause, expr:"
                    + " limit " + selectRelation.getLimit());
        }
        final String countPrefix = MATERIALIZED_VIEW_NAME_PREFIX + FunctionSet.COUNT + "_";
        for (MVColumnItem mvColumnItem : getMVColumnItemList()) {
            if (!isReplay && mvColumnItem.isKey() && !mvColumnItem.getType().canBeMVKey()) {
                throw new UnsupportedMVException(
                        String.format("Invalid data type of materialized key column '%s': '%s'",
                                mvColumnItem.getName(), mvColumnItem.getType()));
            }
            if (mvColumnItem.getName().startsWith(countPrefix)
                    && ((OlapTable) table).getKeysType().isAggregationFamily()) {
                throw new UnsupportedMVException(
                        "Aggregate type table do not support count function in materialized view");
            }
        }
    }

    private int genColumnAndSetIntoStmt(Table table, SelectRelation selectRelation) {
        List<MVColumnItem> mvColumnItemList = Lists.newArrayList();

        boolean meetAggregate = false;
        Set<String> mvColumnNameSet = Sets.newHashSet();
        int beginIndexOfAggregation = -1;
        StringJoiner joiner = new StringJoiner(", ", "[", "]");

        List<SelectListItem> selectListItems = selectRelation.getSelectList().getItems();
        // NOTE: To support complex expression in single table sync MV, convert the expression into:
        // - for aggregate function: mv_ + agg_function_name + base_expression
        // - for non-aggregate function: mv_ + base_expression
        // base_expression now uses Expr.toSQL() as its name.
        for (int i = 0; i < selectListItems.size(); ++i) {
            SelectListItem selectListItem = selectListItems.get(i);
            if (selectListItem.isStar()) {
                throw new UnsupportedMVException(
                        "The materialized view currently does not support * in select statement");
            }
            Expr selectListItemExpr = selectListItem.getExpr();
            List<SlotRef> slots = Lists.newArrayList();
            selectListItemExpr.collect(SlotRef.class, slots);
            if (!isReplay) {
                if (slots.size() == 0) {
                    throw new UnsupportedMVException(String.format("The materialized view currently does not support " +
                            "const expr in select " + "statement: {}", selectListItemExpr.toMySql()));
                }
            }
            MVColumnItem mvColumnItem;
            if (selectListItemExpr instanceof FunctionCallExpr
                    && ((FunctionCallExpr) selectListItemExpr).isAggregateFunction()) {
                // Aggregate Function must match pattern.
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItemExpr;
                String functionName = functionCallExpr.getFnName().getFunction();

                MVColumnPattern mvColumnPattern =
                        CreateMaterializedViewStmt.FN_NAME_TO_PATTERN.get(functionName.toLowerCase());
                if (mvColumnPattern == null) {
                    throw new UnsupportedMVException(
                            "Materialized view does not support function:%s, supported functions are: %s",
                            functionCallExpr.toSqlImpl(), FN_NAME_TO_PATTERN.keySet());
                }
                // current version not support count(distinct) function in creating materialized view
                if (!isReplay && functionCallExpr.isDistinct()) {
                    throw new UnsupportedMVException(
                            "Materialized view does not support distinct function " + functionCallExpr.toSqlImpl());
                }
                if (!mvColumnPattern.match(functionCallExpr)) {
                    throw new UnsupportedMVException(
                            "The function " + functionName + " must match pattern:" + mvColumnPattern);
                }
                if (beginIndexOfAggregation == -1) {
                    beginIndexOfAggregation = i;
                }
                meetAggregate = true;

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
            }
        }
        if (beginIndexOfAggregation == 0) {
            throw new UnsupportedMVException("Only %s found in the select list. " +
                    "Please add group by clause and at least one group by column in the select list", joiner);
        }
        setMvColumnItemList(mvColumnItemList);
        return beginIndexOfAggregation;
    }

    // Convert non-aggregate function to MVColumn
    private MVColumnItem buildNonAggColumnItem(SelectListItem selectListItem,
                                               List<SlotRef> baseSlotRefs) throws SemanticException {
        Expr defineExpr = selectListItem.getExpr();
        Type type = defineExpr.getType();
        String columnName;
        if (defineExpr instanceof SlotRef) {
            columnName = ((SlotRef) defineExpr).getColumnName();
        } else {
            if (Strings.isNullOrEmpty(selectListItem.getAlias())) {
                throw new SemanticException("Create materialized view non-slot ref expression should have an alias:" +
                        selectListItem);
            }
            columnName = MVUtils.getMVColumnName(selectListItem.getAlias());
            type = AnalyzerUtils.transformTableColumnType(type, false);
        }
        Set<String> baseColumnNames = baseSlotRefs.stream().map(slot -> slot.getColumnName())
                        .collect(Collectors.toSet());
        return new MVColumnItem(columnName, type, null, false, defineExpr,
                defineExpr.isNullable(),  baseColumnNames);
    }

    // Convert the aggregate function to MVColumn.
    private MVColumnItem buildAggColumnItem(SelectListItem selectListItem,
                                            List<SlotRef> baseSlotRefs) {
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItem.getExpr();
        String functionName = functionCallExpr.getFnName().getFunction();
        Preconditions.checkState(functionCallExpr.getChildren().size() == 1, "Aggregate function only support one child");
        Expr defineExpr = functionCallExpr.getChild(0);
        AggregateType mvAggregateType = null;
        Type baseType = defineExpr.getType();
        Type type;
        String mvColumnName = null;
        if (defineExpr instanceof SlotRef) {
            String baseColumName = baseSlotRefs.get(0).getColumnName();
            if (functionName.equals(FunctionSet.BITMAP_AGG)) {
                mvColumnName = MVUtils.getMVAggColumnName(FunctionSet.BITMAP_UNION, baseColumName);
            } else {
                mvColumnName = MVUtils.getMVAggColumnName(functionName, baseColumName);
            }
        } else {
            if (defineExpr instanceof FunctionCallExpr) {
                FunctionCallExpr argFunc = (FunctionCallExpr) defineExpr;
                String argFuncName = argFunc.getFnName().getFunction();
                switch (argFuncName) {
                    case FunctionSet.TO_BITMAP:
                    case FunctionSet.HLL_HASH:
                    case FunctionSet.PERCENTILE_HASH: {
                        if (argFunc.getChild(0) instanceof SlotRef) {
                            String baseColumName = baseSlotRefs.get(0).getColumnName();
                            mvColumnName = MVUtils.getMVAggColumnName(functionName, baseColumName);
                        }
                        break;
                    }
                    case FunctionSet.BITMAP_AGG:
                        if (argFunc.getChild(0) instanceof SlotRef) {
                            String baseColumName = baseSlotRefs.get(0).getColumnName();
                            mvColumnName = MVUtils.getMVAggColumnName(FunctionSet.BITMAP_UNION, baseColumName);
                        }
                        break;
                    default:
                }
            }
            if (Strings.isNullOrEmpty(mvColumnName)) {
                if (Strings.isNullOrEmpty(selectListItem.getAlias())) {
                    throw new SemanticException("Create materialized view non-slot ref expression should have an alias:" +
                            selectListItem.getExpr());
                }
                mvColumnName = MVUtils.getMVColumnName(selectListItem.getAlias());
            }
        }
        switch (functionName.toLowerCase()) {
            case "sum":
                PrimitiveType argPrimitiveType = baseType.getPrimitiveType();
                if (argPrimitiveType == PrimitiveType.TINYINT || argPrimitiveType == PrimitiveType.SMALLINT
                        || argPrimitiveType == PrimitiveType.INT) {
                    type = Type.BIGINT;
                } else if (argPrimitiveType == PrimitiveType.FLOAT) {
                    type = Type.DOUBLE;
                } else {
                    type = baseType;
                }
                break;
            case "min":
            case "max":
                type = baseType;
                break;
            case FunctionSet.BITMAP_UNION:
                type = Type.BITMAP;
                break;
            case FunctionSet.BITMAP_AGG:
                // Compatible aggregation models
                if (FunctionSet.BITMAP_AGG_TYPE.contains(baseType)) {
                    Function fn = Expr.getBuiltinFunction(FunctionSet.TO_BITMAP, new Type[] {baseType},
                            Function.CompareMode.IS_IDENTICAL);
                    defineExpr = new FunctionCallExpr(FunctionSet.TO_BITMAP, Lists.newArrayList(defineExpr));
                    defineExpr.setFn(fn);
                    defineExpr.setType(Type.BITMAP);
                } else {
                    throw new SemanticException("Unsupported bitmap_agg type:" + baseType);
                }
                functionName = FunctionSet.BITMAP_UNION;
                type = Type.BITMAP;
                break;
            case FunctionSet.HLL_UNION:
                type = Type.HLL;
                break;
            case FunctionSet.PERCENTILE_UNION:
                type = Type.PERCENTILE;
                break;
            case FunctionSet.COUNT:
                mvAggregateType = AggregateType.SUM;
                defineExpr = new CaseExpr(null, Lists.newArrayList(new CaseWhenClause(
                        new IsNullPredicate(defineExpr, false),
                        new IntLiteral(0, Type.BIGINT))), new IntLiteral(1, Type.BIGINT));
                type = Type.BIGINT;
                break;
            default:
                throw new UnsupportedMVException("Unsupported function:" + functionName);
        }
        if (mvAggregateType == null) {
            mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
        }

        // If isReplay, don't check compatibility because materialized view maybe already created before.
        if (!isReplay && !mvAggregateType.checkCompatibility(type)) {
            throw new SemanticException(
                    String.format("Invalid aggregate function '%s' for '%s'", mvAggregateType, type));
        }
        Set<String> baseColumnNames = baseSlotRefs.stream().map(slot -> slot.getColumnName())
                        .collect(Collectors.toSet());
        return new MVColumnItem(mvColumnName, type, mvAggregateType, false,
                defineExpr, functionCallExpr.isNullable(), baseColumnNames);
    }

    private void analyzeOrderByClause(SelectRelation selectRelation,
                                      int beginIndexOfAggregation) {
        if (!selectRelation.hasOrderByClause() ||
                selectRelation.getGroupBy().size() != selectRelation.getOrderBy().size()) {
            supplyOrderColumn();
            return;
        }

        List<OrderByElement> orderByElements = selectRelation.getOrderBy();
        List<MVColumnItem> mvColumnItemList = getMVColumnItemList();

        if (orderByElements.size() > mvColumnItemList.size()) {
            throw new UnsupportedMVException(
                    "The number of columns in order clause must be less then " + "the number of "
                            + "columns in select clause");
        }
        if (beginIndexOfAggregation != -1 && (orderByElements.size() != (beginIndexOfAggregation))) {
            throw new UnsupportedMVException("The order-by columns must be identical to the group-by columns");
        }
        for (int i = 0; i < orderByElements.size(); i++) {
            Expr orderByElement = orderByElements.get(i).getExpr();
            if (!(orderByElement instanceof SlotRef)) {
                throw new UnsupportedMVException(
                        "The column in order clause must be original column without calculation. "
                                + "Error column: " + orderByElement.toSql());
            }
            MVColumnItem mvColumnItem = mvColumnItemList.get(i);
            SlotRef slotRef = (SlotRef) orderByElement;
            if (!mvColumnItem.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                throw new UnsupportedMVException("The order of columns in order by clause must be same as "
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
    private void supplyOrderColumn() {
        List<MVColumnItem> mvColumnItemList = getMVColumnItemList();

        /*
         * The keys type of Materialized view is aggregation.
         * All of group by columns are keys of materialized view.
         */
        if (getMVKeysType() == KeysType.AGG_KEYS) {
            for (MVColumnItem mvColumnItem : getMVColumnItemList()) {
                if (mvColumnItem.getAggregationType() != null) {
                    break;
                }
                if (!mvColumnItem.getType().isScalarType()) {
                    throw new SemanticException("Key column must be scalar type: " + mvColumnItem.getName());
                }
                mvColumnItem.setIsKey(true);
            }
        } else if (getMVKeysType() == KeysType.DUP_KEYS) {
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
                if (theBeginIndexOfValue + 1 > FeConstants.SHORTKEY_MAX_COLUMN_COUNT ||
                        keySizeByte > FeConstants.SHORTKEY_MAXSIZE_BYTES) {
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
