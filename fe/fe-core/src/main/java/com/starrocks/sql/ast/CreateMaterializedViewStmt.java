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
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.mvpattern.MVColumnBitmapUnionPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnHLLUnionPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnOneChildPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnPattern;
import com.starrocks.sql.analyzer.mvpattern.MVColumnPercentileUnionPattern;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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

    // If the process is replaying log, isReplay is true, otherwise is false,
    // avoid replay process error report, only in Rollup or MaterializedIndexMeta is true
    private boolean isReplay = false;

    // `Populate` means whether to insert the existed data into the new MV. if false, then no insert the existed
    // data into MV.
    private boolean isPopulate = true;

    //  If `targetTableName` is set, use `targetTableName` instead of `mvName` as the result table.
    private TableName targetTableName;

    public CreateMaterializedViewStmt(String mvName, QueryStatement queryStatement,
                                      Map<String, String> properties, TableName targetTableName) {
        super(NodePosition.ZERO);
        this.mvName = mvName;
        this.queryStatement = queryStatement;
        this.properties = properties;
        this.targetTableName = targetTableName;
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

    public boolean isPopulate() {
        return isPopulate;
    }

    public void setPopulate(boolean populate) {
        isPopulate = populate;
    }

    public TableName getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(TableName targetTableName) {
        this.targetTableName = targetTableName;
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
            String alias = selectListItem.getAlias();
            Expr selectListItemExpr = selectListItem.getExpr();

            List<SlotRef> slots = new ArrayList<>();
            selectListItemExpr.collect(SlotRef.class, slots);

            List<String> baseColumnNames = slots.stream().map(slot -> slot.getColumnName().toLowerCase()).
                    collect(Collectors.toList());

            if (selectListItemExpr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) selectListItemExpr;
                if (!Strings.isNullOrEmpty(alias)) {
                    result.put(alias, selectListItemExpr);
                } else {
                    result.put(slotRef.getColumnName(), null);
                }
            } else if (selectListItemExpr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItemExpr;

                String functionName = functionCallExpr.getFnName().getFunction();
                String mvColumnName = Strings.isNullOrEmpty(alias) ?
                        MVUtils.getMVColumnName(functionName, baseColumnNames) : alias;
                Expr defineExpr = functionCallExpr.getChild(0);
                switch (functionName.toLowerCase()) {
                    case "sum":
                    case "min":
                    case "max":
                    case FunctionSet.BITMAP_UNION:
                    case FunctionSet.HLL_UNION:
                    case FunctionSet.PERCENTILE_UNION:
                        break;
                    case FunctionSet.COUNT:
                        defineExpr = new CaseExpr(null, Lists.newArrayList(
                                new CaseWhenClause(new IsNullPredicate(slots.get(0), false),
                                        new IntLiteral(0, Type.BIGINT))), new IntLiteral(1, Type.BIGINT));
                        break;
                    default:
                        if (functionCallExpr.isAggregateFunction()) {
                            throw new AnalysisException("Unsupported function:" + functionName);
                        }
                }
                result.put(mvColumnName, defineExpr);
            } else {
                // Other operator, like arithmetic operator
                String mvColumnName = alias != null ? alias : MVUtils.getMVColumnName(selectListItemExpr.debugString(),
                        baseColumnNames);
                result.put(mvColumnName, selectListItemExpr);
            }
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

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new SyncMVAnalyzerVisitor().visit(stmt, session);
    }

    public static class SyncMVAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
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

            Analyzer.analyze(queryStatement, context);

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

    /// FIXME Need check alias is not same as column names in base schema?
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

            String alias = selectListItem.getAlias();
            Expr selectListItemExpr = selectListItem.getExpr();

            if (selectListItemExpr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) selectListItemExpr;
                String columnName = alias != null ? alias : slotRef.getColumnName().toLowerCase();
                Expr defineExpr = alias != null ? selectListItemExpr : null;
                joiner.add(columnName);
                if (meetAggregate) {
                    throw new SemanticException("Any single column should be before agg column. " +
                            "Column %s at wrong location", columnName);
                }
                // check duplicate column
                if (!mvColumnNameSet.add(columnName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnName);
                }
                MVColumnItem mvColumnItem = new MVColumnItem(columnName, slotRef.getType(), defineExpr);
                mvColumnItemList.add(mvColumnItem);
            } else if (selectListItemExpr instanceof FunctionCallExpr) {
                // Aggregate Function must match pattern.
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItemExpr;
                String functionName = functionCallExpr.getFnName().getFunction();
                MVColumnPattern mvColumnPattern =
                        CreateMaterializedViewStmt.FN_NAME_TO_PATTERN.get(functionName.toLowerCase());
                if (mvColumnPattern == null) {
                    if (functionCallExpr.isAggregateFunction()) {
                        throw new SemanticException(
                                "Materialized view does not support this function:%s, supported functions are: %s",
                                functionCallExpr.toSqlImpl(), FN_NAME_TO_PATTERN.keySet());
                    } else {
                        /// Normal function
                        MVColumnItem mvColumnItem = buildMVColumnItem(alias, functionCallExpr, statement.isReplay(), true);
                        if (!mvColumnNameSet.add(mvColumnItem.getName())) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, mvColumnItem.getName());
                        }
                        mvColumnItemList.add(mvColumnItem);
                    }
                } else {
                    // current version not support count(distinct) function in creating materialized view
                    if (!statement.isReplay() && functionCallExpr.isDistinct()) {
                        throw new SemanticException(
                                "Materialized view does not support distinct function " + functionCallExpr.toSqlImpl());
                    }

                    if (beginIndexOfAggregation == -1) {
                        beginIndexOfAggregation = i;
                    }
                    meetAggregate = true;
                    MVColumnItem mvColumnItem = buildMVColumnItem(alias, functionCallExpr, statement.isReplay(), false);
                    if (!mvColumnNameSet.add(mvColumnItem.getName())) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, mvColumnItem.getName());
                    }
                    mvColumnItemList.add(mvColumnItem);

                }
                joiner.add(functionCallExpr.toSqlImpl());
            } else {
                /// Other operator
                List<SlotRef> slots = new ArrayList<>();
                selectListItemExpr.collect(SlotRef.class, slots);
                Type type = selectListItemExpr.getType();
                List<String> baseColumnNames = slots.stream().map(slot -> slot.getColumnName().toLowerCase()).
                        collect(Collectors.toList());

                String columnName = alias != null ? alias : MVUtils.getMVColumnName(selectListItemExpr.debugString(),
                        baseColumnNames);
                MVColumnItem mvColumnItem = new MVColumnItem(columnName, type, selectListItemExpr, baseColumnNames);

                if (!mvColumnNameSet.add(columnName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, mvColumnItem.getName());
                }
                mvColumnItemList.add(mvColumnItem);
                joiner.add(selectListItemExpr.toSql());
            }
        }
        if (beginIndexOfAggregation == 0) {
            throw new SemanticException("Only %s found in the select list. " +
                    "Please add group by clause and at least one group by column in the select list", joiner);
        }
        statement.setMvColumnItemList(mvColumnItemList);
        return beginIndexOfAggregation;
    }

    private static MVColumnItem buildMVColumnItem(String aliasName,
                                                  FunctionCallExpr functionCallExpr,
                                                  boolean isReplay, boolean isNormalFunction) {
        String functionName = functionCallExpr.getFnName().getFunction();
        List<SlotRef> slots = new ArrayList<>();
        functionCallExpr.collect(SlotRef.class, slots);
        SlotRef baseColumnRef = slots.get(0);
        List<String> baseColumnNames = slots.stream().map(slot -> slot.getColumnName().toLowerCase()).
                collect(Collectors.toList());
        Expr functionChild0 = functionCallExpr.getChild(0);
        AggregateType mvAggregateType;
        Type funcArgType = functionChild0.getType();
        String mvColumnName = Strings.isNullOrEmpty(aliasName) ? MVUtils.getMVColumnName(functionName, baseColumnNames)
                : aliasName;
        Expr defineExpr = functionChild0;
        Type type;
        if (isNormalFunction) {
            type = functionCallExpr.getType();
            defineExpr = functionCallExpr;
            return new MVColumnItem(mvColumnName, type, defineExpr, baseColumnNames);

        } else {
            switch (functionName.toLowerCase()) {
                case "sum":
                    mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                    PrimitiveType argPrimitiveType = funcArgType.getPrimitiveType();
                    if (argPrimitiveType == PrimitiveType.TINYINT || argPrimitiveType == PrimitiveType.SMALLINT
                            || argPrimitiveType == PrimitiveType.INT) {
                        type = Type.BIGINT;
                    } else if (argPrimitiveType == PrimitiveType.FLOAT) {
                        type = Type.DOUBLE;
                    } else {
                        type = funcArgType;
                    }
                    break;
                case "min":
                case "max":
                    mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                    type = funcArgType;
                    break;
                case FunctionSet.BITMAP_UNION:
                    mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                    type = Type.BITMAP;
                    break;
                case FunctionSet.HLL_UNION:
                    mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                    type = Type.HLL;
                    break;
                case FunctionSet.PERCENTILE_UNION:
                    mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                    type = Type.PERCENTILE;
                    break;
                case FunctionSet.COUNT:
                    mvAggregateType = AggregateType.SUM;
                    defineExpr = new CaseExpr(null, Lists.newArrayList(new CaseWhenClause(
                            new IsNullPredicate(baseColumnRef, false),
                            new IntLiteral(0, Type.BIGINT))), new IntLiteral(1, Type.BIGINT));
                    type = Type.BIGINT;
                    break;
                default:
                    throw new SemanticException("Unsupported function:" + functionName);
            }
        }

        // If isReplay, don't check compatibility because materialized view maybe already created before.
        if (!isReplay && !mvAggregateType.checkCompatibility(type)) {
            throw new SemanticException(
                    String.format("Invalid aggregate function '%s' for '%s'", mvAggregateType, type));
        }
        return new MVColumnItem(mvColumnName, type, mvAggregateType, functionCallExpr.isNullable(),
                false, defineExpr, baseColumnNames);
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
                Preconditions.checkArgument(mvColumnItem.getType().isScalarType(), "non scalar type");
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
