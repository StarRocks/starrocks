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

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

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
public class CreateSyncMVStmt extends DdlStmt {

    private TableRef mvTableRef;
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

    public CreateSyncMVStmt(TableRef mvTableRef, QueryStatement queryStatement, Map<String, String> properties) {
        super(NodePosition.ZERO);
        this.mvTableRef = mvTableRef;
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

    public TableRef getMvTableRef() {
        return mvTableRef;
    }

    public void setMvTableRef(TableRef mvTableRef) {
        this.mvTableRef = mvTableRef;
    }

    public String getMVName() {
        return mvTableRef == null ? null : mvTableRef.getTableName();
    }

    public String getCatalogName() {
        return mvTableRef == null ? null : mvTableRef.getCatalogName();
    }

    public String getDbName() {
        return mvTableRef == null ? null : mvTableRef.getDbName();
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
        if (dbName != null) {
            return dbName;
        }
        return mvTableRef == null ? null : mvTableRef.getDbName();
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

    public void setWhereClause(Expr whereClause) {
        this.whereClause = whereClause;
    }

    @Override
    public String toSql() {
        return null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCreateSyncMVStmt(this, context);
    }
}
