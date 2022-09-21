// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowTableStmt.java

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

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/analysis/ShowTableStmt.java
package com.starrocks.analysis;

import com.google.common.base.Strings;
=======
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
>>>>>>> 46e3aa7ac ([BugFix] Add predicate on database for `SHOW TABLES WHERE` (#11411)):fe/fe-core/src/main/java/com/starrocks/sql/ast/ShowTableStmt.java
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.ScalarType;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;

// SHOW TABLES
public class ShowTableStmt extends ShowStmt {
    private static final String NAME_COL_PREFIX = "Tables_in_";
    private static final String TYPE_COL = "Table_type";
    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "tables");
    private String db;
    private final boolean isVerbose;
    private final String pattern;
    private Expr where;

    public ShowTableStmt(String db, boolean isVerbose, String pattern) {
        this.db = db;
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = null;
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, Expr where) {
        this.db = db;
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = where;
    }

    public String getDb() {
        return db;
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public void analyze(Analyzer analyzer) {
    }

    @Override
    public QueryStatement toSelectStmt() {
        if (where == null) {
            return null;
        }
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_NAME"),
                NAME_COL_PREFIX + ClusterNamespace.getNameFromFullName(db));
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, NAME_COL_PREFIX + ClusterNamespace.getNameFromFullName(db)),
                item.getExpr().clone(null));
        if (isVerbose) {
            item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_TYPE"), TYPE_COL);
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, TYPE_COL), item.getExpr().clone(null));
        }
        where = where.substitute(aliasMap);
        // where databases_name = currentdb
        Expr whereDbEQ = new BinaryPredicate(
                BinaryPredicate.Operator.EQ,
                new SlotRef(TABLE_NAME, "TABLE_SCHEMA"),
                new StringLiteral(db));
        // old where + and + db where
        Expr finalWhere = new CompoundPredicate(
                CompoundPredicate.Operator.AND,
                whereDbEQ,
                where);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                finalWhere, null, null));
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW");
        if (isVerbose) {
            sb.append(" FULL");
        }
        sb.append(" TABLES");
        if (!Strings.isNullOrEmpty(db)) {
            sb.append(" FROM ").append(db);
        }
        if (pattern != null) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(
                new Column(NAME_COL_PREFIX + ClusterNamespace.getNameFromFullName(db), ScalarType.createVarchar(20)));
        if (isVerbose) {
            builder.addColumn(new Column(TYPE_COL, ScalarType.createVarchar(20)));
        }
        return builder.build();
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableStmt(this, context);
    }
}
