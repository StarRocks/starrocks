// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowVariablesStmt.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Show variables statement.
public class ShowVariablesStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowVariablesStmt.class);
    private static final String NAME_COL = "Variable_name";
    private static final String VALUE_COL = "Value";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(NAME_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(VALUE_COL, ScalarType.createVarchar(20)))
                    .build();

    private SetType type;
    private final String pattern;
    private Expr where;

    public ShowVariablesStmt(SetType type, String pattern) {
        this.type = type;
        this.pattern = pattern;
    }

    public ShowVariablesStmt(SetType type, String pattern, Expr where) {
        this.type = type;
        this.pattern = pattern;
        this.where = where;
    }

    public SetType getType() {
        return type;
    }

    public void setType(SetType type) {
        this.type = type;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public void analyze(Analyzer analyzer) {
        if (type == null) {
            type = SetType.DEFAULT;
        }
    }

    @Override
    public QueryStatement toSelectStmt() {
        if (where == null) {
            return null;
        }
        if (type == null) {
            type = SetType.DEFAULT;
        }
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        TableName tableName;
        if (type == SetType.GLOBAL) {
            tableName = new TableName(InfoSchemaDb.DATABASE_NAME, "GLOBAL_VARIABLES");
        } else {
            tableName = new TableName(InfoSchemaDb.DATABASE_NAME, "SESSION_VARIABLES");
        }
        // name
        SelectListItem item = new SelectListItem(new SlotRef(tableName, "VARIABLE_NAME"), NAME_COL);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, NAME_COL), item.getExpr().clone(null));
        // value
        item = new SelectListItem(new SlotRef(tableName, "VARIABLE_VALUE"), VALUE_COL);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, VALUE_COL), item.getExpr().clone(null));
        // change
        where = where.substitute(aliasMap);

        return new QueryStatement(new SelectRelation(selectList, new TableRelation(tableName),
                where, null, null));
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW ");
        sb.append(type.toString()).append(" VARIABLES");
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
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowVariablesStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
