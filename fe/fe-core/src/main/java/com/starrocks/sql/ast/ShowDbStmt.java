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

import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;

import static com.starrocks.common.util.Util.normalizeName;

// Show database statement.
public class ShowDbStmt extends EnhancedShowStmt {
    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "schemata");
    private static final String DB_COL = "Database";
    private final String pattern;
    private Expr where;

    private String catalogName;

    public ShowDbStmt(String pattern) {
        this(pattern, null, null, NodePosition.ZERO);
    }

    public ShowDbStmt(String pattern, Expr where) {
        this(pattern, where, null, NodePosition.ZERO);
    }

    public ShowDbStmt(String pattern, String catalogName) {
        this(pattern, null, catalogName, NodePosition.ZERO);
    }

    public ShowDbStmt(String pattern, Expr where, String catalogName) {
        this(pattern, where, catalogName, NodePosition.ZERO);
    }

    public ShowDbStmt(String pattern, Expr where, String catalogName, NodePosition pos) {
        super(pos);
        this.pattern = pattern;
        this.where = where;
        this.catalogName = normalizeName(catalogName);
    }

    public String getPattern() {
        return pattern;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public QueryStatement toSelectStmt() {
        if (where == null) {
            return null;
        }
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "SCHEMA_NAME"), DB_COL);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, DB_COL), item.getExpr().clone(null));
        where = where.substitute(aliasMap);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                where, null, null), this.origStmt);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowDatabasesStatement(this, context);
    }
}
