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
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;

import static com.starrocks.common.util.Util.normalizeName;

// SHOW TABLES
public class ShowTableStmt extends ShowStmt {
    private static final String NAME_COL_PREFIX = "Tables_in_";
    private static final String TYPE_COL = "Table_type";
    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "tables");
    private String db;
    private final boolean isVerbose;
    private final String pattern;
    private Expr where;
    private String catalogName;

    public ShowTableStmt(String db, boolean isVerbose, String pattern) {
        this(db, isVerbose, pattern, null, null, NodePosition.ZERO);
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, String catalogName) {
        this(db, isVerbose, pattern, null, catalogName, NodePosition.ZERO);
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, Expr where, String catalogName) {
        this(db, isVerbose, pattern, where, catalogName, NodePosition.ZERO);
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, Expr where,
                         String catalogName, NodePosition pos) {
        super(pos);
        this.db = normalizeName(db);
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = where;
        this.catalogName = normalizeName(catalogName);
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
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_NAME"),
                NAME_COL_PREFIX + db);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, NAME_COL_PREFIX + db),
                item.getExpr().clone(null));
        if (isVerbose) {
            item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_TYPE"), TYPE_COL);
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, TYPE_COL), item.getExpr().clone(null));
        }
        where = where.substitute(aliasMap);
        // where databases_name = currentdb
        Expr whereDbEQ = new BinaryPredicate(
                BinaryType.EQ,
                new SlotRef(TABLE_NAME, "TABLE_SCHEMA"),
                new StringLiteral(db));
        // old where + and + db where
        Expr finalWhere = new CompoundPredicate(
                CompoundPredicate.Operator.AND,
                whereDbEQ,
                where);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                finalWhere, null, null), this.origStmt);
    }

    public void setDb(String db) {
        this.db = normalizeName(db);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowTableStatement(this, context);
    }
}
