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


package com.starrocks.sql.common;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Help use ids to replace names in sql, it's useful for save statement into catalog
 */
public class SqlWithIdUtils {

    private static final String TABLE_ID_PREFIX = "<table ";

    private static final String DATABASE_ID_PREFIX = "<db ";

    private static final String COMMON_SUFFIX = ">";

    public static String encode(StatementBase statement, ConnectContext context) {
        Map<TableName, Table> tableMap = AnalyzerUtils.collectAllTable(statement);
        Map<String, Database> databaseMap = AnalyzerUtils.collectAllDatabase(context, statement);
        return new SqlEncoderVisitor(databaseMap, tableMap).visit(statement);
    }

    /**
     * decode sql which use database id and table id
     * warning: table id must before db id e.g. <db 10001>.<table 10002>
     *
     * @param sql
     * @return StatementBase
     */
    public static StatementBase decode(String sql, ConnectContext context) {
        Map<Long, Database> dbMap = Maps.newHashMap();
        HashMap<Long, Table> tableMap = Maps.newHashMap();
        String[] dbStrs = sql.split(DATABASE_ID_PREFIX);
        for (String dbStr : dbStrs) {
            if (dbStr.indexOf(COMMON_SUFFIX) < 4) {
                continue;
            }
            long dbId = Long.parseLong(dbStr.substring(0, dbStr.indexOf(COMMON_SUFFIX)));
            Database db = dbMap.get(dbId);
            if (db == null) {
                db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (db == null) {
                    throw new SemanticException("Can not find db id: %s", dbId);
                }
                dbMap.put(dbId, db);
            }
            String tableStr = dbStr.substring(dbStr.indexOf(COMMON_SUFFIX) + 2);
            long tableId = Long.parseLong(tableStr.substring(7, tableStr.indexOf(COMMON_SUFFIX)));
            Table table = tableMap.get(tableId);
            if (table == null) {
                table = db.getTable(tableId);
                if (table == null) {
                    throw new SemanticException("Can not find table id: %s in db: %s", tableId, db.getOriginName());
                }
                tableMap.put(tableId, table);
            }
        }
        for (Database db : dbMap.values()) {
            String fullName = db.getFullName();
            // because SqlParser can't parser cluster
            String dbName = fullName.indexOf(":") > 0 ? fullName.substring(fullName.indexOf(":") + 1) : fullName;
            sql = sql.replaceAll(DATABASE_ID_PREFIX + db.getId() + COMMON_SUFFIX, dbName);
        }
        for (Table table : tableMap.values()) {
            sql = sql.replaceAll(TABLE_ID_PREFIX + table.getId() + COMMON_SUFFIX, table.getName());
        }
        return SqlParser.parse(sql, context.getSessionVariable()).get(0);
    }

    private static class SqlEncoderVisitor extends AstToStringBuilder.AST2StringBuilderVisitor {

        private final Map<TableName, Table> tableMap;
        Map<String, Database> databaseMap;

        public SqlEncoderVisitor(Map<String, Database> databaseMap, Map<TableName, Table> tableMap) {
            super();
            this.databaseMap = databaseMap;
            this.tableMap = tableMap;
        }

        @Override
        public String visitNode(ParseNode node, Void context) {
            return "";
        }

        @Override
        public String visitSelect(SelectRelation stmt, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            SelectList selectList = stmt.getSelectList();
            sqlBuilder.append("SELECT ");
            if (selectList.isDistinct()) {
                sqlBuilder.append("DISTINCT ");
            }
            List<String> selectListString = new ArrayList<>();
            for (int i = 0; i < stmt.getOutputExpression().size(); ++i) {
                Expr expr = stmt.getOutputExpression().get(i);
                String columnName = stmt.getScope().getRelationFields().getFieldByIndex(i).getName();

                if (expr instanceof FieldReference) {
                    Field field = stmt.getScope().getRelationFields().getFieldByIndex(i);
                    TableName tableName = field.getRelationAlias();
                    selectListString.add(
                            (tableName == null ? "" : getTableId(tableName) + ".")
                                    + "`" + field.getName() + "`" + " AS `" + columnName + "`");
                } else {
                    selectListString.add(visit(expr) + " AS `" + columnName + "`");
                }
            }

            sqlBuilder.append(Joiner.on(", ").join(selectListString));

            String fromClause = visit(stmt.getRelation());
            if (fromClause != null) {
                sqlBuilder.append(" FROM ");
                sqlBuilder.append(fromClause);
            }

            if (stmt.hasWhereClause()) {
                sqlBuilder.append(" WHERE ");
                sqlBuilder.append(visit(stmt.getWhereClause()));
            }

            if (stmt.hasGroupByClause()) {
                sqlBuilder.append(" GROUP BY ");
                sqlBuilder.append(visit(stmt.getGroupByClause()));
            }

            if (stmt.hasHavingClause()) {
                sqlBuilder.append(" HAVING ");
                sqlBuilder.append(visit(stmt.getHavingClause()));
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitCTE(CTERelation relation, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(relation.getName());

            if (relation.isResolvedInFromClause()) {
                if (relation.getAlias() != null) {
                    sqlBuilder.append(" AS ").append(relation.getAlias().getTbl());
                }
                return sqlBuilder.toString();
            }

            if (relation.getColumnOutputNames() != null) {
                sqlBuilder.append("(")
                        .append(Joiner.on(", ").join(
                                relation.getColumnOutputNames().stream().map(c -> "`" + c + "`").collect(toList())))
                        .append(")");
            }
            sqlBuilder.append(" AS (").append(visit(relation.getCteQueryStatement())).append(") ");
            return sqlBuilder.toString();
        }

        @Override
        public String visitView(ViewRelation node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(getTableId(node.getName()));
            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append("`").append(node.getAlias().getTbl()).append("`");
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitTable(TableRelation node, Void outerScope) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(getTableId(node.getName()));
            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append("`").append(node.getAlias().getTbl()).append("`");
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitExpression(Expr expr, Void context) {
            return expr.toSql();
        }

        @Override
        public String visitSlot(SlotRef expr, Void context) {
            return getTableId(expr.getTblNameWithoutAnalyzed()) + "." + "`" + expr.getColumnName() + "`";
        }

        private String getTableId(TableName tableName) {
            StringBuilder stringBuilder = new StringBuilder();
            if (tableName.getDb() != null) {
                Database database = databaseMap.get(tableName.getDb());
                if (database != null) {
                    stringBuilder.append(DATABASE_ID_PREFIX)
                            .append(database.getId())
                            .append(COMMON_SUFFIX + ".");
                } else {
                    stringBuilder.append("`" + tableName.getDb() + "`" + ".");
                }
            }
            Table table = tableMap.get(tableName);
            if (table != null) {
                stringBuilder.append(TABLE_ID_PREFIX)
                        .append(table.getId())
                        .append(COMMON_SUFFIX);
            } else {
                stringBuilder.append("`" + tableName.getTbl() + "`");
            }
            return stringBuilder.toString();
        }
    }
}
