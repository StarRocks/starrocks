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
import com.google.common.base.Strings;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CollectionElementExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Pair;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.MapExpr;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.storagevolume.StorageVolume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
=======
import java.util.Set;
import java.util.StringJoiner;
>>>>>>> 7c12370545 ([BugFix] remove backquote of field name in struct (#39753))
import java.util.stream.Collectors;

import static com.starrocks.catalog.FunctionSet.IGNORE_NULL_WINDOW_FUNCTION;
import static java.util.stream.Collectors.toList;

/**
 * `AstToStringBuilder` is mainly used to convert tree-structured AST to string type.
 * Implements all required AST conversion logic, and is the base class for other similar classes.
 * The string generated by `AstToStringBuilder` is not guaranteed to be a sql statement that
 * can be parsed into ast, nor is it necessarily legal sql.
 * If the generated string needs to be legal sql, please use `AstToSQLBuilder`.
 */
public class AstToStringBuilder {
    public static String toString(ParseNode expr) {
        return new AST2StringBuilderVisitor().visit(expr);
    }

    public static String getAliasName(ParseNode expr, boolean addFunctionDbName, boolean withBackquote) {
        return new AST2StringBuilderVisitor(addFunctionDbName, withBackquote).visit(expr);
    }

    public static class AST2StringBuilderVisitor extends AstVisitor<String, Void> {

        // when you want to get the full string of a functionCallExpr set it true
        // when you just want to a function name as its alias set it false
        protected boolean addFunctionDbName;

        // when you want to get an expr name with backquote set it true
        // when you just want to get the real expr name set it false
        protected boolean withBackquote;

        public AST2StringBuilderVisitor() {
            this(true, true);
        }

        public AST2StringBuilderVisitor(boolean addFunctionDbName, boolean withBackquote) {
            this.addFunctionDbName = addFunctionDbName;
            this.withBackquote = withBackquote;
        }

        // ------------------------------------------- Privilege Statement -------------------------------------------------

        @Override
        public String visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt statement, Void context) {
            StringBuilder sb = new StringBuilder();
            if (statement instanceof CreateUserStmt) {
                sb.append("CREATE");
            } else {
                sb.append("ALTER");
            }

            sb.append(" USER ").append(statement.getUserIdentity());
            if (!Strings.isNullOrEmpty(statement.getOriginalPassword())) {
                if (statement.isPasswordPlain()) {
                    sb.append(" IDENTIFIED BY '").append("*XXX").append("'");
                } else {
                    sb.append(" IDENTIFIED BY PASSWORD '").append(statement.getOriginalPassword()).append("'");
                }
            }

            if (!Strings.isNullOrEmpty(statement.getAuthPluginName())) {
                sb.append(" IDENTIFIED WITH ").append(statement.getAuthPluginName());
                if (!Strings.isNullOrEmpty(statement.getAuthStringUnResolved())) {
                    if (statement.isPasswordPlain()) {
                        sb.append(" BY '");
                    } else {
                        sb.append(" AS '");
                    }
                    sb.append(statement.getAuthStringUnResolved()).append("'");
                }
            }

            if (!statement.getDefaultRoles().isEmpty()) {
                sb.append(" DEFAULT ROLE ");
                sb.append(Joiner.on(",").join(
                        statement.getDefaultRoles().stream().map(r -> "'" + r + "'").collect(toList())));
            }

            return sb.toString();
        }

        @Override
        public String visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();
            if (stmt instanceof GrantPrivilegeStmt) {
                sb.append("GRANT ");
            } else {
                sb.append("REVOKE ");
            }
            List<String> privList = new ArrayList<>();
            for (PrivilegeType privilegeType : stmt.getPrivilegeTypes()) {
                privList.add(privilegeType.name().replace("_", " "));
            }

            sb.append(Joiner.on(", ").join(privList));
            sb.append(" ON ");

            if (stmt.getObjectType().equals(ObjectType.SYSTEM)) {
                sb.append(stmt.getObjectType().name());
            } else {
                if (stmt.getObjectList().stream().anyMatch(PEntryObject::isFuzzyMatching)) {
                    sb.append(stmt.getObjectList().get(0).toString());
                } else {
                    sb.append(stmt.getObjectType().name()).append(" ");

                    List<String> objectString = new ArrayList<>();
                    for (PEntryObject tablePEntryObject : stmt.getObjectList()) {
                        objectString.add(tablePEntryObject.toString());
                    }
                    sb.append(Joiner.on(", ").join(objectString));
                }
            }
            if (stmt instanceof GrantPrivilegeStmt) {
                sb.append(" TO ");
            } else {
                sb.append(" FROM ");
            }
            if (stmt.getUserIdentity() != null) {
                sb.append("USER ").append(stmt.getUserIdentity());
            } else {
                sb.append("ROLE '").append(stmt.getRole()).append("'");
            }

            if (stmt instanceof GrantPrivilegeStmt && ((GrantPrivilegeStmt) stmt).isWithGrantOption()) {
                sb.append(" WITH GRANT OPTION");
            }
            return sb.toString();
        }

        @Override
        public String visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            if (statement instanceof GrantRoleStmt) {
                sqlBuilder.append("GRANT ");
            } else {
                sqlBuilder.append("REVOKE ");
            }

            sqlBuilder.append(Joiner.on(", ")
                    .join(statement.getGranteeRole().stream().map(r -> "'" + r + "'").collect(toList())));
            sqlBuilder.append(" ");
            if (statement instanceof GrantRoleStmt) {
                sqlBuilder.append("TO ");
            } else {
                sqlBuilder.append("FROM ");
            }
            if (statement.getRole() != null) {
                sqlBuilder.append(" ROLE ").append(statement.getRole());
            } else {
                sqlBuilder.append(statement.getUserIdentity());
            }

            return sqlBuilder.toString();
        }

        // --------------------------------------------Set Statement -------------------------------------------------------

        @Override
        public String visitSetStatement(SetStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append("SET ");

            List<String> setVarList = new ArrayList<>();
            for (SetListItem setVar : stmt.getSetListItems()) {
                if (setVar instanceof SystemVariable) {
                    SystemVariable systemVariable = (SystemVariable) setVar;
                    String setVarSql = "";
                    setVarSql += systemVariable.getType().toString() + " ";
                    setVarSql += "`" + systemVariable.getVariable() + "`";
                    setVarSql += " = ";
                    setVarSql += visit(systemVariable.getResolvedExpression());

                    setVarList.add(setVarSql);
                } else if (setVar instanceof UserVariable) {
                    UserVariable userVariable = (UserVariable) setVar;
                    String setVarSql = "";
                    setVarSql += "@";
                    setVarSql += "`" + userVariable.getVariable() + "`";
                    setVarSql += " = ";

                    setVarSql += "cast (" + visit(userVariable.getEvaluatedExpression())
                            + " as " + userVariable.getEvaluatedExpression().getType().toSql() + ")";
                    setVarList.add(setVarSql);
                } else if (setVar instanceof SetPassVar) {
                    SetPassVar setPassVar = (SetPassVar) setVar;
                    UserIdentity userIdentity = setPassVar.getUserIdent();
                    String setPassSql = "";
                    if (userIdentity == null) {
                        setPassSql += "PASSWORD";
                    } else {
                        setPassSql += "PASSWORD FOR " + userIdentity;
                    }
                    setPassSql += " = PASSWORD('***')";
                    setVarList.add(setPassSql);
                }
            }

            return sb.append(Joiner.on(",").join(setVarList)).toString();
        }

        @Override
        public String visitSetUserPropertyStatement(SetUserPropertyStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append("SET PROPERTY FOR ").append('\'').append(stmt.getUser()).append('\'');
            int idx = 0;
            for (Pair<String, String> stringStringPair : stmt.getPropertyPairList()) {
                if (idx != 0) {
                    sb.append(", ");
                }
                sb.append(stringStringPair.first).append(" = ").append(stringStringPair.second);
                idx++;
            }
            return sb.toString();
        }

        public String visitCreateResourceStatement(CreateResourceStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE EXTERNAL RESOURCE ").append(stmt.getResourceName());

            sb.append(" PROPERTIES (");
            sb.append(new PrintableMap<String, String>(stmt.getProperties(), "=", true, false, true));
            sb.append(")");
            return sb.toString();
        }

        @Override
        public String visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();
            if (stmt.isExplain()) {
                sb.append("EXPLAIN ");
            }

            sb.append("DROP MATERIALIZED VIEW ");
            if (stmt.isSetIfExists()) {
                sb.append("IF EXISTS ");
            }

            sb.append(stmt.getMvName());
            return sb.toString();
        }

        @Override
        public String visitCreateRoutineLoadStatement(CreateRoutineLoadStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();
            String dbName = null;
            String jobName = null;
            if (stmt.getLabelName() != null) {
                dbName = stmt.getLabelName().getDbName();
                jobName = stmt.getLabelName().getLabelName();
            }
            if (dbName != null) {
                sb.append("CREATE ROUTINE LOAD ").append(dbName).append(".")
                    .append(jobName).append(" ON ").append(stmt.getTableName());
            } else {
                sb.append("CREATE ROUTINE LOAD ").append(jobName).append(" ON ").append(stmt.getTableName());
            }

            if (stmt.getRoutineLoadDesc() != null) {
                sb.append(" ").append(stmt.getRoutineLoadDesc()).append(" ");
            }

            if (!stmt.getJobProperties().isEmpty()) {
                PrintableMap<String, String> map = new PrintableMap<>(stmt.getJobProperties(), "=", true, false);
                sb.append("PROPERTIES ( ").append(map).append(" )");
            }

            sb.append(" FROM ").append(stmt.getTypeName()).append(" ");

            if (!stmt.getDataSourceProperties().isEmpty()) {
                PrintableMap<String, String> map = new PrintableMap<>(stmt.getDataSourceProperties(), "=", true, false, true);
                sb.append("( ").append(map).append(" )");
            }

            return sb.toString();
        }

        @Override
        public String visitLoadStatement(LoadStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();

            sb.append("LOAD LABEL ").append(stmt.getLabel().toString());
            sb.append(" (");
            sb.append(Joiner.on(",").join(
                    stmt.getDataDescriptions().stream().map(DataDescription::toString).collect(toList())));
            sb.append(")");

            if (stmt.getBrokerDesc() != null) {
                sb.append(stmt.getBrokerDesc());
            }

            if (stmt.getCluster() != null) {
                sb.append(" BY '");
                sb.append(stmt.getCluster());
                sb.append("'");
            }
            if (stmt.getResourceDesc() != null) {
                sb.append(stmt.getResourceDesc());
            }

            if (stmt.getProperties() != null && !stmt.getProperties().isEmpty()) {
                sb.append(" PROPERTIES (");
                sb.append(new PrintableMap<>(stmt.getProperties(), "=", true, false));
                sb.append(")");
            }
            return sb.toString();
        }

        @Override
        public String visitExportStatement(ExportStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();

            sb.append("EXPORT TABLE ");
            if (stmt.getTblName() == null) {
                sb.append("non-exist");
            } else {
                sb.append(stmt.getTblName().toSql());
            }

            if (stmt.getPartitions() != null && !stmt.getPartitions().isEmpty()) {
                sb.append(" PARTITION (");
                Joiner.on(",").appendTo(sb, stmt.getPartitions()).append(")");
            }

            if (stmt.getColumnNames() != null && !stmt.getColumnNames().isEmpty()) {
                sb.append("(");
                Joiner.on(",").appendTo(sb, stmt.getColumnNames()).append(")");
            }
            sb.append(" TO ");
            sb.append("\"" + stmt.getPath() + "\" ");
            if (stmt.getProperties() != null && !stmt.getProperties().isEmpty()) {
                sb.append("PROPERTIES (");
                sb.append(new PrintableMap<String, String>(stmt.getProperties(), "=", true, false));
                sb.append(")");
            }
            sb.append("WITH BROKER ");
            if (stmt.getBrokerDesc() != null) {
                if (!stmt.getBrokerDesc().getName().isEmpty()) {
                    sb.append(stmt.getBrokerDesc().getName());
                }
                sb.append("' (");
                sb.append(new PrintableMap<String, String>(stmt.getBrokerDesc().getProperties(), "=", true, false, true));
                sb.append(")");
            }
            return sb.toString();
        }

        @Override
        public String visitQueryStatement(QueryStatement stmt, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            QueryRelation queryRelation = stmt.getQueryRelation();

            if (queryRelation.hasWithClause()) {
                sqlBuilder.append("WITH ");
                List<String> cteStrings =
                        queryRelation.getCteRelations().stream().map(this::visit).collect(Collectors.toList());
                sqlBuilder.append(Joiner.on(", ").join(cteStrings));
            }

            sqlBuilder.append(visit(queryRelation));

            if (queryRelation.hasOrderByClause()) {
                List<OrderByElement> sortClause = queryRelation.getOrderBy();
                sqlBuilder.append(" ORDER BY ").append(visitAstList(sortClause)).append(" ");
            }

            // Limit clause.
            if (queryRelation.getLimit() != null) {
                sqlBuilder.append(visit(queryRelation.getLimit()));
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
                        .append(Joiner.on(", ").join(relation.getColumnOutputNames())).append(")");
            }
            sqlBuilder.append(" AS (").append(visit(relation.getCteQueryStatement())).append(") ");
            return sqlBuilder.toString();
        }

        @Override
        public String visitSelect(SelectRelation stmt, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            SelectList selectList = stmt.getSelectList();
            sqlBuilder.append("SELECT ");
            if (selectList.isDistinct()) {
                sqlBuilder.append("DISTINCT ");
            }

            for (int i = 0; i < selectList.getItems().size(); ++i) {
                if (i != 0) {
                    sqlBuilder.append(", ");
                }

                String selectItemLabel;
                SelectListItem item = selectList.getItems().get(i);
                if (!item.isStar()) {
                    String aliasSql = null;
                    if (item.getAlias() != null) {
                        aliasSql = "AS " + item.getAlias();
                    }
                    selectItemLabel = visit(item.getExpr()) + ((aliasSql == null) ? "" : " " + aliasSql);
                } else if (item.getTblName() != null) {
                    selectItemLabel = item.getTblName().toString() + ".*";
                } else {
                    selectItemLabel = "*";
                }

                sqlBuilder.append(selectItemLabel);
            }

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
        public String visitSubquery(SubqueryRelation node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder("(" + visit(node.getQueryStatement()) + ")");

            if (node.getAlias() != null) {
                sqlBuilder.append(" ").append(node.getAlias().getTbl());

                if (node.getExplicitColumnNames() != null) {
                    sqlBuilder.append("(");
                    sqlBuilder.append(Joiner.on(",").join(node.getExplicitColumnNames()));
                    sqlBuilder.append(")");
                }
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitView(ViewRelation node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(node.getName().toSql());

            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append(node.getAlias().getTbl());
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitJoin(JoinRelation relation, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(visit(relation.getLeft())).append(" ");
            if (relation.isImplicit()) {
                sqlBuilder.append(",");
            } else {
                sqlBuilder.append(relation.getJoinOp());
            }
            if (relation.getJoinHint() != null && !relation.getJoinHint().isEmpty()) {
                sqlBuilder.append(" [").append(relation.getJoinHint()).append("]");
            }
            sqlBuilder.append(" ");
            if (relation.isLateral()) {
                sqlBuilder.append("LATERAL ");
            }
            sqlBuilder.append(visit(relation.getRight())).append(" ");

            if (relation.getOnPredicate() != null) {
                sqlBuilder.append("ON ").append(visit(relation.getOnPredicate()));
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitUnion(UnionRelation relation, Void context) {
            return processSetOp(relation);
        }

        @Override
        public String visitExcept(ExceptRelation relation, Void context) {
            return processSetOp(relation);
        }

        @Override
        public String visitIntersect(IntersectRelation relation, Void context) {
            return processSetOp(relation);
        }

        private String processSetOp(SetOperationRelation relation) {
            StringBuilder sqlBuilder = new StringBuilder();

            sqlBuilder.append(visit(relation.getRelations().get(0)));

            for (int i = 1; i < relation.getRelations().size(); ++i) {
                if (relation instanceof UnionRelation) {
                    sqlBuilder.append(" UNION ");
                } else if (relation instanceof ExceptRelation) {
                    sqlBuilder.append(" EXCEPT ");
                } else {
                    sqlBuilder.append(" INTERSECT ");
                }

                sqlBuilder.append(relation.getQualifier() == SetQualifier.ALL ? "ALL " : "");

                Relation setChildRelation = relation.getRelations().get(i);
                if (setChildRelation instanceof SetOperationRelation) {
                    sqlBuilder.append("(");
                }
                sqlBuilder.append(visit(setChildRelation));
                if (setChildRelation instanceof SetOperationRelation) {
                    sqlBuilder.append(")");
                }
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitTable(TableRelation node, Void outerScope) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(node.getName().toString());

            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append(node.getAlias().getTbl());
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitValues(ValuesRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();
            if (node.isNullValues()) {
                return null;
            }

            sqlBuilder.append("(VALUES");
            List<String> values = new ArrayList<>();
            for (int i = 0; i < node.getRows().size(); ++i) {
                StringBuilder rowBuilder = new StringBuilder();
                rowBuilder.append("(");
                List<String> rowStrings =
                        node.getRows().get(i).stream().map(this::visit).collect(Collectors.toList());
                rowBuilder.append(Joiner.on(", ").join(rowStrings));
                rowBuilder.append(")");
                values.add(rowBuilder.toString());
            }
            sqlBuilder.append(Joiner.on(", ").join(values));
            sqlBuilder.append(")");
            if (node.getAlias() != null) {
                sqlBuilder.append(" ").append(node.getAlias().getTbl());

                if (node.getExplicitColumnNames() != null) {
                    sqlBuilder.append("(");
                    sqlBuilder.append(Joiner.on(",").join(node.getExplicitColumnNames()));
                    sqlBuilder.append(")");
                }
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitTableFunction(TableFunctionRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();

            sqlBuilder.append(node.getFunctionName());
            sqlBuilder.append("(");

            List<String> childSql = node.getChildExpressions().stream().map(this::visit).collect(toList());
            sqlBuilder.append(Joiner.on(",").join(childSql));

            sqlBuilder.append(")");
            if (node.getAlias() != null) {
                sqlBuilder.append(" ").append(node.getAlias().getTbl());

                if (node.getColumnOutputNames() != null) {
                    sqlBuilder.append("(");
                    sqlBuilder.append(Joiner.on(",").join(node.getColumnOutputNames()));
                    sqlBuilder.append(")");
                }
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("TABLE(");

            TableFunctionRelation tableFunction = (TableFunctionRelation) node.getRight();
            sqlBuilder.append(tableFunction.getFunctionName());
            sqlBuilder.append("(");
            sqlBuilder.append(tableFunction.getChildExpressions().stream().map(this::visit).collect(Collectors.joining(",")));
            sqlBuilder.append(")");
            sqlBuilder.append(")"); // TABLE(

            if (tableFunction.getAlias() != null) {
                sqlBuilder.append(" ").append(tableFunction.getAlias().getTbl());
                if (tableFunction.getColumnOutputNames() != null) {
                    sqlBuilder.append("(");
                    sqlBuilder.append(Joiner.on(",").join(tableFunction.getColumnOutputNames()));
                    sqlBuilder.append(")");
                }
            }

            return sqlBuilder.toString();
        }

        // ---------------------------------- Expression --------------------------------

        @Override
        public String visitExpression(Expr node, Void context) {
            return node.toSql();
        }

        @Override
        public String visitArithmeticExpr(ArithmeticExpr node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();

            if (node.getChildren().size() == 1) {
                sqlBuilder.append(node.getOp());
                sqlBuilder.append(printWithParentheses(node.getChild(0)));
            } else {
                sqlBuilder.append(printWithParentheses(node.getChild(0)));
                sqlBuilder.append(" ").append(node.getOp()).append(" ");
                sqlBuilder.append(printWithParentheses(node.getChild(1)));
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitAnalyticExpr(AnalyticExpr node, Void context) {
            FunctionCallExpr fnCall = node.getFnCall();
            List<Expr> partitionExprs = node.getPartitionExprs();
            List<OrderByElement> orderByElements = node.getOrderByElements();
            AnalyticWindow window = node.getWindow();

            StringBuilder sb = new StringBuilder();
            sb.append(visit(fnCall)).append(" OVER (");
            if (!partitionExprs.isEmpty()) {
                sb.append("PARTITION BY ").append(visitAstList(partitionExprs)).append(" ");
            }
            if (!orderByElements.isEmpty()) {
                sb.append("ORDER BY ").append(visitAstList(orderByElements)).append(" ");
            }
            if (window != null) {
                sb.append(window.toSql());
            }
            sb.append(")");
            return sb.toString();
        }

        public String visitArrayExpr(ArrayExpr node, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            sb.append(visitAstList(node.getChildren()));
            sb.append(']');
            return sb.toString();
        }

        public String visitMapExpr(MapExpr node, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append("map{");
            for (int i = 0; i < node.getChildren().size(); i = i + 2) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(visit(node.getChild(i)) + ":" + visit(node.getChild(i + 1)));
            }
            sb.append("}");
            return sb.toString();
        }

        @Override
        public String visitCollectionElementExpr(CollectionElementExpr node, Void context) {
            return visit(node.getChild(0)) + "[" + visit(node.getChild(1)) + "]";
        }

        @Override
        public String visitArrowExpr(ArrowExpr node, Void context) {
            return String.format("%s->%s", visit(node.getItem(), context), visit(node.getKey(), context));
        }

        @Override
        public String visitBetweenPredicate(BetweenPredicate node, Void context) {
            String notStr = (node.isNotBetween()) ? "NOT " : "";
            return printWithParentheses(node.getChild(0)) + " " + notStr + "BETWEEN " +
                    printWithParentheses(node.getChild(1)) + " AND " + printWithParentheses(node.getChild(2));
        }

        @Override
        public String visitBinaryPredicate(BinaryPredicate node, Void context) {
            return printWithParentheses(node.getChild(0)) +
                    " " + node.getOp() + " " +
                    printWithParentheses(node.getChild(1));
        }

        @Override
        public String visitCaseWhenExpr(CaseExpr node, Void context) {
            boolean hasCaseExpr = node.hasCaseExpr();
            boolean hasElseExpr = node.hasElseExpr();
            StringBuilder output = new StringBuilder("CASE");
            int childIdx = 0;
            if (hasCaseExpr) {
                output.append(" ").append(printWithParentheses(node.getChild(childIdx++)));
            }
            while (childIdx + 2 <= node.getChildren().size()) {
                output.append(" WHEN ").append(printWithParentheses(node.getChild(childIdx++)));
                output.append(" THEN ").append(printWithParentheses(node.getChild(childIdx++)));
            }
            if (hasElseExpr) {
                output.append(" ELSE ").append(printWithParentheses(node.getChild(node.getChildren().size() - 1)));
            }
            output.append(" END");
            return output.toString();
        }

        @Override
        public String visitCastExpr(CastExpr node, Void context) {
            boolean isImplicit = node.isImplicit();
            if (isImplicit) {
                return visit(node.getChild(0));
            }
            return "CAST(" + printWithParentheses(node.getChild(0)) + " AS " + node.getTargetTypeDef().toString() + ")";
        }

        public String visitCompoundPredicate(CompoundPredicate node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            if (CompoundPredicate.Operator.NOT.equals(node.getOp())) {
                sqlBuilder.append("NOT ");
                sqlBuilder.append(printWithParentheses(node.getChild(0)));
            } else {
                sqlBuilder.append(printWithParentheses(node.getChild(0)));
                sqlBuilder.append(" ").append(node.getOp()).append(" ");
                sqlBuilder.append(printWithParentheses(node.getChild(1)));
            }
            return sqlBuilder.toString();
        }

        public String visitDefaultValueExpr(DefaultValueExpr node, Void context) {
            return visitExpression(node, context);
        }

        @Override
        public String visitExistsPredicate(ExistsPredicate node, Void context) {
            StringBuilder strBuilder = new StringBuilder();
            if (node.isNotExists()) {
                strBuilder.append("NOT ");

            }
            strBuilder.append("EXISTS ");
            strBuilder.append(visit(node.getChild(0)));
            return strBuilder.toString();
        }

        public String visitFieldReference(FieldReference node, Void context) {
            return String.valueOf(node.getFieldIndex() + 1);
        }

        @Override
        public String visitFunctionCall(FunctionCallExpr node, Void context) {
            FunctionParams fnParams = node.getParams();
            StringBuilder sb = new StringBuilder();
            if (addFunctionDbName && node.getFnName().getDb() != null) {
                sb.append("`" + node.getFnName().getDb() + "`.");
            }
            String functionName = node.getFnName().getFunction();
            sb.append(functionName);

            sb.append("(");
            if (fnParams.isStar()) {
                sb.append("*");
            }
            if (fnParams.isDistinct()) {
                sb.append("DISTINCT ");
            }

            if (functionName.equalsIgnoreCase(FunctionSet.TIME_SLICE) || functionName.equalsIgnoreCase(FunctionSet.DATE_SLICE)) {
                sb.append(visit(node.getChild(0))).append(", ");
                sb.append("INTERVAL ");
                sb.append(visit(node.getChild(1)));
                StringLiteral ident = (StringLiteral) node.getChild(2);
                sb.append(" ").append(ident.getValue());
                StringLiteral boundary = (StringLiteral) node.getChild(3);
                sb.append(", ").append(boundary.getValue());
                sb.append(")");
            } else if (functionName.equals(FunctionSet.ARRAY_AGG) || functionName.equals(FunctionSet.GROUP_CONCAT)) {
                int end = 1;
                if (functionName.equals(FunctionSet.GROUP_CONCAT)) {
                    end = fnParams.exprs().size() - fnParams.getOrderByElemNum() - 1;
                }
                for (int i = 0; i < end && i < node.getChildren().size(); ++i) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append(visit(node.getChild(i)));
                }
                List<OrderByElement> sortClause = fnParams.getOrderByElements();
                if (sortClause != null) {
                    sb.append(" ORDER BY ").append(visitAstList(sortClause));
                }
                if (functionName.equals(FunctionSet.GROUP_CONCAT) && end < node.getChildren().size() && end > 0) {
                    sb.append(" SEPARATOR ");
                    sb.append(visit(node.getChild(end)));
                }
                sb.append(")");
            } else if (IGNORE_NULL_WINDOW_FUNCTION.contains(functionName)) {
                List<String> p = node.getChildren().stream().map(child -> {
                    String str = visit(child);
                    if (child instanceof SlotRef && node.getIgnoreNulls()) {
                        str += " ignore nulls";
                    }
                    return str;
                }).collect(Collectors.toList());
                sb.append(Joiner.on(", ").join(p)).append(")");
            } else {
                List<String> p = node.getChildren().stream().map(this::visit).collect(Collectors.toList());
                sb.append(Joiner.on(", ").join(p)).append(")");
            }
            return sb.toString();
        }

        // Logical is the same as toSqlImpl() in LambdaFunctionExpr
        @Override
        public String visitLambdaFunctionExpr(LambdaFunctionExpr node, Void context) {
            List<Expr> children = node.getChildren();
            String names = visit(children.get(1));

            if (children.size() > 2) {
                names = "(" + visit(children.get(1));
                for (int i = 2; i < children.size(); ++i) {
                    names = names + ", " + visit(children.get(i));
                }
                names = names + ")";
            }
            return String.format("%s -> %s", names, visit(children.get(0)));
        }

        @Override
        public String visitSubfieldExpr(SubfieldExpr node, Void context) {
            StringJoiner joiner = new StringJoiner(".");
            for (String field : node.getFieldNames()) {
                if (withBackquote) {
                    joiner.add(ParseUtil.backquote(field));
                } else {
                    joiner.add(field);
                }

            }
            return String.format("%s.%s", visit(node.getChild(0)), joiner);
        }

        public String visitGroupingFunctionCall(GroupingFunctionCallExpr node, Void context) {
            return visitFunctionCall(node, context);
        }

        public String visitInformationFunction(InformationFunction node, Void context) {
            return visitExpression(node, context);
        }

        @Override
        public String visitInPredicate(InPredicate node, Void context) {
            StringBuilder strBuilder = new StringBuilder();
            String notStr = (node.isNotIn()) ? "NOT " : "";
            strBuilder.append(printWithParentheses(node.getChild(0))).append(" ").append(notStr).append("IN (");
            for (int i = 1; i < node.getChildren().size(); ++i) {
                strBuilder.append(printWithParentheses(node.getChild(i)));
                strBuilder.append((i + 1 != node.getChildren().size()) ? ", " : "");
            }
            strBuilder.append(")");
            return strBuilder.toString();
        }

        public String visitIsNullPredicate(IsNullPredicate node, Void context) {
            return printWithParentheses(node.getChild(0)) + (node.isNotNull() ? " IS NOT NULL" : " IS NULL");
        }

        public String visitLikePredicate(LikePredicate node, Void context) {
            return printWithParentheses(node.getChild(0))
                    + " " + node.getOp() + " " + printWithParentheses(node.getChild(1));
        }

        @Override
        public String visitLiteral(LiteralExpr node, Void context) {
            if (node instanceof DecimalLiteral) {
                if ((((DecimalLiteral) node).getValue().scale() == 0)) {
                    return ((DecimalLiteral) node).getValue().toString() + "E0";
                } else {
                    return visitExpression(node, context);
                }
            } else {
                return visitExpression(node, context);
            }
        }

        @Override
        public String visitSlot(SlotRef node, Void context) {
            if (node.getTblNameWithoutAnalyzed() != null) {
                return node.getTblNameWithoutAnalyzed().toString() + "." + node.getColumnName();
            } else {
                return node.getColumnName();
            }
        }

        @Override
        public String visitSubquery(Subquery node, Void context) {
            return "(" + visit(node.getQueryStatement()) + ")";
        }

        public String visitVariableExpr(VariableExpr node, Void context) {
            StringBuilder sb = new StringBuilder();
            if (node.getSetType() == SetType.USER) {
                sb.append("@");
            } else {
                sb.append("@@");
                if (node.getSetType() == SetType.GLOBAL) {
                    sb.append("GLOBAL.");
                } else if (node.getSetType() != null) {
                    sb.append("SESSION.");
                }
            }
            sb.append(node.getName());
            return sb.toString();
        }

        @Override
        public String visitTimestampArithmeticExpr(TimestampArithmeticExpr node, Void context) {
            String funcName = node.getFuncName();
            String timeUnitIdent = node.getTimeUnitIdent();
            boolean intervalFirst = node.isIntervalFirst();
            ArithmeticExpr.Operator op = node.getOp();

            StringBuilder strBuilder = new StringBuilder();
            if (funcName != null) {
                if (funcName.equalsIgnoreCase("TIMESTAMPDIFF") || funcName.equalsIgnoreCase("TIMESTAMPADD")) {
                    strBuilder.append(funcName).append("(");
                    strBuilder.append(timeUnitIdent).append(", ");
                    strBuilder.append(visit(node.getChild(1))).append(", ");
                    strBuilder.append(visit(node.getChild(0))).append(")");
                    return strBuilder.toString();
                }
                // Function-call like version.
                strBuilder.append(funcName).append("(");
                strBuilder.append(visit(node.getChild(0))).append(", ");
                strBuilder.append("INTERVAL ");
                strBuilder.append(visit(node.getChild(1)));
                strBuilder.append(" ").append(timeUnitIdent);
                strBuilder.append(")");
                return strBuilder.toString();
            }
            if (intervalFirst) {
                // Non-function-call like version with interval as first operand.
                strBuilder.append("INTERVAL ");
                strBuilder.append(visit(node.getChild(1))).append(" ");
                strBuilder.append(timeUnitIdent);
                strBuilder.append(" ").append(op.toString()).append(" ");
                strBuilder.append(visit(node.getChild(0)));
            } else {
                // Non-function-call like version with interval as second operand.
                strBuilder.append(visit(node.getChild(0)));
                strBuilder.append(" ").append(op.toString()).append(" ");
                strBuilder.append("INTERVAL ");
                strBuilder.append(visit(node.getChild(1))).append(" ");
                strBuilder.append(timeUnitIdent);
            }
            return strBuilder.toString();
        }

        // ----------------- AST ---------------
        @Override
        public String visitLimitElement(LimitElement node, Void context) {
            if (node.getLimit() == -1) {
                return "";
            }
            StringBuilder sb = new StringBuilder(" LIMIT ");
            if (node.getOffset() != 0) {
                sb.append(node.getOffset()).append(", ");
            }
            sb.append(node.getLimit());
            return sb.toString();
        }

        @Override
        public String visitOrderByElement(OrderByElement node, Void context) {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append(visit(node.getExpr()));
            strBuilder.append(node.getIsAsc() ? " ASC" : " DESC");

            // When ASC and NULLS FIRST or DESC and NULLS LAST, we do not print NULLS FIRST/LAST
            // because it is the default behavior
            if (node.getNullsFirstParam() != null) {
                if (node.getIsAsc() && !node.getNullsFirstParam()) {
                    // If ascending, nulls are first by default, so only add if nulls last.
                    strBuilder.append(" NULLS LAST");
                } else if (!node.getIsAsc() && node.getNullsFirstParam()) {
                    // If descending, nulls are last by default, so only add if nulls first.
                    strBuilder.append(" NULLS FIRST");
                }
            }
            return strBuilder.toString();
        }

        @Override
        public String visitGroupByClause(GroupByClause node, Void context) {
            GroupByClause.GroupingType groupingType = node.getGroupingType();
            List<ArrayList<Expr>> groupingSetList = node.getGroupingSetList();
            List<Expr> oriGroupingExprs = node.getOriGroupingExprs();

            StringBuilder strBuilder = new StringBuilder();
            switch (groupingType) {
                case GROUP_BY:
                    if (oriGroupingExprs != null) {
                        for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                            strBuilder.append(visit(oriGroupingExprs.get(i)));
                            strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                        }
                    }
                    break;
                case GROUPING_SETS:
                    if (groupingSetList != null) {
                        strBuilder.append("GROUPING SETS (");
                        boolean first = true;
                        for (List<Expr> groupingExprs : groupingSetList) {
                            if (first) {
                                strBuilder.append("(");
                                first = false;
                            } else {
                                strBuilder.append(", (");
                            }
                            for (int i = 0; i < groupingExprs.size(); ++i) {
                                strBuilder.append(visit(groupingExprs.get(i)));
                                strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                            }
                            strBuilder.append(")");
                        }
                        strBuilder.append(")");
                    }
                    break;
                case CUBE:
                    if (oriGroupingExprs != null) {
                        strBuilder.append("CUBE (");
                        for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                            strBuilder.append(visit(oriGroupingExprs.get(i)));
                            strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                        }
                        strBuilder.append(")");
                    }
                    break;
                case ROLLUP:
                    if (oriGroupingExprs != null) {
                        strBuilder.append("ROLLUP (");
                        for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                            strBuilder.append(visit(oriGroupingExprs.get(i)));
                            strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                        }
                        strBuilder.append(")");
                    }
                    break;
                default:
                    break;
            }
            return strBuilder.toString();
        }

        private String visitAstList(List<? extends ParseNode> contexts) {
            return Joiner.on(", ").join(contexts.stream().map(this::visit).collect(toList()));
        }

        private String printWithParentheses(ParseNode node) {
            if (node instanceof SlotRef || node instanceof LiteralExpr) {
                return visit(node);
            } else {
                return "(" + visit(node) + ")";
            }
        }

        // --------------------------------------------Storage volume Statement ----------------------------------------

        @Override
        public String visitCreateStorageVolumeStatement(CreateStorageVolumeStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE STORAGE VOLUME ");
            if (stmt.isSetIfNotExists()) {
                sb.append("IF NOT EXISTS ");
            }
            sb.append(stmt.getName());
            sb.append(" TYPE = ").append(stmt.getStorageVolumeType());
            sb.append(" LOCATIONS = (");
            List<String> locations = stmt.getStorageLocations();
            for (int i = 0; i < locations.size(); ++i) {
                if (i == 0) {
                    sb.append("'").append(locations.get(i)).append("'");
                } else {
                    sb.append(", '").append(locations.get(i)).append("'");
                }
            }
            sb.append(")");
            if (!stmt.getComment().isEmpty()) {
                sb.append(" COMMENT '").append(stmt.getComment()).append("'");
            }
            Map<String, String> properties = new HashMap<>(stmt.getProperties());
            StorageVolume.addMaskForCredential(properties);
            if (!stmt.getProperties().isEmpty()) {
                sb.append(" PROPERTIES (")
                        .append(new PrintableMap<>(properties, "=", true, false)).append(")");
            }
            return sb.toString();
        }

        @Override
        public String visitAlterStorageVolumeStatement(AlterStorageVolumeStmt stmt, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append("ALTER STORAGE VOLUME ").append(stmt.getName());
            if (!stmt.getComment().isEmpty()) {
                sb.append(" COMMENT = '").append(stmt.getComment()).append("'");
            }
            Map<String, String> properties = new HashMap<>(stmt.getProperties());
            StorageVolume.addMaskForCredential(properties);
            if (!properties.isEmpty()) {
                sb.append(" SET (").
                        append(new PrintableMap<>(properties, "=", true, false))
                        .append(")");
            }
            return sb.toString();
        }
    }
}
