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

package com.starrocks.sql.formatter;

import com.google.common.base.Joiner;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TableSampleClause;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FieldReference;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.LargeInPredicate;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TableName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class AST2SQLVisitor extends AST2StringVisitor {
    private static final int MASSIVE_COMPOUND_LIMIT = 16;

    // use options:
    //   columnSimplifyTableName;
    //   columnWithTableName;
    //   enableDigest;
    //   enableNewLine;
    //   enableMassiveExpr;
    //   printActualSelectItem;
    //   enableHints;
    public static AST2SQLVisitor withOptions(FormatOptions options) {
        AST2SQLVisitor visitor = new AST2SQLVisitor();
        visitor.options = options;
        return visitor;
    }

    // --------------------------------------- Statement -------------------------------------------------
    private String buildColumnName(TableName tableName, String fieldName, String columnName) {
        String res = "";
        if (tableName != null && options.isColumnWithTableName()) {
            if (!options.isColumnSimplifyTableName()) {
                res = tableName.toSql();
            } else {
                res = "`" + tableName.getTbl() + "`";
            }
            res += ".";
        }

        res += '`' + fieldName + '`';
        if (columnName != null && !fieldName.equalsIgnoreCase(columnName)) {
            res += " AS `" + columnName + "`";
        }
        return res;
    }

    private String buildStructColumnName(TableName tableName, String fieldName, String columnName) {
        String res = "";
        if (tableName != null) {
            if (!options.isColumnSimplifyTableName()) {
                res = tableName.toSql();
            } else {
                res = "`" + tableName.getTbl() + "`";
            }
            res += ".";
        }

        fieldName = handleColumnName(fieldName);
        res += fieldName;

        if (columnName != null) {
            columnName = handleColumnName(columnName);
            if (!fieldName.equalsIgnoreCase(columnName)) {
                res += " AS " + columnName;
            }
        }
        return res;
    }

    // Consider struct, like fieldName = a.b.c, columnName = a.b.c
    private String handleColumnName(String name) {
        String[] fields = name.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            sb.append("`");
            sb.append(fields[i]);
            sb.append("`");
            if (i < fields.length - 1) {
                sb.append(".");
            }
        }
        return sb.toString();
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

        // add hint
        if (options.isEnableHints() && selectList.getHintNodes() != null) {
            sqlBuilder.append(extractHintStr(selectList.getHintNodes()));
        }

        if (selectList.isDistinct()) {
            sqlBuilder.append("DISTINCT ");
        }

        sqlBuilder.append(Joiner.on(", ").join(visitSelectItemList(stmt)));

        String fromClause = visit(stmt.getRelation());
        if (fromClause != null) {
            sqlBuilder.append(options.newLine()).append("FROM ");
            sqlBuilder.append(fromClause);
        }

        if (stmt.hasWhereClause()) {
            sqlBuilder.append(options.newLine()).append("WHERE ");
            sqlBuilder.append(visit(stmt.getWhereClause()));
        }

        if (stmt.hasGroupByClause()) {
            sqlBuilder.append(options.newLine()).append("GROUP BY ");
            sqlBuilder.append(visit(stmt.getGroupByClause()));
        }

        if (stmt.hasHavingClause()) {
            sqlBuilder.append(options.newLine()).append("HAVING ");
            sqlBuilder.append(visit(stmt.getHavingClause()));
        }

        return sqlBuilder.toString();
    }

    protected List<String> visitSelectItemList(SelectRelation stmt) {
        if (options.isPrintActualSelectItem() && CollectionUtils.isNotEmpty(stmt.getOutputExpression())) {
            List<String> selectListString = new ArrayList<>();
            List<String> columnNameList = stmt.getColumnOutputNames();
            for (int i = 0; i < stmt.getOutputExpression().size(); ++i) {
                Expr expr = stmt.getOutputExpression().get(i);
                String columnName = columnNameList.get(i);

                if (expr instanceof FieldReference) {
                    Field field = stmt.getScope().getRelationFields().getFieldByIndex(i);
                    selectListString.add(buildColumnName(field.getRelationAlias(), field.getName(), columnName));
                } else if (expr instanceof SlotRef slot) {
                    if (slot.getOriginType().isStructType()) {
                        selectListString.add(buildStructColumnName(slot.getTblNameWithoutAnalyzed(),
                                slot.getColumnName(), columnName));
                    } else {
                        selectListString.add(buildColumnName(slot.getTblNameWithoutAnalyzed(), slot.getColumnName(),
                                columnName));
                    }
                } else if (columnName != null) {
                    selectListString.add(visit(expr) + " AS `" + columnName + "`");
                } else {
                    selectListString.add(visit(expr));
                }
            }
            return selectListString;
        } else {
            List<String> selectListString = new ArrayList<>();
            for (SelectListItem item : stmt.getSelectList().getItems()) {
                if (item.isStar()) {
                    String tmp = "";
                    if (item.getTblName() != null) {
                        tmp = item.getTblName() + ".*";
                    } else {
                        tmp = "*";
                    }
                    if (!item.getExcludedColumns().isEmpty()) {
                        tmp += " EXCLUDE ( ";
                        tmp += item.getExcludedColumns().stream()
                                .map(col -> "\"" + col + "\"")
                                .collect(Collectors.joining(","));
                        tmp += " ) ";
                    }
                    selectListString.add(tmp);
                } else if (item.getExpr() != null) {
                    Expr expr = item.getExpr();
                    String str = visit(expr);
                    if (StringUtils.isNotEmpty(item.getAlias())) {
                        str += " AS " + ParseUtil.backquote(item.getAlias());
                    }
                    selectListString.add(str);
                }
            }
            return selectListString;
        }
    }

    @Override
    public String visitJoin(JoinRelation relation, Void context) {
        String join;
        if (options.isEnableHints()) {
            join = super.visitJoin(relation, context);
        } else {
            String hints = relation.getJoinHint();
            relation.setJoinHint(null);
            join = super.visitJoin(relation, context);
            relation.setJoinHint(hints);
        }
        return join;
    }

    @Override
    public String visitCTE(CTERelation relation, Void context) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("`" + relation.getName() + "`");

        if (relation.isResolvedInFromClause()) {
            if (relation.getAlias() != null) {
                sqlBuilder.append(" AS ").append(relation.getAlias().getTbl());
            }
            return sqlBuilder.toString();
        }

        if (relation.getColumnOutputNames() != null) {
            sqlBuilder.append(" (")
                    .append(Joiner.on(", ").join(
                            relation.getColumnOutputNames().stream().map(c -> "`" + c + "`").collect(toList())))
                    .append(")");
        }
        sqlBuilder.append(" AS (").append(visit(relation.getCteQueryStatement())).append(") ");
        return sqlBuilder.toString();
    }

    @Override
    public String visitSubqueryRelation(SubqueryRelation node, Void context) {
        StringBuilder sqlBuilder = new StringBuilder("(" + visit(node.getQueryStatement()) + ")");

        if (node.getAlias() != null) {
            sqlBuilder.append(" ").append(ParseUtil.backquote(node.getAlias().getTbl()));

            if (node.getExplicitColumnNames() != null) {
                List<String> explicitColNames = new ArrayList<>();
                node.getExplicitColumnNames().forEach(e -> explicitColNames.add(ParseUtil.backquote(e)));
                sqlBuilder.append("(");
                sqlBuilder.append(Joiner.on(",").join(explicitColNames));
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
            sqlBuilder.append("`").append(node.getAlias().getTbl()).append("`");
        }
        return sqlBuilder.toString();
    }

    @Override
    public String visitTable(TableRelation node, Void outerScope) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(node.getName().toSql());

        if (node.getPartitionNames() != null) {
            List<String> partitionNames = node.getPartitionNames().getPartitionNames();
            if (partitionNames != null && !partitionNames.isEmpty()) {
                sqlBuilder.append(" PARTITION (");
                sqlBuilder.append(partitionNames.stream().map(c -> "`" + c + "`")
                        .collect(Collectors.joining(", ")));
                sqlBuilder.append(")");
            }
        }

        for (TableRelation.TableHint hint : CollectionUtils.emptyIfNull(node.getTableHints())) {
            sqlBuilder.append(" [");
            sqlBuilder.append(hint.name());
            sqlBuilder.append("] ");
        }

        if (node.getSampleClause() != null) {
            TableSampleClause sample = node.getSampleClause();
            sqlBuilder.append(" ").append(sample.toSql());
        }

        if (node.getAlias() != null) {
            sqlBuilder.append(" AS ");
            sqlBuilder.append("`").append(node.getAlias().getTbl()).append("`");
        }
        return sqlBuilder.toString();
    }

    @Override
    public String visitTableFunction(TableFunctionRelation node, Void scope) {
        StringBuilder sqlBuilder = new StringBuilder();

        sqlBuilder.append(node.getFunctionName());
        sqlBuilder.append("(");

        List<String> childSql = Optional.ofNullable(node.getChildExpressions())
                .orElse(node.getFunctionParams().exprs()).stream().map(this::visit).collect(toList());
        sqlBuilder.append(Joiner.on(",").join(childSql));

        sqlBuilder.append(")");
        if (node.getAlias() != null) {
            sqlBuilder.append(" ").append(node.getAlias().getTbl());

            if (node.getColumnOutputNames() != null) {
                sqlBuilder.append("(");
                String names = node.getColumnOutputNames().stream().map(c -> "`" + c + "`")
                        .collect(Collectors.joining(","));
                sqlBuilder.append(names);
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
        sqlBuilder.append(
                Optional.ofNullable(tableFunction.getChildExpressions())
                        .orElse(Collections.emptyList()).stream().map(this::visit)
                        .collect(Collectors.joining(",")));
        sqlBuilder.append(")");
        sqlBuilder.append(")"); // TABLE(

        if (tableFunction.getAlias() != null) {
            sqlBuilder.append(" ").append(tableFunction.getAlias().getTbl());
            if (tableFunction.getColumnOutputNames() != null) {
                sqlBuilder.append("(");
                String names = tableFunction.getColumnOutputNames().stream().map(c -> "`" + c + "`")
                        .collect(Collectors.joining(","));
                sqlBuilder.append(names);
                sqlBuilder.append(")");
            }
        }

        return sqlBuilder.toString();
    }

    @Override
    public String visitExpression(Expr expr, Void context) {
        return expr.toSql();
    }

    @Override
    public String visitSlot(SlotRef expr, Void context) {
        if (expr.getOriginType().isStructType()) {
            return buildStructColumnName(expr.getTblNameWithoutAnalyzed(),
                    expr.getColumnName(), expr.getColumnName());
        } else {
            return buildColumnName(expr.getTblNameWithoutAnalyzed(),
                    expr.getColumnName(), expr.getColumnName());
        }
    }

    @Override
    public String visitArrayExpr(ArrayExpr node, Void context) {
        StringBuilder sb = new StringBuilder();
        Type type = AnalyzerUtils.replaceNullType2Boolean(node.getType());
        sb.append(type.toString());
        sb.append('[');
        sb.append(node.getChildren().stream().map(this::visit).collect(Collectors.joining(", ")));
        sb.append(']');
        return sb.toString();
    }

    @Override
    public String visitMapExpr(MapExpr node, Void context) {
        StringBuilder sb = new StringBuilder();
        Type type = AnalyzerUtils.replaceNullType2Boolean(node.getType());
        sb.append(type.toString());
        sb.append("{");
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
    public String visitInPredicate(InPredicate node, Void context) {
        if (!options.isEnableDigest()) {
            return super.visitInPredicate(node, context);
        }
        if (!node.isConstantValues()) {
            return super.visitInPredicate(node, context);
        }

        StringBuilder strBuilder = new StringBuilder();
        String notStr = (node.isNotIn()) ? "NOT " : "";
        strBuilder.append(printWithParentheses(node.getChild(0))).append(" ").append(notStr).append("IN ");
        if (options.isEnableMassiveExpr()) {
            strBuilder.append("(?)");
        } else {
            strBuilder.append("(");
            strBuilder.append(StringUtils.repeat("?", ", ", node.getChildren().size() - 1));
            strBuilder.append(")");
        }
        return strBuilder.toString();
    }

    @Override
    public String visitLargeInPredicate(LargeInPredicate node, Void context) {
        if (!options.isEnableDigest()) {
            return super.visitLargeInPredicate(node, context);
        }

        StringBuilder strBuilder = new StringBuilder();
        String notStr = (node.isNotIn()) ? "NOT " : "";
        strBuilder.append(printWithParentheses(node.getCompareExpr())).append(" ").append(notStr).append("IN ");
        if (options.isEnableMassiveExpr()) {
            strBuilder.append("(?)");
        } else {
            strBuilder.append("(");
            strBuilder.append(StringUtils.repeat("?", ", ", node.getConstantCount()));
            strBuilder.append(")");
        }
        return strBuilder.toString();
    }

    @Override
    public String visitCompoundPredicate(CompoundPredicate node, Void context) {
        if (!options.isEnableDigest()) {
            return super.visitCompoundPredicate(node, context);
        }
        List<Expr> flatten = AnalyzerUtils.flattenPredicate(node);
        if (flatten.size() >= MASSIVE_COMPOUND_LIMIT && options.isEnableMassiveExpr()) {
            // Only record de-duplicated slots if there are too many compounds
            List<SlotRef> exprs = node.collectAllSlotRefs(true);
            String sortedSlots = exprs.stream()
                    .filter(SlotRef::isColumnRef)
                    .map(SlotRef::toSql)
                    .sorted()
                    .collect(Collectors.joining(","));
            return "$massive_compounds[" + sortedSlots + "]$";
        } else {
            // TODO: it will introduce a little bit overhead in top-down visiting, in which the
            //  flattenPredicate is duplicated revoked. it's better to eliminate this overhead
            return super.visitCompoundPredicate(node, context);
        }
    }

    @Override
    public String visitValues(ValuesRelation node, Void scope) {
        if (!options.isEnableDigest()) {
            return super.visitValues(node, scope);
        }

        if (node.isNullValues()) {
            return "VALUES(NULL)";
        }
        StringBuilder sqlBuilder = new StringBuilder("VALUES");
        if (!node.getRows().isEmpty()) {
            StringBuilder rowBuilder = new StringBuilder();
            rowBuilder.append("(");
            List<String> rowStrings =
                    node.getRows().get(0).stream().map(this::visit).collect(Collectors.toList());
            rowBuilder.append(Joiner.on(", ").join(rowStrings));
            rowBuilder.append(")");
            sqlBuilder.append(rowBuilder);
        }
        return sqlBuilder.toString();
    }

    @Override
    protected void visitInsertLabel(String label, StringBuilder sb) {
        if (!options.isEnableDigest()) {
            super.visitInsertLabel(label, sb);
            return;
        }
        if (StringUtils.isNotEmpty(label)) {
            sb.append("WITH LABEL ? ");
        }
    }

    @Override
    public String visitLiteral(LiteralExpr expr, Void context) {
        if (!options.isEnableDigest()) {
            return super.visitLiteral(expr, context);
        }
        return "?";
    }

    @Override
    public String visitLimitElement(LimitElement node, Void context) {
        if (!options.isEnableDigest()) {
            return super.visitLimitElement(node, context);
        }
        if (node.getLimit() == -1) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" LIMIT ");
        if (node.getOffset() != 0) {
            sb.append(" ?, ");
        }
        sb.append(" ? ");
        return sb.toString();
    }
}
