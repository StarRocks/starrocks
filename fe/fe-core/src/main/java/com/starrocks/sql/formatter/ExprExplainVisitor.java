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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.ArraySliceExpr;
import com.starrocks.sql.ast.expression.ArrowExpr;
import com.starrocks.sql.ast.expression.BetweenPredicate;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CloneExpr;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DefaultValueExpr;
import com.starrocks.sql.ast.expression.DictMappingExpr;
import com.starrocks.sql.ast.expression.DictQueryExpr;
import com.starrocks.sql.ast.expression.DictionaryGetExpr;
import com.starrocks.sql.ast.expression.ExistsPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FieldReference;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.LambdaArgument;
import com.starrocks.sql.ast.expression.LambdaFunctionExpr;
import com.starrocks.sql.ast.expression.LargeInPredicate;
import com.starrocks.sql.ast.expression.LargeStringLiteral;
import com.starrocks.sql.ast.expression.LikePredicate;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.ast.expression.MaxLiteral;
import com.starrocks.sql.ast.expression.MultiInPredicate;
import com.starrocks.sql.ast.expression.NamedArgument;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.Parameter;
import com.starrocks.sql.ast.expression.PlaceHolderExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.SubfieldExpr;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.ast.expression.VarBinaryLiteral;
import com.starrocks.sql.ast.expression.VariableExpr;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*
 * @Todo: merge with AST2StringVisitor
 */
public class ExprExplainVisitor implements AstVisitorExtendInterface<String, Void> {
    private FormatOptions options = FormatOptions.allEnable();

    public ExprExplainVisitor() {
        options.setEnableDigest(false);
    }

    public ExprExplainVisitor(FormatOptions options) {
        this.options = options;
    }

    // ========================================= Helper Methods =========================================
    private List<String> visitChildren(List<Expr> children) {
        if (children == null || children.isEmpty()) {
            return List.of();
        }
        return children.stream()
                .map(child -> child.accept(this, null))
                .collect(Collectors.toList());
    }

    // ========================================= Literal Expressions =========================================
    public String visitLiteral(LiteralExpr node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }
        return node.getStringValue();
    }

    @Override
    public String visitStringLiteral(StringLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }

        String sql = node.getStringValue();
        if (sql != null) {
            if (sql.contains("\\")) {
                sql = sql.replace("\\", "\\\\");
            }
            sql = sql.replace("'", "\\'");
        }
        return "'" + sql + "'";
    }

    @Override
    public String visitBoolLiteral(BoolLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }
        return node.getValue() ? "TRUE" : "FALSE";
    }

    @Override
    public String visitNullLiteral(NullLiteral node, Void context) {
        return "NULL";
    }

    @Override
    public String visitDateLiteral(DateLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }

        return "'" + node.getStringValue() + "'";
    }

    @Override
    public String visitVarBinaryLiteral(VarBinaryLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }

        return "'" + node.getStringValue() + "'";
    }

    @Override
    public String visitLargeStringLiteral(LargeStringLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }

        String fullSql = visitStringLiteral(node, context);
        fullSql = fullSql.substring(0, LargeStringLiteral.LEN_LIMIT);
        return fullSql + "...'";
    }

    @Override
    public String visitMaxLiteral(MaxLiteral node, Void context) {
        return "MAXVALUE";
    }

    @Override
    public String visitIntervalLiteral(IntervalLiteral node, Void context) {
        return "interval " + visit(node.getValue()) + " " + node.getUnitIdentifier().getDescription();
    }

    // ========================================= References and Function Calls =========================================
    @Override
    public String visitSlot(SlotRef node, Void context) {
        StringBuilder sb = new StringBuilder();
        TableName tblName = node.getTblName();

        if (tblName != null && !node.isFromLambda()) {
            return tblName.toSql() + "." + "`" + node.getColName() + "`";
        } else if (node.getLabel() != null) {
            if (node.isBackQuoted() && !(node.getLabel().startsWith("`") && node.getLabel().endsWith("`"))) {
                sb.append("`").append(node.getLabel()).append("`");
                return sb.toString();
            } else {
                return node.getLabel();
            }
        } else if (node.getDesc().getSourceExprs() != null) {
            sb.append("<slot ").append(node.getDesc().getId().asInt()).append(">");
            for (Expr expr : node.getDesc().getSourceExprs()) {
                sb.append(" ");
                sb.append(visit(expr));
            }
            return sb.toString();
        } else {
            return "<slot " + node.getDesc().getId().asInt() + ">";
        }
    }

    // ========================================= Arithmetic and Predicates =========================================

    @Override
    public String visitArithmeticExpr(ArithmeticExpr node, Void context) {
        if (node.getChildren().size() == 1) {
            return node.getOp().toString() + " " + node.getChild(0).accept(this, context);
        } else {
            String left = node.getChild(0).accept(this, context);
            String right = node.getChild(1).accept(this, context);
            return left + " " + node.getOp().toString() + " " + right;
        }
    }

    @Override
    public String visitBinaryPredicate(BinaryPredicate node, Void context) {
        String left = node.getChild(0).accept(this, context);
        String right = node.getChild(1).accept(this, context);
        return left + " " + node.getOp().toString() + " " + right;
    }

    @Override
    public String visitCompoundPredicate(CompoundPredicate node, Void context) {
        String operator = node.getOp().toString();

        if (node.getOp() == CompoundPredicate.Operator.NOT) {
            return "NOT (" + node.getChild(0).accept(this, context) + ")";
        } else {
            String left = node.getChild(0).accept(this, context);
            String right = node.getChild(1).accept(this, context);
            return "(" + left + ") " + operator + " (" + right + ")";
        }
    }

    @Override
    public String visitBetweenPredicate(BetweenPredicate node, Void context) {
        String expr = node.getChild(0).accept(this, context);
        String lower = node.getChild(1).accept(this, context);
        String upper = node.getChild(2).accept(this, context);
        String notStr = node.isNotBetween() ? " NOT" : "";
        return expr + notStr + " BETWEEN " + lower + " AND " + upper;
    }

    @Override
    public String visitInPredicate(InPredicate node, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.getChild(0).accept(this, context));

        if (node.isNotIn()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        sb.append(node.getChildren().stream()
                .skip(1).map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitLargeInPredicate(LargeInPredicate node, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.getCompareExpr().accept(this, context));

        if (node.isNotIn()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        sb.append(node.getRawText());
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitIsNullPredicate(IsNullPredicate node, Void context) {
        String expr = node.getChild(0).accept(this, context);
        return expr + (node.isNotNull() ? " IS NOT NULL" : " IS NULL");
    }

    @Override
    public String visitLikePredicate(LikePredicate node, Void context) {
        String expr = node.getChild(0).accept(this, context);
        String pattern = node.getChild(1).accept(this, context);
        return expr + " " + node.getOp().toString() + " " + pattern;
    }

    @Override
    public String visitExistsPredicate(ExistsPredicate node, Void context) {
        String notStr = node.isNotExists() ? "NOT " : "";
        return notStr + "EXISTS " + node.getChild(0).accept(this, context);
    }

    // ========================================= Function Calls =========================================

    @Override
    public String visitFunctionCall(FunctionCallExpr node, Void context) {
        StringBuilder sb = new StringBuilder();

        sb.append(node.getFnName());
        sb.append("(");

        if (node.getFnParams().isStar()) {
            sb.append("*");
        }
        if (node.isDistinct()) {
            sb.append("DISTINCT ");
        }

        String childrenSql = node.getChildren().stream()
                .limit(node.getChildren().size() - node.getFnParams().getOrderByElemNum())
                .map(c -> visit(c, context))
                .collect(Collectors.joining(", "));
        sb.append(childrenSql);

        if (node.getFnParams().getOrderByElements() != null) {
            sb.append(node.getFnParams().getOrderByStringToSql());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitInformationFunction(InformationFunction node, Void context) {
        String funcName = node.getFuncType();
        return funcName + "()";
    }

    // ========================================= Cast and Case Expressions =========================================
    @Override
    public String visitCastExpr(CastExpr node, Void context) {
        String expr = node.getChild(0).accept(this, context);
        if (node.getTargetTypeDef() == null) {
            return "CAST(" + expr + " AS " + node.getType() + ")";
        } else {
            return "CAST(" + expr + " AS " + node.getTargetTypeDef() + ")";
        }
    }

    // ========================================= Collection Expressions =========================================

    @Override
    public String visitArrayExpr(ArrayExpr node, Void context) {
        return "[" + Joiner.on(",").join(visitChildren(node.getChildren())) + "]";
    }

    @Override
    public String visitMapExpr(MapExpr node, Void context) {
        List<String> pairs = new ArrayList<>();
        List<Expr> children = node.getChildren();

        for (int i = 0; i < children.size(); i += 2) {
            String key = children.get(i).accept(this, context);
            String value = children.get(i + 1).accept(this, context);
            pairs.add(key + ":" + value);
        }
        return "map{" + Joiner.on(",").join(pairs) + "}";
    }

    @Override
    public String visitCollectionElementExpr(CollectionElementExpr node, Void context) {
        String collection = node.getChild(0).accept(this, context);
        String index = node.getChild(1).accept(this, context);
        return collection + "[" + index + "]";
    }

    // ========================================= Subquery =========================================

    @Override
    public String visitSubqueryExpr(Subquery node, Void context) {
        return "(" + AstToStringBuilder.toString(node.getQueryStatement()) + ")";
    }

    // ========================================= Multi-Value Predicates =========================================
    @Override
    public String visitMultiInPredicate(MultiInPredicate node, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(node.getChildren().stream()
                .limit(node.getNumberOfColumns())
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        if (node.isNotIn()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        sb.append(node.getChildren().stream()
                .skip(node.getNumberOfColumns())
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    // ========================================= Analytic Expressions =========================================

    @Override
    public String visitAnalyticExpr(AnalyticExpr node, Void context) {
        if (node.getSqlString() != null) {
            return node.getSqlString();
        }
        StringBuilder sb = new StringBuilder();
        if (CollectionUtils.isNotEmpty(node.getPartitionExprs())) {
            sb.append(" PARTITION BY ");
            sb.append(String.join(", ", visitChildren(node.getPartitionExprs())));
        }

        if (CollectionUtils.isNotEmpty(node.getOrderByElements())) {
            sb.append(" ORDER BY ");
            sb.append(node.getOrderByElements().stream()
                    .map(c -> visit(c, context))
                    .collect(Collectors.joining(",")));
        }
        if (node.getWindow() != null) {
            sb.append(" ").append(node.getWindow().toSql());
        }

        FunctionCallExpr fnCall = node.getFnCall();
        return fnCall.accept(this, context) + " OVER (" + sb.toString().trim() + ")";
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

    // ========================================= Special Expressions =========================================
    @Override
    public String visitTimestampArithmeticExpr(TimestampArithmeticExpr node, Void context) {
        String funcName = node.getFuncName();
        StringBuilder strBuilder = new StringBuilder();
        if (funcName != null) {
            if (funcName.equalsIgnoreCase(FunctionSet.TIMESTAMPDIFF) || funcName.equalsIgnoreCase(FunctionSet.TIMESTAMPADD)) {
                strBuilder.append(funcName).append("(");
                strBuilder.append(node.getTimeUnitIdent()).append(", ");
                strBuilder.append(visit(node.getChild(1))).append(", ");
                strBuilder.append(visit(node.getChild(0))).append(")");
                return strBuilder.toString();
            }
            // Function-call like version.
            strBuilder.append(funcName).append("(");
            strBuilder.append(visit(node.getChild(0))).append(", ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(visit(node.getChild(1)));
            strBuilder.append(" ").append(node.getTimeUnitIdent());
            strBuilder.append(")");
            return strBuilder.toString();
        }
        if (node.isIntervalFirst()) {
            // Non-function-call like version with interval as first operand.
            strBuilder.append("INTERVAL ");
            strBuilder.append(visit(node.getChild(1))).append(" ");
            strBuilder.append(node.getTimeUnitIdent());
            strBuilder.append(" ").append(node.getOp().toString()).append(" ");
            strBuilder.append(visit(node.getChild(0)));
        } else {
            // Non-function-call like version with interval as second operand.
            strBuilder.append(visit(node.getChild(0)));
            strBuilder.append(" ").append(node.getOp().toString()).append(" ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(visit(node.getChild(1))).append(" ");
            strBuilder.append(node.getTimeUnitIdent());
        }
        return strBuilder.toString();
    }

    @Override
    public String visitVariableExpr(VariableExpr node, Void context) {
        return node.getSetType() + " " + node.getName() +
                (node.getValue() != null ? " = " + node.getValue() : "");
    }

    @Override
    public String visitUserVariableExpr(UserVariableExpr node, Void context) {
        return "@" + node.getName();
    }

    @Override
    public String visitDefaultValueExpr(DefaultValueExpr node, Void context) {
        return null;
    }

    @Override
    public String visitCloneExpr(CloneExpr node, Void context) {
        return "clone(" + node.getChild(0).accept(this, context) + ")";
    }

    // ========================================= Lambda Expressions =========================================
    @Override
    public String visitLambdaFunctionExpr(LambdaFunctionExpr node, Void context) {
        StringBuilder names = new StringBuilder(visit(node.getChild(1)));
        int realChildrenNum = node.getChildren().size() - 2 * node.getCommonSubOperatorNum();
        if (realChildrenNum > 2) {
            for (int i = 2; i < realChildrenNum; ++i) {
                names.append(", ").append(visit(node.getChild(i)));
            }
            names = new StringBuilder("(" + names + ")");
        }

        StringBuilder commonSubOp = new StringBuilder();
        if (node.getCommonSubOperatorNum() > 0) {
            commonSubOp.append("\n        lambda common expressions:");
        }

        for (int i = realChildrenNum; i < realChildrenNum + node.getCommonSubOperatorNum(); ++i) {
            commonSubOp.append("{")
                    .append(visit(node.getChild(i)))
                    .append(" <-> ")
                    .append(visit(node.getChild(i + node.getCommonSubOperatorNum())))
                    .append("}");
        }
        if (node.getCommonSubOperatorNum() > 0) {
            commonSubOp.append("\n        ");
        }
        return String.format("%s -> %s%s", names, visit(node.getChild(0)), commonSubOp);
    }

    @Override
    public String visitLambdaArguments(LambdaArgument node, Void context) {
        return node.getName();
    }

    // ========================================= Dictionary Expressions =========================================
    @Override
    public String visitDictionaryGetExpr(DictionaryGetExpr node, Void context) {
        String message = "DICTIONARY_GET(";
        int size = (node.getChildren().size() == 3) ? node.getChildren().size() - 1 : node.getChildren().size();
        message += node.getChildren().stream().limit(size)
                .map(this::visit)
                .collect(Collectors.joining(", "));
        message += ", " + (node.getNullIfNotExist() ? "true" : "false");
        message += ")";
        return message;
    }

    @Override
    public String visitDictMappingExpr(DictMappingExpr node, Void context) {
        String fnName = node.getType().matchesType(node.getChild(1).getType()) ? "DictDecode" : "DictDefine";

        if (node.getChildren().size() == 2) {
            return fnName + "(" + visit(node.getChild(0)) + ", [" + visit(node.getChild(1)) + "])";
        }
        return fnName + "(" + visit(node.getChild(0)) + ", ["
                + visit(node.getChild(1)) + "], "
                + visit(node.getChild(2)) + ")";
    }

    @Override
    public String visitDictQueryExpr(DictQueryExpr node, Void context) {
        return visitFunctionCall(node, context);
    }

    // ========================================= Arrow and Subfield Expressions =========================================

    @Override
    public String visitArraySliceExpr(ArraySliceExpr node, Void context) {
        return visit(node.getChild(0))
                + "[" + visit(node.getChild(1))
                + ":" + visit(node.getChild(2)) + "]";
    }

    @Override
    public String visitArrowExpr(ArrowExpr node, Void context) {
        return node.getItem().accept(this, context) + "->" +
                node.getKey().accept(this, context);
    }

    @Override
    public String visitSubfieldExpr(SubfieldExpr node, Void context) {
        return node.getChild(0).accept(this, context)
                + "." + String.join(".", node.getFieldNames())
                + "[" + node.isCopyFlag() + "]";
    }

    // ========================================= Match Expression =========================================

    @Override
    public String visitMatchExpr(MatchExpr node, Void context) {
        return visit(node.getChild(0)) + " " + node.getMatchOperator().getName() + " " + visit(node.getChild(1));
    }

    @Override
    public String visitFieldReference(FieldReference node, Void context) {
        return "FieldReference(" + node.getFieldIndex() + ")";
    }

    // ========================================= Named Arguments =========================================

    @Override
    public String visitNamedArgument(NamedArgument node, Void context) {
        return node.getName() + " => " + node.getExpr().accept(this, context);
    }

    @Override
    public String visitExpression(Expr node, Void context) {
        return "<" + node.getClass().getSimpleName() + ">";
    }

    @Override
    public String visitCaseWhenExpr(CaseExpr node, Void context) {
        StringBuilder output = new StringBuilder("CASE");
        int childIdx = 0;
        if (node.hasCaseExpr()) {
            output.append(" ").append(visit(node.getChildren().get(childIdx++)));
        }
        while (childIdx + 2 <= node.getChildren().size()) {
            output.append(" WHEN ").append(visit(node.getChildren().get(childIdx++)));
            output.append(" THEN ").append(visit(node.getChildren().get(childIdx++)));
        }
        if (node.hasElseExpr()) {
            output.append(" ELSE ").append(visit(node.getChildren().get(node.getChildren().size() - 1)));
        }
        output.append(" END");
        return output.toString();
    }

    @Override
    public String visitParameterExpr(Parameter node, Void context) {
        return "?";
    }

    @Override
    public String visitPlaceHolderExpr(PlaceHolderExpr node, Void context) {
        return "<place-holder>";
    }
}
