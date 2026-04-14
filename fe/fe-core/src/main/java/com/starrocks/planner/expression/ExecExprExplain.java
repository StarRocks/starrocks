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

package com.starrocks.planner.expression;

import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.Type;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates human-readable strings from {@link ExecExpr} trees for EXPLAIN output.
 * Follows the same formatting conventions as {@code ExprExplainVisitor} to ensure
 * test compatibility.
 */
public class ExecExprExplain implements ExecExprVisitor<String, Void> {

    private static final ExecExprExplain INSTANCE = new ExecExprExplain();

    public static String explain(ExecExpr expr) {
        return expr.accept(INSTANCE, null);
    }

    public static String explainList(java.util.List<? extends ExecExpr> exprs) {
        return exprs.stream()
                .map(ExecExprExplain::explain)
                .collect(Collectors.joining(", "));
    }

    /**
     * Instance that produces full SQL text without string truncation.
     * Used for generating actual SQL sent to external databases (MySQL/JDBC filters).
     */
    private static final ExecExprExplain SQL_INSTANCE = new ExecExprExplain() {
        @Override
        public String visitExecLiteral(ExecLiteral expr, Void context) {
            ConstantOperator value = expr.getValue();
            if (value.isNull()) {
                return "NULL";
            }
            Type t = expr.getType();
            if (t.isStringType() || t.isChar() || t.isVarchar()) {
                String s = value.getVarchar();
                s = s.replace("\\", "\\\\");
                s = s.replace("'", "\\'");
                // No truncation for SQL output
                return "'" + s + "'";
            }
            // For non-string types, delegate to the standard implementation
            return INSTANCE.visitExecLiteral(expr, context);
        }
    };

    /**
     * Generate SQL text for an expression without string truncation.
     * Suitable for SQL sent to external databases (MySQL/JDBC filter pushdown).
     */
    public static String toSql(ExecExpr expr) {
        return expr.accept(SQL_INSTANCE, null);
    }

    private static final ExecExprExplain VERBOSE_INSTANCE = new ExecExprExplain() {
        @Override
        public String visitExecSlotRef(ExecSlotRef expr, Void context) {
            // Use the descriptor's type for verbose explain, matching the old AST SlotRef behavior
            // where ExprVerboseVisitor used node.getDesc().getType() (which keeps CHAR as CHAR)
            // rather than the SlotRef's own type (which converts CHAR -> VARCHAR).
            com.starrocks.type.Type displayType = expr.getDesc() != null ? expr.getDesc().getType() : expr.getType();
            boolean nullable = expr.getDesc() != null ? expr.getDesc().getIsNullable() : expr.isNullable();
            if (expr.getLabel() != null) {
                return "[" + expr.getLabel() + ", " + displayType + ", " + nullable + "]";
            } else {
                return "[" + expr.getSlotId().asInt() + ", " + displayType + ", " + nullable + "]";
            }
        }

        @Override
        public String visitExecCast(ExecCast expr, Void context) {
            String child = expr.getChild(0).accept(this, context);
            // Skip display for no-op casts (child type matches target type)
            if (expr.getChild(0).getType().matchesType(expr.getType())) {
                return child;
            }
            return "cast(" + child + " as " + expr.getType() + ")";
        }

        @Override
        public String visitExecFunctionCall(ExecFunctionCall expr, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append(expr.getFnName());
            sb.append("[(");
            if (expr.isCountStar()) {
                sb.append("*");
            } else {
                if (expr.isDistinct()) {
                    sb.append("DISTINCT ");
                }
                sb.append(expr.getChildren().stream()
                        .map(c -> c.accept(this, context))
                        .collect(Collectors.joining(", ")));
            }
            sb.append(");");
            if (expr.getFn() != null) {
                sb.append(" args: ");
                com.starrocks.type.Type[] args = expr.getFn().getArgs();
                for (int i = 0; i < args.length; i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append(args[i].getPrimitiveType().toString());
                }
                sb.append(";");
                sb.append(" result: ").append(expr.getType()).append(";");
            }
            sb.append(" args nullable: ").append(hasNullableChildLegacy(expr)).append(";");
            sb.append(" result nullable: ").append(expr.isNullable());
            sb.append("]");
            return sb.toString();
        }

    };

    /**
     * Verbose explain format: recursively uses [slotId: label, TYPE, nullable] for slot refs.
     * Used for VERBOSE and COSTS EXPLAIN levels.
     */
    public static String verboseExplain(ExecExpr expr) {
        return expr.accept(VERBOSE_INSTANCE, null);
    }

    public static String verboseExplainList(java.util.List<? extends ExecExpr> exprs) {
        return exprs.stream()
                .map(ExecExprExplain::verboseExplain)
                .collect(Collectors.joining(", "));
    }

    @Override
    public String visitExecExpr(ExecExpr expr, Void context) {
        return "<unknown-exec-expr>";
    }

    @Override
    public String visitExecSlotRef(ExecSlotRef expr, Void context) {
        if (expr.getLabel() != null) {
            return expr.getLabel();
        }
        return "<slot " + expr.getSlotId() + ">";
    }

    /**
     * Maximum length for string literal display in explain output.
     * Matches the old LargeStringLiteral.LEN_LIMIT behavior: strings longer than this
     * are truncated with "..." appended.
     */
    private static final int LARGE_STRING_LEN_LIMIT = 50;

    @Override
    public String visitExecLiteral(ExecLiteral expr, Void context) {
        ConstantOperator value = expr.getValue();
        if (value.isNull()) {
            return "NULL";
        }
        Type t = expr.getType();
        if (t.isBoolean()) {
            return value.getBoolean() ? "TRUE" : "FALSE";
        } else if (t.isTinyint()) {
            return String.valueOf(value.getTinyInt());
        } else if (t.isSmallint()) {
            return String.valueOf(value.getSmallint());
        } else if (t.isInt()) {
            return String.valueOf(value.getInt());
        } else if (t.isBigint()) {
            return String.valueOf(value.getBigint());
        } else if (t.isLargeint()) {
            return value.getLargeInt().toString();
        } else if (t.isFloatingPointType()) {
            return String.valueOf(value.getDouble());
        } else if (t.isTime()) {
            return String.valueOf(value.getDouble());
        } else if (t.isStringType() || t.isChar() || t.isVarchar()) {
            String s = value.getVarchar();
            s = s.replace("\\", "\\\\");
            s = s.replace("'", "\\'");
            String result = "'" + s + "'";
            // Truncate long strings to match the old LargeStringLiteral explain behavior
            if (result.length() > LARGE_STRING_LEN_LIMIT) {
                return result.substring(0, LARGE_STRING_LEN_LIMIT) + "...'";
            }
            return result;
        } else if (t.isDate()) {
            LocalDateTime dt = value.getDatetime();
            return String.format("'%04d-%02d-%02d'", dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
        } else if (t.isDatetime()) {
            LocalDateTime dt = value.getDatetime();
            long microsecond = dt.getNano() / 1000;
            if (microsecond != 0) {
                return String.format("'%04d-%02d-%02d %02d:%02d:%02d.%06d'",
                        dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                        dt.getHour(), dt.getMinute(), dt.getSecond(), microsecond);
            }
            return String.format("'%04d-%02d-%02d %02d:%02d:%02d'",
                    dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                    dt.getHour(), dt.getMinute(), dt.getSecond());
        } else if (t.isDecimalOfAnyVersion()) {
            return value.getDecimal().toPlainString();
        } else if (t.isBinaryType()) {
            return "X'" + bytesToHex(value.getBinary()) + "'";
        }
        return String.valueOf(value.getValue());
    }

    @Override
    public String visitExecFunctionCall(ExecFunctionCall expr, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append(expr.getFnName()).append("(");
        if (expr.isCountStar()) {
            sb.append("*");
        } else {
            if (expr.isDistinct()) {
                sb.append("DISTINCT ");
            }
            List<ExecExpr> children = expr.getChildren();
            for (int i = 0; i < children.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                String childStr = children.get(i).accept(this, context);
                sb.append(childStr);
                // If this direct child is a lambda with its OWN common expressions
                // and there are more children after it, put them on a new indented line with leading comma
                if (children.get(i) instanceof ExecLambdaFunction
                        && ((ExecLambdaFunction) children.get(i)).getCommonSubOperatorNum() > 0
                        && i < children.size() - 1) {
                    sb.append("\n        ");
                    for (int j = i + 1; j < children.size(); j++) {
                        if (j > i) {
                            sb.append(", ");
                        }
                        sb.append(children.get(j).accept(this, context));
                    }
                    break;
                }
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitExecCast(ExecCast expr, Void context) {
        String child = expr.getChild(0).accept(this, context);
        return "CAST(" + child + " AS " + expr.getType() + ")";
    }

    @Override
    public String visitExecBinaryPredicate(ExecBinaryPredicate expr, Void context) {
        String left = expr.getChild(0).accept(this, context);
        String right = expr.getChild(1).accept(this, context);
        return left + " " + binaryTypeToString(expr.getOp()) + " " + right;
    }

    @Override
    public String visitExecCompoundPredicate(ExecCompoundPredicate expr, Void context) {
        CompoundPredicate.Operator op = expr.getCompoundType();
        if (op == CompoundPredicate.Operator.NOT) {
            return "NOT (" + expr.getChild(0).accept(this, context) + ")";
        }
        String left = expr.getChild(0).accept(this, context);
        String right = expr.getChild(1).accept(this, context);
        return "(" + left + ") " + op.toString() + " (" + right + ")";
    }

    @Override
    public String visitExecInPredicate(ExecInPredicate expr, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append(expr.getChild(0).accept(this, context));
        if (expr.isNotIn()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        sb.append(expr.getChildren().stream()
                .skip(1)
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitExecIsNullPredicate(ExecIsNullPredicate expr, Void context) {
        String child = expr.getChild(0).accept(this, context);
        return child + (expr.isNotNull() ? " IS NOT NULL" : " IS NULL");
    }

    @Override
    public String visitExecLikePredicate(ExecLikePredicate expr, Void context) {
        String child = expr.getChild(0).accept(this, context);
        String pattern = expr.getChild(1).accept(this, context);
        String opName = expr.isRegexp() ? "REGEXP" : "LIKE";
        return child + " " + opName + " " + pattern;
    }

    @Override
    public String visitExecBetweenPredicate(ExecBetweenPredicate expr, Void context) {
        String e = expr.getChild(0).accept(this, context);
        String lower = expr.getChild(1).accept(this, context);
        String upper = expr.getChild(2).accept(this, context);
        String notStr = expr.isNotBetween() ? " NOT" : "";
        return e + notStr + " BETWEEN " + lower + " AND " + upper;
    }

    @Override
    public String visitExecCaseWhen(ExecCaseWhen expr, Void context) {
        StringBuilder sb = new StringBuilder("CASE");
        int childIdx = 0;
        if (expr.hasCase()) {
            sb.append(" ").append(expr.getChild(childIdx++).accept(this, context));
        }
        while (childIdx + 2 <= expr.getNumChildren()) {
            sb.append(" WHEN ").append(expr.getChild(childIdx++).accept(this, context));
            sb.append(" THEN ").append(expr.getChild(childIdx++).accept(this, context));
        }
        if (expr.hasElse()) {
            sb.append(" ELSE ").append(expr.getChild(expr.getNumChildren() - 1).accept(this, context));
        }
        sb.append(" END");
        return sb.toString();
    }

    @Override
    public String visitExecMatchExpr(ExecMatchExpr expr, Void context) {
        String left = expr.getChild(0).accept(this, context);
        String right = expr.getChild(1).accept(this, context);
        return left + " " + matchOpToString(expr.getMatchOp()) + " " + right;
    }

    @Override
    public String visitExecArrayExpr(ExecArrayExpr expr, Void context) {
        return "[" + expr.getChildren().stream()
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(",")) + "]";
    }

    @Override
    public String visitExecMapExpr(ExecMapExpr expr, Void context) {
        StringBuilder sb = new StringBuilder("map{");
        java.util.List<ExecExpr> children = expr.getChildren();
        for (int i = 0; i < children.size(); i += 2) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(children.get(i).accept(this, context));
            sb.append(":");
            sb.append(children.get(i + 1).accept(this, context));
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String visitExecCollectionElement(ExecCollectionElement expr, Void context) {
        String collection = expr.getChild(0).accept(this, context);
        String index = expr.getChild(1).accept(this, context);
        return collection + "[" + index + "]";
    }

    @Override
    public String visitExecArraySlice(ExecArraySlice expr, Void context) {
        String array = expr.getChild(0).accept(this, context);
        String lower = expr.getChild(1).accept(this, context);
        String upper = expr.getChild(2).accept(this, context);
        return array + "[" + lower + ":" + upper + "]";
    }

    @Override
    public String visitExecSubfield(ExecSubfield expr, Void context) {
        String child = expr.getChild(0).accept(this, context);
        return child + "." + String.join(".", expr.getFieldNames()) + "[" + expr.isCopyFlag() + "]";
    }

    @Override
    public String visitExecLambdaFunction(ExecLambdaFunction expr, Void context) {
        // Children layout: [lambdaBody, arg1, arg2, ..., commonSlot1..N, commonExpr1..N]
        // Real argument count = totalChildren - 1(body) - 2*commonSubOperatorNum
        int commonNum = expr.getCommonSubOperatorNum();
        int realChildrenNum = expr.getNumChildren() - 2 * commonNum;
        String body = expr.getChild(0).accept(this, context);

        // Build argument names
        StringBuilder names;
        if (realChildrenNum == 2) {
            names = new StringBuilder(expr.getChild(1).accept(this, context));
        } else {
            names = new StringBuilder("(");
            for (int i = 1; i < realChildrenNum; i++) {
                if (i > 1) {
                    names.append(", ");
                }
                names.append(expr.getChild(i).accept(this, context));
            }
            names.append(")");
        }

        // Build common sub-expression info
        StringBuilder commonSubOp = new StringBuilder();
        if (commonNum > 0) {
            commonSubOp.append("\n        lambda common expressions:");
            for (int i = 0; i < commonNum; i++) {
                ExecExpr slot = expr.getChild(realChildrenNum + i);
                ExecExpr subExpr = expr.getChild(realChildrenNum + commonNum + i);
                commonSubOp.append("{").append(slot.accept(this, context))
                        .append(" <-> ").append(subExpr.accept(this, context)).append("}");
            }
        }

        return String.format("%s -> %s%s", names, body, commonSubOp);
    }

    @Override
    public String visitExecDictMapping(ExecDictMapping expr, Void context) {
        // Match ExprExplainVisitor.visitDictMappingExpr format: DictDecode/DictDefine(slot, [inner])
        String fnName = expr.getType().matchesType(expr.getChild(1).getType()) ? "DictDecode" : "DictDefine";
        if (expr.getNumChildren() == 2) {
            return fnName + "(" + expr.getChild(0).accept(this, context) +
                    ", [" + expr.getChild(1).accept(this, context) + "])";
        }
        return fnName + "(" + expr.getChild(0).accept(this, context) +
                ", [" + expr.getChild(1).accept(this, context) + "], " +
                expr.getChild(2).accept(this, context) + ")";
    }

    @Override
    public String visitExecClone(ExecClone expr, Void context) {
        return "clone(" + expr.getChild(0).accept(this, context) + ")";
    }

    @Override
    public String visitExecDictQuery(ExecDictQuery expr, Void context) {
        return "dict_query(" + expr.getChildren().stream()
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String visitExecDictionaryGet(ExecDictionaryGet expr, Void context) {
        return "dictionary_get(" + expr.getChildren().stream()
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String visitExecPlaceHolder(ExecPlaceHolder expr, Void context) {
        return "<place-holder>";
    }

    @Override
    public String visitExecArithmetic(ExecArithmetic expr, Void context) {
        ArithmeticExpr.Operator op = expr.getOp();
        if (expr.getNumChildren() == 1) {
            return op.toString() + " " + expr.getChild(0).accept(this, context);
        }
        String left = expr.getChild(0).accept(this, context);
        String right = expr.getChild(1).accept(this, context);
        return left + " " + op.toString() + " " + right;
    }

    @Override
    public String visitExecInformationFunction(ExecInformationFunction expr, Void context) {
        return expr.getFuncName() + "()";
    }

    // ---- Helpers ----

    private static String binaryTypeToString(BinaryType op) {
        switch (op) {
            case EQ:
                return "=";
            case NE:
                return "!=";
            case LT:
                return "<";
            case LE:
                return "<=";
            case GT:
                return ">";
            case GE:
                return ">=";
            case EQ_FOR_NULL:
                return "<=>";
            default:
                return op.toString();
        }
    }

    private static String matchOpToString(MatchExpr.MatchOperator op) {
        switch (op) {
            case MATCH:
                return "MATCH";
            case MATCH_ANY:
                return "MATCH_ANY";
            case MATCH_ALL:
                return "MATCH_ALL";
            default:
                return op.toString();
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    /**
     * Compute hasNullableChild using the legacy AST Expr behavior for explain output compatibility.
     * The old FunctionCallExpr.hasNullableChild() checked isMergeAggFn first, then delegated to
     * the base Expr.hasNullableChild() which called isNullable() on each child.
     */
    private static boolean hasNullableChildLegacy(ExecFunctionCall expr) {
        if (expr.isMergeAggFn()) {
            return true;
        }
        for (ExecExpr child : expr.getChildren()) {
            if (isNullableLegacy(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Legacy isNullable: matches the old AST Expr.isNullable() behavior.
     * The old Expr base class returned true by default. Only certain subclasses overrode it:
     * - SlotRef: desc.getIsNullable()
     * - LiteralExpr: this instanceof NullLiteral
     * - ArithmeticExpr: divide/mod=true, else check children
     * - BinaryPredicate: !EQ_FOR_NULL && hasNullableChild()
     * - IsNullPredicate: false
     * - CastExpr: if fully compatible, delegate to child; else true
     * - FunctionCallExpr: fn.isNullable() check
     * Types like CompoundPredicate, InPredicate, CaseExpr, LikePredicate, etc. used the default true.
     */
    private static boolean isNullableLegacy(ExecExpr expr) {
        if (expr instanceof ExecSlotRef) {
            return expr.isNullable();
        }
        if (expr instanceof ExecLiteral) {
            return ((ExecLiteral) expr).getValue().isNull();
        }
        if (expr instanceof ExecArithmetic) {
            ExecArithmetic arith = (ExecArithmetic) expr;
            ArithmeticExpr.Operator op = arith.getOp();
            if (op == ArithmeticExpr.Operator.DIVIDE || op == ArithmeticExpr.Operator.INT_DIVIDE
                    || op == ArithmeticExpr.Operator.MOD) {
                return true;
            }
            return arith.getChildren().stream().anyMatch(c -> isNullableLegacy(c) || c.getType().isDecimalV3());
        }
        if (expr instanceof ExecBinaryPredicate) {
            ExecBinaryPredicate bp = (ExecBinaryPredicate) expr;
            if (bp.getOp() == com.starrocks.sql.ast.expression.BinaryType.EQ_FOR_NULL) {
                return false;
            }
            return expr.getChildren().stream().anyMatch(ExecExprExplain::isNullableLegacy);
        }
        if (expr instanceof ExecIsNullPredicate) {
            return false;
        }
        if (expr instanceof ExecPlaceHolder) {
            return ((ExecPlaceHolder) expr).isNullable();
        }
        if (expr instanceof ExecCast) {
            ExecExpr child = expr.getChild(0);
            if (child.getType().matchesType(expr.getType())) {
                return isNullableLegacy(child);
            }
            return true;
        }
        if (expr instanceof ExecFunctionCall) {
            ExecFunctionCall fn = (ExecFunctionCall) expr;
            if (fn.getFn() != null && !fn.getFn().isNullable()) {
                return false;
            }
            return true;
        }
        // Default: true, matching old Expr.isNullable() default
        return true;
    }
}
