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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/RoutineLoadDesc.java

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

package com.starrocks.load;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.TableName;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.ImportMetadataStmt;
import com.starrocks.sql.ast.ImportWhereStmt;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.RowDelimiter;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.DictionaryGetExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.ast.expression.VarBinaryLiteral;
import com.starrocks.sql.formatter.AST2StringVisitor;
import com.starrocks.type.AnyMapType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class RoutineLoadDesc {
    private ColumnSeparator columnSeparator;
    private RowDelimiter rowDelimiter;
    private ImportColumnsStmt columnsInfo;
    private ImportWhereStmt wherePredicate;
    // nullable
    private PartitionRef partitionNames;
    // nullable; the INCLUDE METADATA (...) clause for routine load
    private ImportMetadataStmt metadata;

    public RoutineLoadDesc() {
    }

    public RoutineLoadDesc(ColumnSeparator columnSeparator, RowDelimiter rowDelimiter, ImportColumnsStmt columnsInfo,
                           ImportWhereStmt wherePredicate, PartitionRef partitionNames) {
        this.columnSeparator = columnSeparator;
        this.rowDelimiter = rowDelimiter;
        this.columnsInfo = columnsInfo;
        this.wherePredicate = wherePredicate;
        this.partitionNames = partitionNames;
    }

    public ColumnSeparator getColumnSeparator() {
        return columnSeparator;
    }

    public void setColumnSeparator(ColumnSeparator columnSeparator) {
        this.columnSeparator = columnSeparator;
    }

    public RowDelimiter getRowDelimiter() {
        return rowDelimiter;
    }

    public void setRowDelimiter(RowDelimiter rowDelimiter) {
        this.rowDelimiter = rowDelimiter;
    }

    public ImportColumnsStmt getColumnsInfo() {
        return columnsInfo;
    }

    public void setColumnsInfo(ImportColumnsStmt importColumnsStmt) {
        this.columnsInfo = importColumnsStmt;
    }

    public ImportWhereStmt getWherePredicate() {
        return wherePredicate;
    }

    public void setWherePredicate(ImportWhereStmt wherePredicate) {
        this.wherePredicate = wherePredicate;
    }

    // nullable
    public PartitionRef getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(PartitionRef partitionNames) {
        this.partitionNames = partitionNames;
    }

    // nullable
    public ImportMetadataStmt getMetadata() {
        return metadata;
    }

    public void setMetadata(ImportMetadataStmt metadata) {
        this.metadata = metadata;
    }

    /**
     * True when no load property was given. This is what an ALTER ROUTINE LOAD that only changes
     * PROPERTIES or FROM KAFKA(...) produces: the parser hands over an empty load property list and
     * {@link com.starrocks.sql.ast.CreateRoutineLoadStmt#buildLoadDesc} turns it into a desc with every
     * clause null. Applying such a desc must leave the job's load definition and its persisted statement
     * untouched.
     */
    public boolean isEmpty() {
        return columnSeparator == null && rowDelimiter == null && columnsInfo == null
                && wherePredicate == null && partitionNames == null && metadata == null;
    }

    /**
     * Renders a COLUMNS mapping or WHERE expression as SQL that parses back to the same tree.
     * <p>
     * Both consumers of this output are re-parsed later: the statement persisted by
     * {@code RoutineLoadJob#mergeLoadDescToOriginStatement} is re-parsed when the FE loads the job from
     * an image, and SHOW CREATE ROUTINE LOAD is meant to be run again as DDL. The printer therefore has to
     * be round-trip safe, and {@link com.starrocks.sql.ast.expression.ExprToSql#toSql} is not: it is an
     * EXPLAIN-oriented printer that joins arithmetic operands without parentheses, so
     * {@code floor((ts + 32400) / 86400)} came back as {@code floor(ts + 32400 / 86400)}. The parser does not
     * keep the user's parentheses in the AST either (they are unwrapped in
     * {@code AstBuilder#visitParenthesizedExpression}), so precedence has to be restored by the printer.
     * {@link AST2StringVisitor} does that by parenthesizing every operand that is not a column or a literal;
     * generated columns are persisted the same way (see {@code ColumnIdExpr}).
     * <p>
     * {@code AstToSQLBuilder.toSQL} (AST2SQLVisitor) is not an option either: it expects an analyzed tree (its
     * SlotRef printer dereferences the resolved type, and its ARRAY / MAP printers call
     * {@code AnalyzerUtils.replaceNullType2Boolean} on the element type, which is null for an untyped
     * {@code [1, 2]}), while routine load expressions are only analyzed when a task is planned.
     */
    public static String exprToSql(Expr expr) {
        return new LoadExprSerializer().visit(expr);
    }

    /**
     * Compares the expression-bearing clauses of two descs structurally: the COLUMNS mappings and the
     * WHERE predicate. The remaining clauses (separators, partitions, INCLUDE METADATA) are plain values
     * rendered by their own {@code toSql()}, so they are not part of this check. Used to verify that a
     * regenerated statement still describes the same load before it is persisted.
     */
    @VisibleForTesting
    public boolean hasSameExpressions(RoutineLoadDesc other) {
        return findExpressionDifference(other).isEmpty();
    }

    /**
     * The first difference {@link #hasSameExpressions} would fail on, as text for an error message, or empty when
     * the two descs carry the same COLUMNS / WHERE expressions. Expressions are compared tree by tree (see
     * {@link #sameExpression}). A printer that drops parentheses produces output that is stable under
     * re-printing and so cannot be caught by comparing strings.
     */
    public Optional<String> findExpressionDifference(RoutineLoadDesc other) {
        if ((columnsInfo == null) != (other.columnsInfo == null)) {
            return Optional.of("COLUMNS clause present on one side only");
        }
        if (columnsInfo != null) {
            List<ImportColumnDesc> columns = columnsInfo.getColumns();
            List<ImportColumnDesc> otherColumns = other.columnsInfo.getColumns();
            if (columns.size() != otherColumns.size()) {
                return Optional.of("COLUMNS has " + columns.size() + " vs " + otherColumns.size() + " entries");
            }
            for (int i = 0; i < columns.size(); i++) {
                ImportColumnDesc column = columns.get(i);
                ImportColumnDesc otherColumn = otherColumns.get(i);
                if (!column.getColumnName().equalsIgnoreCase(otherColumn.getColumnName())) {
                    return Optional.of("COLUMNS entry " + (i + 1) + " is " + column.getColumnName()
                            + " vs " + otherColumn.getColumnName());
                }
                if (!sameExpression(column.getExpr(), otherColumn.getExpr())) {
                    return Optional.of("COLUMNS " + column.getColumnName() + " = "
                            + describeDifference(column.getExpr(), otherColumn.getExpr()));
                }
            }
        }
        if ((wherePredicate == null) != (other.wherePredicate == null)) {
            return Optional.of("WHERE clause present on one side only");
        }
        if (wherePredicate != null && !sameExpression(wherePredicate.getExpr(), other.wherePredicate.getExpr())) {
            return Optional.of("WHERE " + describeDifference(wherePredicate.getExpr(), other.wherePredicate.getExpr()));
        }
        return Optional.empty();
    }

    /**
     * Both renderings and, when they read the same (a literal that another sql_mode parses as a different
     * type, for instance), the kinds of the first nodes that differ.
     */
    private static String describeDifference(Expr left, Expr right) {
        String rendering = describe(left) + " vs " + describe(right);
        if (left == null || right == null || !describe(left).equals(describe(right))) {
            return rendering;
        }
        Expr[] differing = firstDifferingNodes(left, right);
        return rendering + " (" + differing[0].getClass().getSimpleName() + " vs "
                + differing[1].getClass().getSimpleName() + ")";
    }

    private static Expr[] firstDifferingNodes(Expr left, Expr right) {
        if (left.getChildren().size() == right.getChildren().size()) {
            for (int i = 0; i < left.getChildren().size(); i++) {
                if (!sameExpression(left.getChild(i), right.getChild(i))) {
                    return firstDifferingNodes(left.getChild(i), right.getChild(i));
                }
            }
        }
        return new Expr[] {left, right};
    }

    /**
     * {@code Expr.equals} up to the case of function names, plus the declared ARRAY / MAP types. The parser
     * keeps function names as the user wrote them (FROM_UNIXTIME), the printer lower-cases them, and
     * {@code FunctionCallExpr.equals} compares them case-sensitively, so without the normalization a tree
     * would never equal its own re-parsed rendering.
     */
    @VisibleForTesting
    static boolean sameExpression(Expr left, Expr right) {
        if (left == null || right == null) {
            return left == right;
        }
        return withLowerCaseFunctionNames(left).equals(withLowerCaseFunctionNames(right))
                && sameCollectionTypes(left, right);
    }

    private static Expr withLowerCaseFunctionNames(Expr expr) {
        Expr copy = expr.clone();
        lowerCaseFunctionNames(copy);
        return copy;
    }

    private static void lowerCaseFunctionNames(Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) expr;
            // getFunctionName() is the lower-cased last part of the name; the db prefix, if any, is kept.
            call.resetFnName(call.getDbName(), call.getFunctionName());
        }
        for (Expr child : expr.getChildren()) {
            lowerCaseFunctionNames(child);
        }
    }

    private static boolean sameCollectionTypes(Expr left, Expr right) {
        if (left == null) {
            return true;
        }
        // Expr.equals deliberately ignores types before analysis. For collection constructors the
        // declared type is already meaningful, including nested element types and typed vs. untyped.
        if ((left instanceof ArrayExpr || left instanceof MapExpr) && !Objects.equals(left.getType(), right.getType())) {
            return false;
        }
        for (int i = 0; i < left.getChildren().size(); i++) {
            if (!sameCollectionTypes(left.getChild(i), right.getChild(i))) {
                return false;
            }
        }
        return true;
    }

    private static String describe(Expr expr) {
        return expr == null ? "<none>" : exprToSql(expr);
    }

    public String toSql() {
        List<String> subSQLs = new ArrayList<>();
        if (columnSeparator != null) {
            subSQLs.add("COLUMNS TERMINATED BY " + columnSeparator.toSql());
        }
        if (rowDelimiter != null) {
            subSQLs.add("ROWS TERMINATED BY " + rowDelimiter.toSql());
        }
        if (metadata != null && metadata.getItems() != null && !metadata.getItems().isEmpty()) {
            subSQLs.add(metadata.toSql());
        }
        if (columnsInfo != null) {
            String subSQL = "COLUMNS(" +
                    columnsInfo.getColumns().stream().map(this::columnToString)
                            .collect(Collectors.joining(", ")) +
                    ")";
            subSQLs.add(subSQL);
        }
        if (partitionNames != null) {
            String subSQL = null;
            if (partitionNames.isTemp()) {
                subSQL = "TEMPORARY PARTITION";
            } else {
                subSQL = "PARTITION";
            }
            subSQL += "(" + partitionNames.getPartitionNames().stream().map(this::pack)
                    .collect(Collectors.joining(", "))
                    + ")";
            subSQLs.add(subSQL);
        }
        if (wherePredicate != null) {
            subSQLs.add("WHERE " + exprToSql(wherePredicate.getExpr()));
        }
        return String.join(", ", subSQLs);
    }

    private String pack(String str) {
        return ParseUtil.backquote(str);
    }

    public String columnToString(ImportColumnDesc desc) {
        String str = pack(desc.getColumnName());
        if (desc.getExpr() != null) {
            str += " = " + exprToSql(desc.getExpr());
        }
        return str;
    }

    @Override
    public String toString() {
        return toSql();
    }

    /**
     * Prints an unanalyzed COLUMNS / WHERE expression so that it parses back to the same tree. Column references
     * are printed from what the user wrote, never from an analyzed slot, and always backquoted; literals that
     * carry a type keyword keep it; function calls and casts are not wrapped in extra parentheses because they
     * delimit themselves. {@code ColumnIdExpr} persists generated columns with the same kind of visitor.
     */
    private static class LoadExprSerializer extends AST2StringVisitor {
        @Override
        public String visitSlot(SlotRef node, Void context) {
            QualifiedName qualifiedName = node.getQualifiedName();
            if (qualifiedName != null) {
                // Part by part as written: this keeps an explicit default_catalog prefix, which TableName.toSql()
                // would drop, and prints a deep struct path (kept by SlotRef as one dotted name) correctly.
                return qualifiedName.getParts().stream().map(ParseUtil::backquote).collect(Collectors.joining("."));
            }
            String column = ParseUtil.backquote(node.getColumnName());
            TableName tableName = node.getTblNameWithoutAnalyzed();
            return tableName == null ? column : tableName.toSql() + "." + column;
        }

        @Override
        protected String printWithParentheses(ParseNode node) {
            if (node instanceof FunctionCallExpr || (node instanceof CastExpr && !((CastExpr) node).isImplicit())) {
                return visit(node);
            }
            return super.printWithParentheses(node);
        }

        @Override
        public String visitTimestampArithmeticExpr(TimestampArithmeticExpr node, Void context) {
            if (node.getFuncName() != null) {
                return super.visitTimestampArithmeticExpr(node, context);
            }
            // In particular, INTERVAL 1 DAY + (ts + INTERVAL 1 MONTH) must not change the order of
            // the additions: calendar-month arithmetic is not associative around month boundaries.
            String timestamp = printWithParentheses(node.getChild(0));
            String interval = "INTERVAL " + visit(node.getChild(1)) + " " + node.getTimeUnitIdent();
            return node.isIntervalFirst() ? interval + " " + node.getOp() + " " + timestamp
                    : timestamp + " " + node.getOp() + " " + interval;
        }

        @Override
        public String visitDecimalLiteral(DecimalLiteral node, Void context) {
            String value = node.getValue().toPlainString();
            if (node.getValue().scale() != 0) {
                return value;
            }
            // Integers beyond LARGEINT already parse as decimals, including with MODE_DOUBLE_LITERAL.
            // Scientific notation (E0) instead parses them as DOUBLE and loses DECIMAL256 precision.
            if (node.getValue().toBigInteger().abs().compareTo(LargeIntLiteral.LARGE_INT_MAX_ABS) > 0) {
                return value;
            }
            // Smaller scale-zero decimals need the dot to avoid re-parsing as INT or LARGEINT.
            return value + ".";
        }

        @Override
        public String visitCastExpr(CastExpr node, Void context) {
            if (node.isImplicit()) {
                return visit(node.getChild(0));
            }
            Type type = node.getTargetTypeDef() == null ? node.getType() : node.getTargetTypeDef().getType();
            return "CAST(" + printWithParentheses(node.getChild(0)) + " AS " + typeToSql(type) + ")";
        }

        private String typeToSql(Type type) {
            // Neither Type.toString() nor Type.toSql() distinguishes DECIMALV2 from DECIMAL: both print it as
            // decimal(p, s), which re-parses as decimal v3 by default. Recurse through collection and struct
            // types so nested explicit types survive.
            if (type.isDecimalV2()) {
                ScalarType decimal = (ScalarType) type;
                return "DECIMALV2(" + decimal.getScalarPrecision() + "," + decimal.getScalarScale() + ")";
            }
            if (type instanceof ArrayType array) {
                return "ARRAY<" + typeToSql(array.getItemType()) + ">";
            }
            if (type instanceof MapType map) {
                return "MAP<" + typeToSql(map.getKeyType()) + "," + typeToSql(map.getValueType()) + ">";
            }
            if (type instanceof StructType struct) {
                return "STRUCT<" + struct.getFields().stream()
                        .map(field -> ParseUtil.backquote(field.getName()) + " " + typeToSql(field.getType())
                                + (field.getComment() == null ? "" : " COMMENT "
                                + visit(new StringLiteral(field.getComment()))))
                        .collect(Collectors.joining(", ")) + ">";
            }
            return type.toString();
        }

        @Override
        public String visitDateLiteral(DateLiteral node, Void context) {
            // AST2StringVisitor prints these as plain strings, which re-parse as StringLiteral.
            return (node.getType().isDate() ? "DATE '" : "DATETIME '") + node.getStringValue() + "'";
        }

        @Override
        public String visitVarBinaryLiteral(VarBinaryLiteral node, Void context) {
            return "x'" + node.getStringValue() + "'";
        }

        @Override
        public String visitDictionaryGetExpr(DictionaryGetExpr node, Void context) {
            // AST2StringVisitor hands this one to the EXPLAIN printer, which appends the analysis-time
            // null_if_not_exist flag (always false before analysis) and drops parentheses in the arguments.
            return "dictionary_get("
                    + node.getChildren().stream().map(this::visit).collect(Collectors.joining(", ")) + ")";
        }

        @Override
        public String visitArrayExpr(ArrayExpr node, Void context) {
            // Keep the element type the user wrote (ARRAY<BIGINT>[1, 2]); an untyped literal has no type yet.
            Type type = node.getType();
            return (type == null ? "" : typeToSql(type)) + super.visitArrayExpr(node, context);
        }

        @Override
        public String visitMapExpr(MapExpr node, Void context) {
            Type type = node.getType();
            StringBuilder sb = new StringBuilder(type == null || type instanceof AnyMapType ? "map" : typeToSql(type));
            sb.append("{");
            for (int i = 0; i < node.getChildren().size(); i += 2) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(visit(node.getChild(i))).append(":").append(visit(node.getChild(i + 1)));
            }
            return sb.append("}").toString();
        }
    }
}
