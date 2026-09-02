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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.Column;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Converts safe partition predicates to the filter syntax accepted by Hive Metastore. */
public final class HivePartitionFilterConverter {
    private static final Pattern HIVE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private HivePartitionFilterConverter() {
    }

    public static Optional<Result> convert(LogicalScanOperator operator, List<Column> partitionColumns,
                                           ScalarOperator predicate) {
        if (predicate == null || partitionColumns.isEmpty()) {
            return Optional.empty();
        }

        Map<ColumnRefOperator, Column> partitionColumnMap = new HashMap<>();
        for (Column partitionColumn : partitionColumns) {
            partitionColumnMap.put(operator.getColumnReference(partitionColumn), partitionColumn);
        }
        return convert(predicate, partitionColumnMap);
    }

    static Optional<Result> convert(ScalarOperator predicate, Map<ColumnRefOperator, Column> partitionColumnMap) {
        return convertPredicate(predicate, partitionColumnMap)
                .map(converted -> new Result(converted.filter, converted.requiresFilterApi));
    }

    private static Optional<ConvertedPredicate> convertPredicate(
            ScalarOperator predicate, Map<ColumnRefOperator, Column> partitionColumnMap) {
        if (predicate instanceof BinaryPredicateOperator) {
            return convertBinaryPredicate((BinaryPredicateOperator) predicate, partitionColumnMap);
        }
        if (predicate instanceof InPredicateOperator) {
            return convertInPredicate((InPredicateOperator) predicate, partitionColumnMap);
        }
        if (predicate instanceof CompoundPredicateOperator) {
            return convertCompoundPredicate((CompoundPredicateOperator) predicate, partitionColumnMap);
        }
        return Optional.empty();
    }

    private static Optional<ConvertedPredicate> convertBinaryPredicate(
            BinaryPredicateOperator predicate, Map<ColumnRefOperator, Column> partitionColumnMap) {
        ScalarOperator left = predicate.getChild(0);
        ScalarOperator right = predicate.getChild(1);
        BinaryType binaryType = predicate.getBinaryType();

        ColumnRefOperator columnRef;
        ConstantOperator constant;
        boolean columnOnLeft;
        if (left instanceof ColumnRefOperator && right instanceof ConstantOperator) {
            columnRef = (ColumnRefOperator) left;
            constant = (ConstantOperator) right;
            columnOnLeft = true;
        } else if (left instanceof ConstantOperator && right instanceof ColumnRefOperator) {
            columnRef = (ColumnRefOperator) right;
            constant = (ConstantOperator) left;
            binaryType = binaryType.commutative();
            columnOnLeft = false;
        } else {
            return Optional.empty();
        }

        Column column = partitionColumnMap.get(columnRef);
        if (column == null || constant.isNull() || !isSupportedIdentifier(column.getName())) {
            return Optional.empty();
        }

        Optional<String> literal = formatLiteral(column.getType(), constant);
        Optional<String> operator = formatOperator(binaryType);
        if (literal.isEmpty() || operator.isEmpty()) {
            return Optional.empty();
        }

        boolean requiresFilterApi = binaryType != BinaryType.EQ || !column.getType().isStringType() || !columnOnLeft;
        return Optional.of(new ConvertedPredicate(
                column.getName() + " " + operator.get() + " " + literal.get(), requiresFilterApi));
    }

    private static Optional<ConvertedPredicate> convertInPredicate(
            InPredicateOperator predicate, Map<ColumnRefOperator, Column> partitionColumnMap) {
        if (predicate.isNotIn() || !(predicate.getChild(0) instanceof ColumnRefOperator)) {
            return Optional.empty();
        }

        Column column = partitionColumnMap.get((ColumnRefOperator) predicate.getChild(0));
        if (column == null || !isSupportedIdentifier(column.getName())) {
            return Optional.empty();
        }

        List<String> literals = new ArrayList<>();
        for (ScalarOperator child : predicate.getListChildren()) {
            if (!(child instanceof ConstantOperator) || ((ConstantOperator) child).isNull()) {
                return Optional.empty();
            }
            Optional<String> literal = formatLiteral(column.getType(), (ConstantOperator) child);
            if (literal.isEmpty()) {
                return Optional.empty();
            }
            literals.add(literal.get());
        }
        if (literals.isEmpty()) {
            return Optional.empty();
        }

        String filter = literals.stream()
                .map(literal -> column.getName() + " = " + literal)
                .collect(Collectors.joining(" or ", "(", ")"));
        return Optional.of(new ConvertedPredicate(filter, true));
    }

    private static Optional<ConvertedPredicate> convertCompoundPredicate(
            CompoundPredicateOperator predicate, Map<ColumnRefOperator, Column> partitionColumnMap) {
        if (predicate.isNot()) {
            return Optional.empty();
        }

        List<ConvertedPredicate> convertedChildren = predicate.getChildren().stream()
                .map(child -> convertPredicate(child, partitionColumnMap))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        if (predicate.isOr() && convertedChildren.size() != predicate.getChildren().size()) {
            return Optional.empty();
        }
        if (convertedChildren.isEmpty()) {
            return Optional.empty();
        }

        String separator = predicate.isAnd() ? " and " : " or ";
        String filter = convertedChildren.stream()
                .map(converted -> converted.filter)
                .collect(Collectors.joining(separator, "(", ")"));
        boolean requiresFilterApi = predicate.isOr() || convertedChildren.stream()
                .anyMatch(converted -> converted.requiresFilterApi);
        return Optional.of(new ConvertedPredicate(filter, requiresFilterApi));
    }

    private static Optional<String> formatLiteral(Type partitionType, ConstantOperator constant) {
        if (partitionType.isStringType() || partitionType.isDate()) {
            return quoteStringLiteral(constant.toString());
        }
        if (partitionType.isIntegerType() || partitionType.isLargeIntType()) {
            return Optional.of(constant.toString());
        }
        return Optional.empty();
    }

    private static Optional<String> quoteStringLiteral(String value) {
        // HMS filter literals use backslashes as escape characters. Falling back to FE pruning is safer than
        // generating a filter that may represent a different partition value.
        if (value.contains("\\")) {
            return Optional.empty();
        }
        if (!value.contains("\"")) {
            return Optional.of("\"" + value + "\"");
        }
        if (!value.contains("'")) {
            return Optional.of("'" + value + "'");
        }
        return Optional.empty();
    }

    private static Optional<String> formatOperator(BinaryType binaryType) {
        switch (binaryType) {
            case EQ:
                return Optional.of("=");
            case NE:
                return Optional.of("!=");
            case LT:
                return Optional.of("<");
            case LE:
                return Optional.of("<=");
            case GT:
                return Optional.of(">");
            case GE:
                return Optional.of(">=");
            default:
                return Optional.empty();
        }
    }

    private static boolean isSupportedIdentifier(String name) {
        return HIVE_IDENTIFIER.matcher(name).matches();
    }

    public static final class Result {
        private final String filter;
        private final boolean requiresFilterApi;

        private Result(String filter, boolean requiresFilterApi) {
            this.filter = filter;
            this.requiresFilterApi = requiresFilterApi;
        }

        public String getFilter() {
            return filter;
        }

        public boolean requiresFilterApi() {
            return requiresFilterApi;
        }
    }

    private static final class ConvertedPredicate {
        private final String filter;
        private final boolean requiresFilterApi;

        private ConvertedPredicate(String filter, boolean requiresFilterApi) {
            this.filter = filter;
            this.requiresFilterApi = requiresFilterApi;
        }
    }
}
