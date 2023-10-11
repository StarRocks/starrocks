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

package com.starrocks.planner.tupledomain;

import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;

import java.util.Objects;
import java.util.Optional;
import javax.validation.constraints.NotNull;

public class ColumnValue implements Comparable<ColumnValue> {
    /**
     * Describes the value of a column in terms of a literal
     * Requires matching types between the column and the literal
     * Used in column domain to form ranges, following comparison between Literals
     * For the same column, column values are expected to have the same type. undefined behavior otherwise.
     * Two special literals will be used: NullLiteral for min and MaxLiteral for max, when tight bounds are unavailable
     */
    private final LiteralExpr value;

    public ColumnValue(LiteralExpr value) {
        this.value = value;
    }

    public static Optional<ColumnValue> create(Column column, LiteralExpr value) {
        if (matchingType(column.getType(), value.getType())) {
            return Optional.of(new ColumnValue(value));
        }
        return Optional.empty();
    }

    public static boolean matchingType(Type columnType, Type literalType) {
        if (columnType.isIntegerType() && columnType.isIntegerType()) {
            return columnType.equals(literalType);
        }
        return (columnType.isBoolean() && literalType.isBoolean())
                || (columnType.isVarchar() && literalType.isStringType());
    }

    public static ColumnValue negativeInfinity() {
        return new ColumnValue(new NullLiteral());
    }

    public static ColumnValue positiveInfinity() {
        return new ColumnValue(MaxLiteral.MAX_VALUE);
    }

    public boolean isNegativeInfinity() {
        return value instanceof NullLiteral;
    }

    public boolean isPositiveInfinity() {
        return value instanceof MaxLiteral;
    }

    public boolean isInfinity() {
        return isNegativeInfinity() || isPositiveInfinity();
    }

    public LiteralExpr getValue() {
        return value;
    }

    @Override
    public int compareTo(ColumnValue other) {
        return value.compareTo(other.value);
    }

    public static ColumnValue MIN_VALUE(@NotNull Type type) {
        try {
            switch (type.getPrimitiveType()) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    return new ColumnValue(IntLiteral.createMinValue(type));
                case LARGEINT:
                    return new ColumnValue(LargeIntLiteral.createMinValue());
                case DATE:
                case DATETIME:
                    return new ColumnValue(DateLiteral.createMinValue(type));
                default:
                    throw new AnalysisException("Invalid data type for min: " + type);
            }
        } catch (AnalysisException e) {
            return negativeInfinity();
        }
    }

    public static ColumnValue MAX_VALUE(@NotNull Type type) {
        try {
            switch (type.getPrimitiveType()) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    return new ColumnValue(IntLiteral.createMaxValue(type));
                case LARGEINT:
                    return new ColumnValue(LargeIntLiteral.createMinValue());
                case DATE:
                case DATETIME:
                    return new ColumnValue(DateLiteral.createMaxValue(type));
                default:
                    throw new AnalysisException("Invalid data type for max: " + type);
            }
        } catch (AnalysisException e) {
            return positiveInfinity();
        }
    }

    @Override
    public String toString() {
        return value.toSql();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnValue that = (ColumnValue) o;
        if (this.isInfinity() || that.isInfinity()) {
            return false;
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
