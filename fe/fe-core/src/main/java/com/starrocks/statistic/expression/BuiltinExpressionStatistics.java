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

package com.starrocks.statistic.expression;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class BuiltinExpressionStatistics {
    private static final ExpressionStatistic UPPER = new ExpressionStatistic() {
        @Override
        public String getName() {
            return "UPPER";
        }

        @Override
        public ExpressionStatsKind getKind() {
            return ExpressionStatsKind.SCALAR;
        }

        @Override
        public boolean appliesTo(Type sourceType) {
            return sourceType.isStringType();
        }

        @Override
        public Type getExpressionType(Type sourceType) {
            if (!(sourceType instanceof ScalarType)) {
                return VarcharType.VARCHAR;
            }
            int length = ((ScalarType) sourceType).getLength();
            return length > 0 ? new VarcharType(length) : VarcharType.VARCHAR;
        }

        @Override
        public String getExpressionSql(String baseColumnName) {
            return "upper(`" + baseColumnName + "`)";
        }

        @Override
        public boolean matches(CallOperator call) {
            return call.getFnName().equalsIgnoreCase(FunctionSet.UPPER) &&
                    call.getChildren().size() == 1 &&
                    call.getChild(0) instanceof ColumnRefOperator;
        }
    };

    private static final ExpressionStatistic UNNEST = new ExpressionStatistic() {
        @Override
        public String getName() {
            return "UNNEST";
        }

        @Override
        public ExpressionStatsKind getKind() {
            return ExpressionStatsKind.TABLE_FUNCTION;
        }

        @Override
        public boolean appliesTo(Type sourceType) {
            if (!sourceType.isArrayType()) {
                return false;
            }
            Type itemType = ((ArrayType) sourceType).getItemType();
            return itemType.canStatistic() && !itemType.isCollectionType();
        }

        @Override
        public Type getExpressionType(Type sourceType) {
            if (!sourceType.isArrayType()) {
                throw new IllegalArgumentException("UNNEST expression statistics only support array columns");
            }
            return ((ArrayType) sourceType).getItemType();
        }

        @Override
        public String getExpressionSql(String baseColumnName) {
            return "unnest(`" + baseColumnName + "`)";
        }

        @Override
        public boolean matches(CallOperator call) {
            return false;
        }

        @Override
        public boolean requiresLateralJoin() {
            return true;
        }
    };

    private static final List<ExpressionStatistic> STATISTICS = ImmutableList.of(UPPER, UNNEST);

    private BuiltinExpressionStatistics() {
    }

    public static List<ExpressionStatistic> getAll() {
        return STATISTICS;
    }

    public static Optional<ExpressionStatistic> findByName(String name) {
        return STATISTICS.stream().filter(statistic -> statistic.getName().equalsIgnoreCase(name)).findFirst();
    }

    public static Optional<ExpressionStatistic> find(CallOperator call) {
        return STATISTICS.stream().filter(statistic -> statistic.matches(call)).findFirst();
    }

    public static List<ExpressionStatistic> getApplicable(Type sourceType) {
        return STATISTICS.stream().filter(statistic -> statistic.appliesTo(sourceType)).collect(Collectors.toList());
    }

    public static Optional<ExpressionStatistic> findTableFunction(String functionName) {
        return findByName(functionName).filter(ExpressionStatistic::requiresLateralJoin);
    }

}
