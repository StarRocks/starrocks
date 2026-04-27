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

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.statistic.ExpressionStatsMeta;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.type.Type;

import java.time.LocalDateTime;
import java.util.List;

public interface ExpressionStatistic {
    /**
     * Stable identifier used by normalized expression keys and persisted expression stats metadata.
     */
    String getName();

    ExpressionStatsKind getKind();

    boolean appliesTo(Type sourceType);

    Type getExpressionType(Type sourceType);

    /**
     * SQL expression evaluated during collection. For row-expanding expressions this is the table function call.
     */
    String getExpressionSql(String baseColumnName);

    /**
     * Whether a scalar expression in optimizer can use this statistic.
     */
    boolean matches(CallOperator call);

    default boolean requiresLateralJoin() {
        return getKind() == ExpressionStatsKind.TABLE_FUNCTION;
    }

    default String getValueColumnName(String baseColumnName) {
        return getName().toLowerCase() + "_" + baseColumnName + "_value";
    }

    default ExpressionStatsKey getKey(Column baseColumn) {
        return ExpressionStatsKey.create(getKind() + "(" + getName().toLowerCase() + ",COLUMN_ID(" +
                baseColumn.getUniqueId() + "))");
    }

    default String getStorageColumnName(Column baseColumn) {
        return getKey(baseColumn).getStorageColumnName();
    }

    default ExpressionStatsMeta createMeta(Column baseColumn, StatsConstants.AnalyzeType analyzeType,
                                           LocalDateTime updateTime) {
        return new ExpressionStatsMeta(getKey(baseColumn), getKind(), getName(), getExpressionSql(baseColumn.getName()),
                List.of(baseColumn.getUniqueId()), analyzeType, updateTime);
    }
}
