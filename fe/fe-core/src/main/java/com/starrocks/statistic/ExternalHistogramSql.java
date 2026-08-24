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

package com.starrocks.statistic;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import org.apache.velocity.VelocityContext;

/**
 * The external histogram SQL both collection strategies need. Strategy-specific SQL lives in
 * {@link ExternalHistogramLegacyTarget} and {@link ExternalHistogramBatchedTarget}.
 */
final class ExternalHistogramSql {
    /**
     * The MCV and default-bucket templates scan the whole table - unlike the histogram templates,
     * they carry no sample clause - so their counts are already full-table counts and the shared
     * helpers must not scale them.
     */
    static final double UNSAMPLED_RATIO = 1.0;

    private static final String MCV_STATISTIC_TEMPLATE =
            "select cast(version as INT), " +
                    "cast(column_key as varchar), cast(column_value as varchar) from (" +
                    "select " + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as version, " +
                    "$columnName as column_key, " +
                    "count($columnName) as column_value " +
                    "from `$catalogName`.`$dbName`.`$tableName` where $columnName is not null " +
                    "group by $columnName " +
                    "order by column_value desc limit $topN ) t";

    private ExternalHistogramSql() {
    }

    static String buildMcvQuery(Database database, Table table, String catalogName, Long topN, String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("catalogName", catalogName);
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", topN);

        return StatisticsCollectJob.build(context, MCV_STATISTIC_TEMPLATE);
    }
}
