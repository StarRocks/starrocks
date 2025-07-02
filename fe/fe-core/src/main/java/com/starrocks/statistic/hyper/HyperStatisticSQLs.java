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

package com.starrocks.statistic.hyper;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.base.ColumnStats;
import com.starrocks.statistic.base.PartitionSampler;
import com.starrocks.statistic.sample.SampleInfo;
import com.starrocks.statistic.sample.TabletStats;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.starrocks.statistic.base.PartitionSampler.HIGH_WEIGHT_READ_RATIO;
import static com.starrocks.statistic.base.PartitionSampler.LOW_WEIGHT_READ_RATIO;
import static com.starrocks.statistic.base.PartitionSampler.MEDIUM_HIGH_WEIGHT_READ_RATIO;
import static com.starrocks.statistic.base.PartitionSampler.MEDIUM_LOW_WEIGHT_READ_RATIO;

public class HyperStatisticSQLs {
    private static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    //| table_id       | bigint           | NO   | true  | <null>  |       |
    //| partition_id   | bigint           | NO   | true  | <null>  |       |
    //| column_name    | varchar(65530)   | NO   | true  | <null>  |       |
    //| db_id          | bigint           | NO   | false | <null>  |       |
    //| table_name     | varchar(65530)   | NO   | false | <null>  |       |
    //| partition_name | varchar(65530)   | NO   | false | <null>  |       |
    //| row_count      | bigint           | NO   | false | <null>  |       |
    //| data_size      | bigint           | NO   | false | <null>  |       |
    //| ndv            | hll              | NO   | false |         |       |
    //| null_count     | bigint           | NO   | false | <null>  |       |
    //| max            | varchar(1048576) | NO   | false | <null>  |       |
    //| min            | varchar(1048576) | NO   | false | <null>  |       |
    //| update_time    | datetime         | NO   | false | <null>  |       |
    //| collection_size| bigint           | NO   | false | <null>  |       |
    public static final String BATCH_FULL_STATISTIC_TEMPLATE = "SELECT cast($version as INT)" +
            ", cast($partitionId as BIGINT)" + // BIGINT
            ", '$columnNameStr'" + // VARCHAR
            ", cast(COUNT(1) as BIGINT)" + // BIGINT
            ", cast($dataSize as BIGINT)" + // BIGINT
            ", $hllFunction" + // VARBINARY
            ", cast($countNullFunction as BIGINT)" + // BIGINT
            ", $maxFunction" + // VARCHAR
            ", $minFunction" + // VARCHAR
            ", cast($collectionSizeFunction as BIGINT)" + // BIGINT
            " FROM `$dbName`.`$tableName` partition `$partitionName`";

    public static final String BATCH_META_STATISTIC_TEMPLATE = "SELECT cast($version as INT)" +
            ", cast($partitionId as BIGINT)" + // BIGINT, partition_id
            ", '$columnNameStr'" + // VARCHAR, column_name
            ", cast(COUNT(*) as BIGINT)" + // BIGINT, row_count
            ", cast(0 as BIGINT)" + // BIGINT, data_size
            ", '00'" + // VARBINARY, ndv
            ", cast(0 as BIGINT)" + // BIGINT, null_count
            ", $maxFunction" + // VARCHAR, max
            ", $minFunction " + // VARCHAR, min
            ", cast(-1 as BIGINT) " + // BIGINT, collection_size
            " FROM `$dbName`.`$tableName` partitions(`$partitionName`) [_META_]";

    public static final String BATCH_DATA_STATISTIC_SELECT_TEMPLATE = "SELECT cast($version as INT)" +
            ", cast($partitionId as BIGINT)" + // BIGINT, partition_id
            ", '$columnNameStr'" + // VARCHAR, column_name
            ", cast(0 as BIGINT)" + // BIGINT, row_count
            ", cast($dataSize as BIGINT)" + // BIGINT, data_size
            ", $hllFunction" + // VARBINARY, ndv
            ", cast($countNullFunction as BIGINT)" + // BIGINT, null_count
            ", ''" + // VARCHAR, max
            ", ''" + // VARCHAR, min
            ", cast($collectionSizeFunction as BIGINT)" + // BIGINT, collection_size
            " FROM base_cte_table ";

    public static final String BATCH_SAMPLE_STATISTIC_SELECT_TEMPLATE = "SELECT cast($version as INT)" +
            ", cast($partitionId as BIGINT)" + // BIGINT
            ", '$columnNameStr'" + // VARCHAR
            ", cast($rowCount as BIGINT)" + // BIGINT
            ", cast($dataSize as BIGINT)" + // BIGINT
            ", $hllFunction" + // VARBINARY
            ", cast($countNullFunction as BIGINT)" + // BIGINT
            ", $maxFunction" + // VARCHAR
            ", $minFunction" + // VARCHAR
            ", cast($collectionSizeFunction as BIGINT)" + // BIGINT, collection_size
            " FROM base_cte_table ";

    public static final String FULL_MULTI_COLUMN_STATISTICS_SELECT_TEMPLATE =
            "SELECT cast($version as INT), $columnIdsStr, cast($ndvFunction as BIGINT) from `$dbName`.`$tableName`";

    public static final String SAMPLE_MULTI_COLUMN_STATISTICS_SELECT_TMEPLATE = "$base_cte_table " +
            "SELECT\n" +
            "    cast($version as INT),\n" +
            "    $columnIdsStr,\n" +
            "    cast($ndvFunction as BIGINT)\n" +
            "FROM (\n" +
            "    SELECT\n" +
            "        t0.`column_key`,\n" +
            "        COUNT(1) as count\n" +
            "    FROM (\n" +
            "        SELECT\n" +
            "            combined_column_key AS column_key\n" +
            "        FROM\n" +
            "            `base_cte_table`\n" +
            "    ) as t0\n" +
            "    GROUP BY t0.column_key \n" +
            ") AS t1;";

    public static String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }

    public static VelocityContext buildBaseContext(Database db, Table table, Partition p, ColumnStats stats) {
        VelocityContext context = new VelocityContext();
        String columnNameStr = stats.getColumnNameStr();
        String quoteColumnName = stats.getQuotedColumnName();
        context.put("version", StatsConstants.STATISTIC_BATCH_VERSION_V5);
        context.put("partitionId", p.getId());
        context.put("columnNameStr", columnNameStr);
        context.put("partitionName", p.getName());
        context.put("dbName", db.getOriginName());
        context.put("tableName", table.getName());
        context.put("quoteColumnName", quoteColumnName);
        return context;
    }

    public static String buildSampleSQL(Database db, Table table, Partition p, List<ColumnStats> stats,
                                        PartitionSampler sampler, String template) {
        String tableName = "`" + db.getOriginName() + "`.`" + table.getName() + "`";

        SampleInfo info = sampler.getSampleInfo(p.getId());
        List<String> groupSQLs = Lists.newArrayList();
        StringBuilder sqlBuilder = new StringBuilder();
        groupSQLs.add(generateRatioTable(tableName, info.getHighWeightTablets(),
                HIGH_WEIGHT_READ_RATIO, "t_high"));
        groupSQLs.add(generateRatioTable(tableName, info.getMediumHighWeightTablets(),
                MEDIUM_HIGH_WEIGHT_READ_RATIO, "t_medium_high"));
        groupSQLs.add(generateRatioTable(tableName, info.getMediumLowWeightTablets(),
                MEDIUM_LOW_WEIGHT_READ_RATIO, "t_medium_low"));
        groupSQLs.add(generateRatioTable(tableName, info.getLowWeightTablets(),
                LOW_WEIGHT_READ_RATIO, "t_low"));
        if (groupSQLs.stream().allMatch(Objects::isNull)) {
            groupSQLs.add("SELECT * FROM " + tableName + " LIMIT " + Config.statistic_sample_collect_rows);
        }

        sqlBuilder.append("with base_cte_table as (");
        sqlBuilder.append(groupSQLs.stream().filter(Objects::nonNull).collect(Collectors.joining(" UNION ALL ")));
        sqlBuilder.append(") ");
        groupSQLs.clear();

        for (ColumnStats stat : stats) {
            VelocityContext context = buildBaseContext(db, table, p, stat);
            context.put("rowCount", info.getTotalRowCount());
            context.put("dataSize", stat.getSampleDateSize(info));
            context.put("hllFunction", stat.getNDV());
            context.put("countNullFunction", stat.getSampleNullCount(info));
            context.put("maxFunction", stat.getMax());
            context.put("minFunction", stat.getMin());
            context.put("collectionSizeFunction", stat.getCollectionSize());
            groupSQLs.add(HyperStatisticSQLs.build(context, template));
        }
        sqlBuilder.append(String.join(" UNION ALL ", groupSQLs));
        return sqlBuilder.toString();
    }

    private static String generateRatioTable(String table, List<TabletStats> tablets, double ratio, String alias) {
        if (tablets.isEmpty()) {
            return null;
        }

        String tabletHint =
                tablets.stream().map(t -> String.valueOf(t.getTabletId())).collect(Collectors.joining(", "));
        if (!Config.enable_use_table_sample_collect_statistics) {
            return String.format(" SELECT * FROM (SELECT * " +
                            " FROM %s tablet(%s) " +
                            " WHERE rand() <= %f " +
                            " ) %s",
                    table, tabletHint, ratio, alias);
        } else {
            int percent = (int) (ratio * 100);
            return String.format(" SELECT * FROM (SELECT * FROM %s TABLET(%s) SAMPLE('percent'='%d')) %s",
                    table, tabletHint, percent, alias);
        }
    }
}
