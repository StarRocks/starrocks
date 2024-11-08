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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.base.ColumnStats;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;

public class StatisticSQLs {
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
    public static final String TABLE_NAME = "column_statistics";
    public static final String BATCH_FULL_STATISTIC_TEMPLATE = "SELECT cast($version as INT)" +
            ", cast($partitionId as BIGINT)" + // BIGINT
            ", '$columnNameStr'" + // VARCHAR
            ", cast(COUNT(1) as BIGINT)" + // BIGINT
            ", cast($dataSize as BIGINT)" + // BIGINT
            ", $hllFunction" + // VARBINARY
            ", cast($countNullFunction as BIGINT)" + // BIGINT
            ", $maxFunction" + // VARCHAR
            ", $minFunction " + // VARCHAR
            " FROM `$dbName`.`$tableName` partition `$partitionName`";

    public static final String BATCH_META_STATISTIC_TEMPLATE = "SELECT cast($version as INT)" +
            ", cast($partitionId as BIGINT)" + // BIGINT, partition_id
            ", '$columnNameStr'" + // VARCHAR, column_name
            ", cast(COUNT(*) as BIGINT)" + // BIGINT, row_count
            ", cast($dataSize as BIGINT)" + // BIGINT, data_size
            ", '00'" + // VARBINARY, ndv
            ", cast($countNullFunction as BIGINT)" + // BIGINT, null_count
            ", $maxFunction" + // VARCHAR, max
            ", $minFunction " + // VARCHAR, min
            " FROM `$dbName`.`$tableName` partitions(`$partitionName`) [_META_]";

    public static final String BATCH_NDV_STATISTIC_TEMPLATE = "SELECT cast($version as INT)" +
            ", cast($partitionId as BIGINT)" + // BIGINT, partition_id
            ", '$columnNameStr'" + // VARCHAR, column_name
            ", cast(0 as BIGINT)" + // BIGINT, row_count
            ", cast(0 as BIGINT)" + // BIGINT, data_size
            ", $hllFunction" + // VARBINARY, ndv
            ", cast(0 as BIGINT)" + // BIGINT, null_count
            ", ''" + // VARCHAR, max
            ", '' " + // VARCHAR, min
            " FROM `$dbName`.`$tableName` partitions(`$partitionName`)";

    public static String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }

    public static String buildFullSQL(Database db, Table table, Partition p, ColumnStats stats, String template) {
        StringBuilder builder = new StringBuilder();
        VelocityContext context = new VelocityContext();

        String columnNameStr = stats.getColumnNameStr();
        String quoteColumnName = stats.getQuotedColumnName();
        context.put("version", StatsConstants.STATISTIC_BATCH_VERSION);
        context.put("partitionId", p.getId());
        context.put("columnNameStr", columnNameStr);
        context.put("dataSize", stats.getFullDateSize());
        context.put("partitionName", p.getName());
        context.put("dbName", db.getOriginName());
        context.put("tableName", table.getName());
        context.put("quoteColumnName", quoteColumnName);
        context.put("countNullFunction", stats.getFullNullCount());
        context.put("hllFunction", stats.getFullNDV());
        context.put("maxFunction", stats.getFullMax());
        context.put("minFunction", stats.getFullMin());
        builder.append(StatisticSQLs.build(context, template));
        return builder.toString();
    }
}
