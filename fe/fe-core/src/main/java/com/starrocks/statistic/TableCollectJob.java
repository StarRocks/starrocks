// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.cluster.ClusterNamespace;
import org.apache.velocity.VelocityContext;

import java.io.StringWriter;
import java.util.List;

public class TableCollectJob extends StatisticsCollectJob {
    private static final String INSERT_SELECT_FULL_TEMPLATE =
            "SELECT $tableId, '$columnName', $dbId, '$tableName', '$dbName', COUNT(1), "
                    + "$dataSize, $countDistinctFunction, $countNullFunction, $maxFunction, $minFunction, NOW() "
                    + "FROM $tableName";

    public TableCollectJob(AnalyzeJob analyzeJob, Database db, OlapTable table, List<String> columns) {
        super(analyzeJob, db, table, columns);
    }

    @Override
    public void collect() throws Exception {
        for (String column : columns) {
            String sql = buildFullInsertSQL(db, table, Lists.newArrayList(column));
            collectStatisticSync(sql);
        }
    }

    private String buildFullInsertSQL(Database database, OlapTable table, List<String> columnNames) {
        StringBuilder builder = new StringBuilder(INSERT_STATISTIC_TEMPLATE).append(" ");

        for (String name : columnNames) {
            VelocityContext context = new VelocityContext();
            Column column = table.getColumn(name);

            context.put("dbId", database.getId());
            context.put("tableId", table.getId());
            context.put("columnName", name);
            context.put("dbName", db.getFullName());
            context.put("tableName", ClusterNamespace.getNameFromFullName(db.getFullName()) + "." + table.getName());
            context.put("dataSize", getDataSize(column, false));

            if (!column.getType().canStatistic()) {
                context.put("countDistinctFunction", "0");
                context.put("countNullFunction", "0");
                context.put("maxFunction", "''");
                context.put("minFunction", "''");
            } else {
                context.put("countDistinctFunction", "approx_count_distinct(`" + name + "`)");
                context.put("countNullFunction", "COUNT(1) - COUNT(`" + name + "`)");
                context.put("maxFunction", "IFNULL(MAX(`" + name + "`), '')");
                context.put("minFunction", "IFNULL(MIN(`" + name + "`), '')");
            }

            StringWriter sw = new StringWriter();
            DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", INSERT_SELECT_FULL_TEMPLATE);

            builder.append(sw);
            builder.append(" UNION ALL ");
        }

        return builder.substring(0, builder.length() - "UNION ALL ".length());
    }
}
