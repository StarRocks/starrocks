// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.cluster.ClusterNamespace;
import org.apache.velocity.VelocityContext;

import java.util.List;

import static com.starrocks.statistic.Constants.HistogramStatisticsTableName;

public class HistogramStatisticsCollectJob extends BaseCollectJob {
    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT $tableId, $columnName, '$dbName.$tableName', histogram($columnName, $totalRows, $bucketNum)"
                    + " FROM $dbName.$tableName";

    private final Long bucketNum;

    public HistogramStatisticsCollectJob(AnalyzeJob analyzeJob, Database db, OlapTable table, List<String> columns,
                                         Long bucketNum) {
        super(analyzeJob, db, table, columns);
        this.bucketNum = bucketNum;
    }

    public Long getBucketNum() {
        return bucketNum;
    }

    void collect() throws Exception {
        Long totalRows = table.getRowCount();
        for (String column : analyzeJob.getColumns()) {
            String sql = buildCollectHistogram(db, table, totalRows, bucketNum, column);
            collectStatisticSync(sql);
        }
    }

    public String buildCollectHistogram(Database database, OlapTable table, Long totalRows, Long bucketNum, String columnName) {
        StringBuilder builder = new StringBuilder("INSERT INTO ").append(HistogramStatisticsTableName).append(" ");

        VelocityContext context = new VelocityContext();
        context.put("dbName", ClusterNamespace.getNameFromFullName(database.getFullName()));
        context.put("tableName", table.getName());
        context.put("columnName", columnName);

        context.put("$totalRows", totalRows);
        context.put("$bucketNum", bucketNum);

        builder.append(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
        return builder.toString();
    }
}
