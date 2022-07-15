// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import org.apache.velocity.VelocityContext;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.parser.SqlParser.parseFirstStatement;
import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

public class HistogramStatisticsCollectJob extends StatisticsCollectJob {
    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnName', '$dbName.$tableName', histogram($columnName, $bucketNum, $sampleRatio, $topN )"
                    + " FROM (SELECT * FROM $dbName.$tableName $sampleTabletHint ORDER BY $columnName LIMIT $totalRows) t";

    public HistogramStatisticsCollectJob(Database db, OlapTable table, List<String> columns,
                                         StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                         Map<String, String> properties) {
        super(db, table, columns, type, scheduleType, properties);
    }

    @Override
    public void collect() throws Exception {
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.getSessionVariable().setNewPlanerAggStage(1);

        long totalRows = table.getRowCount();
        long sampleRows = Long.parseLong(properties.get(StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS));
        long bucketNum = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_BUCKET_NUM));
        long topN = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_TOPN_SIZE));

        for (String column : columns) {
            String sql = buildCollectHistogram(db, table, totalRows, sampleRows, bucketNum, topN, column);

            StatementBase parsedStmt = parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                throw new DdlException(context.getState().getErrorMessage());
            }
        }
    }

    public String buildCollectHistogram(Database database, OlapTable table, Long totalRows, Long sampleRows,
                                        Long bucketNum, Long topN, String columnName) {
        StringBuilder builder = new StringBuilder("INSERT INTO ").append(HISTOGRAM_STATISTICS_TABLE_NAME).append(" ");

        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", columnName);
        context.put("dbName", ClusterNamespace.getNameFromFullName(database.getFullName()));
        context.put("tableName", table.getName());

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRows / totalRows);
        context.put("totalRows", totalRows);
        context.put("topN", topN);

        if (sampleRows >= totalRows) {
            context.put("sampleTabletHint", "");
        } else {
            context.put("sampleTabletHint", getSampleTabletHint(table, sampleRows));
        }

        builder.append(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
        return builder.toString();
    }
}
