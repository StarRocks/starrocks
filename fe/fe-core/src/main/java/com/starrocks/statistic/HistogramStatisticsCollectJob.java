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

import static com.starrocks.sql.parser.SqlParser.parseFirstStatement;
import static com.starrocks.statistic.Constants.HistogramStatisticsTableName;

public class HistogramStatisticsCollectJob extends StatisticsCollectJob {
    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnName', '$dbName.$tableName', histogram($columnName, $totalRows, $sampleRows, $bucketNum)"
                    + " FROM (SELECT * FROM $dbName.$tableName $sampleTabletHint ORDER BY $columnName LIMIT $totalRows) t";

    public HistogramStatisticsCollectJob(AnalyzeJob analyzeJob, Database db, OlapTable table, List<String> columns) {
        super(analyzeJob, db, table, columns);
    }

    @Override
    public void collect() throws Exception {
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.getSessionVariable().setNewPlanerAggStage(1);

        long totalRows = table.getRowCount();
        long sampleRows = Long.parseLong(analyzeJob.getProperties().get(Constants.PROP_SAMPLE_COLLECT_ROWS_KEY));
        long bucketNum = Long.parseLong(analyzeJob.getProperties().get(Constants.PRO_BUCKET_NUM));

        for (String column : analyzeJob.getColumns()) {
            String sql = buildCollectHistogram(db, table, totalRows, sampleRows, bucketNum, column);

            StatementBase parsedStmt = parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                throw new DdlException(context.getState().getErrorMessage());
            }
        }
    }

    public String buildCollectHistogram(Database database, OlapTable table, Long totalRows, Long sampleRows,
                                        Long bucketNum, String columnName) {
        StringBuilder builder = new StringBuilder("INSERT INTO ").append(HistogramStatisticsTableName).append(" ");

        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", columnName);
        context.put("dbName", ClusterNamespace.getNameFromFullName(database.getFullName()));
        context.put("tableName", table.getName());

        context.put("totalRows", totalRows);
        context.put("sampleRows", sampleRows);
        context.put("bucketNum", bucketNum);

        if (sampleRows >= totalRows) {
            context.put("sampleTabletHint", "");
        } else {
            context.put("sampleTabletHint", getSampleTabletHint(table, sampleRows));
        }

        builder.append(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
        return builder.toString();
    }
}
