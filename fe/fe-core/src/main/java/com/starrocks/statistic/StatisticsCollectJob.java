// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.statistic;

import com.google.common.collect.Sets;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(StatisticsMetaManager.class);

    protected final Database db;
    protected final OlapTable table;
    protected final List<String> columns;

    protected final StatsConstants.AnalyzeType type;
    protected final StatsConstants.ScheduleType scheduleType;
    protected final Map<String, String> properties;

    protected StatisticsCollectJob(Database db, OlapTable table, List<String> columns,
                                StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                Map<String, String> properties) {
        this.db = db;
        this.table = table;
        this.columns = columns;

        this.type = type;
        this.scheduleType = scheduleType;
        this.properties = properties;
    }

    protected static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_LOGSYSTEM_CLASS,
                "org.apache.velocity.runtime.log.Log4JLogChute");
        DEFAULT_VELOCITY_ENGINE.setProperty("runtime.log.logsystem.log4j.logger", "velocity");
    }

    public abstract void collect() throws Exception;

    public Database getDb() {
        return db;
    }

    public Table getTable() {
        return table;
    }

    public List<String> getColumns() {
        return columns;
    }

    public StatsConstants.AnalyzeType getType() {
        return type;
    }

    public StatsConstants.ScheduleType getScheduleType() {
        return scheduleType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void collectStatisticSync(String sql) throws Exception {

        LOG.debug("statistics collect sql : " + sql);
        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
        StmtExecutor executor = new StmtExecutor(context, parsedStmt);
        executor.execute();

        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            throw new DdlException(context.getState().getErrorMessage());
        }
    }

    protected String getDataSize(Column column, boolean isSample) {
        if (column.getPrimitiveType().isCharFamily() || column.getPrimitiveType().isJsonType()) {
            if (isSample) {
                return "IFNULL(SUM(CHAR_LENGTH(`" + column.getName() + "`) * t1.count), 0)";
            }
            return "IFNULL(SUM(CHAR_LENGTH(`" + column.getName() + "`)), 0)";
        }

        long typeSize = column.getType().getTypeSize();

        if (isSample && column.getType().canStatistic()) {
            return "IFNULL(SUM(t1.count), 0) * " + typeSize;
        }
        return "COUNT(1) * " + typeSize;
    }

    protected String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }

    protected String getSampleTabletHint(OlapTable table, long rows) {
        Set<String> randomTablets = Sets.newHashSet();
        rows = Math.max(rows, 1);

        // calculate the number of tablets by each partition
        // simpleTabletNums = simpleRows / partitionNums / (actualPartitionRows / actualTabletNums)
        long avgRowsPerPartition = rows / Math.max(table.getPartitions().size(), 1);

        for (Partition p : table.getPartitions()) {
            List<Long> ids = p.getBaseIndex().getTabletIdsInOrder();

            if (ids.isEmpty()) {
                continue;
            }

            if (p.getBaseIndex().getRowCount() < (avgRowsPerPartition / 2)) {
                continue;
            }

            long avgRowsPerTablet = Math.max(p.getBaseIndex().getRowCount() / ids.size(), 1);
            long tabletCounts = Math.max(avgRowsPerPartition / avgRowsPerTablet, 1);
            tabletCounts = Math.min(tabletCounts, ids.size());

            for (int i = 0; i < tabletCounts; i++) {
                randomTablets.add(String.valueOf(ids.get(i)));
            }
        }

        return " Tablet(" + String.join(", ", randomTablets) + ")";
    }
}
