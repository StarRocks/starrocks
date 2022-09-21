// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
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

    public abstract void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception;

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

    public void collectStatisticSync(String sql, ConnectContext context) throws Exception {
        LOG.debug("statistics collect sql : " + sql);
        StatementBase parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
        StmtExecutor executor = new StmtExecutor(context, parsedStmt);
        context.setExecutor(executor);
        context.setQueryId(UUIDUtil.genUUID());
        context.setStartTime();
        executor.execute();

        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            LOG.warn("Statistics collect fail | Error Message [" + context.getState().getErrorMessage() + "] | " +
                    "SQL [" + sql + "]");
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

    protected int splitColumns(long rowCount) {
        long splitSize;
        if (rowCount == 0) {
            splitSize = columns.size();
        } else {
            splitSize = Config.statistic_collect_max_row_count_per_query / rowCount + 1;
            if (splitSize > columns.size()) {
                splitSize = columns.size();
            }
        }
        return (int) splitSize;
    }

    protected String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }
}
