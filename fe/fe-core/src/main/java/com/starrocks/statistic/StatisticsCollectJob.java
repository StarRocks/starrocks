// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.statistic;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang.StringUtils;
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
    protected final Table table;
    protected final List<String> columns;

    protected final StatsConstants.AnalyzeType type;
    protected final StatsConstants.ScheduleType scheduleType;
    protected final Map<String, String> properties;

    protected StatisticsCollectJob(Database db, Table table, List<String> columns,
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

    protected void collectStatisticSync(String sql, ConnectContext context) throws Exception {
        int count = 0;
        int maxRetryTimes = 5;
        do {
            LOG.debug("statistics collect sql : {}", sql);
<<<<<<< HEAD
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
=======
            StatementBase parsedStmt = SqlParser.parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
>>>>>>> branch-2.5-mrs
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            SessionVariable sessionVariable = context.getSessionVariable();
            sessionVariable.setEnableMaterializedViewRewrite(false);
            // Statistics collecting is not user-specific, which means response latency is not that important.
            // Normally, if the page cache is enabled, the page cache must be full. Page cache is used for query 
            // acceleration, then page cache is better filled with the user's data. 
            sessionVariable.setUsePageCache(false);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("Statistics collect fail | Error Message [{}] | {} | SQL [{}]",
                        context.getState().getErrorMessage(), DebugUtil.printId(context.getQueryId()), sql);
                if (StringUtils.contains(context.getState().getErrorMessage(), "Too many versions")) {
                    Thread.sleep(Config.statistic_collect_too_many_version_sleep);
                    count++;
                } else {
                    throw new DdlException(context.getState().getErrorMessage());
                }
            } else {
                return;
            }
        } while (count < maxRetryTimes);

        throw new DdlException(context.getState().getErrorMessage());
    }

    protected String getMinMaxFunction(Column column, String name, boolean isMax) {
        String fn = isMax ? "MAX" : "MIN";
        if (column.getPrimitiveType().isCharFamily()) {
            fn = fn + "(LEFT(" + name + ", 200))";
        } else {
            fn = fn + "(" + name + ")";
        }
        fn = "IFNULL(" + fn + ", '')";
        return fn;
    }

    protected String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }
}
