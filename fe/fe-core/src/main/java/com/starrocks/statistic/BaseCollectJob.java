// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.List;

import static com.starrocks.sql.parser.SqlParser.parseFirstStatement;

public class BaseCollectJob {
    protected final AnalyzeJob analyzeJob;
    protected final Database db;
    protected final OlapTable table;
    protected final List<String> columns;

    public BaseCollectJob(AnalyzeJob analyzeJob, Database db, OlapTable table, List<String> columns) {
        this.analyzeJob = analyzeJob;
        this.db = db;
        this.table = table;
        this.columns = columns;
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

    protected static final String INSERT_STATISTIC_TEMPLATE = "INSERT INTO " + Constants.StatisticsTableName;


    public AnalyzeJob getAnalyzeJob() {
        return analyzeJob;
    }

    public Database getDb() {
        return db;
    }

    public Table getTable() {
        return table;
    }

    public void collectStatisticSync(String sql) throws Exception {
        ConnectContext context = StatisticUtils.buildConnectContext();
        StatementBase parsedStmt = parseFirstStatement(sql, context.getSessionVariable().getSqlMode());
        StmtExecutor executor = new StmtExecutor(context, parsedStmt);
        executor.execute();

        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            throw new DdlException(context.getState().getErrorMessage());
        }
    }
    void collect() throws Exception {

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
}
