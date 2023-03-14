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

package com.starrocks.statistic;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
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

    public void collectStatisticSync(String sql, ConnectContext context) throws Exception {
        int count = 0;
        int maxRetryTimes = 10;
        do {
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
                if (context.getState().getErrorMessage().contains("Too many versions")) {
                    Thread.sleep(60000);
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

    protected String getDataSize(Column column, boolean isSample) {
        if (column.getPrimitiveType().isCharFamily() || column.getPrimitiveType().isJsonType()) {
            if (isSample) {
                return "IFNULL(SUM(CHAR_LENGTH(`column_key`) * t1.count), 0)";
            } else {
                return "IFNULL(SUM(CHAR_LENGTH(" + StatisticUtils.quoting(column.getName()) + ")), 0)";
            }
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
        // Supports a maximum of 1024 tasks for a union,
        // preventing unexpected situations caused by too many tasks being executed at one time
        return (int) Math.min(1024, splitSize);
    }

    protected String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }
}
