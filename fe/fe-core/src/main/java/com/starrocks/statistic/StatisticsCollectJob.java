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

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.DdlException;
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
import java.nio.charset.StandardCharsets;
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

    public String getCatalogName() {
        return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    }

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

    protected void setDefaultSessionVariable(ConnectContext context) {
        SessionVariable sessionVariable = context.getSessionVariable();
        // Statistics collecting is not user-specific, which means response latency is not that important.
        // Normally, if the page cache is enabled, the page cache must be full. Page cache is used for query
        // acceleration, then page cache is better filled with the user's data.
        sessionVariable.setUsePageCache(false);
        sessionVariable.setEnableMaterializedViewRewrite(false);
    }

    protected void collectStatisticSync(String sql, ConnectContext context) throws Exception {
        int count = 0;
        int maxRetryTimes = 5;
        do {
            LOG.debug("statistics collect sql : {}", sql);
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);

            // set default session variables for stats context
            setDefaultSessionVariable(context);

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

    public static Expr hllDeserialize(byte[] hll) {
        String str = new String(hll, StandardCharsets.UTF_8);
        Function unhex = Expr.getBuiltinFunction("unhex", new Type[] {Type.VARCHAR},
                Function.CompareMode.IS_IDENTICAL);

        FunctionCallExpr unhexExpr = new FunctionCallExpr("unhex", Lists.newArrayList(new StringLiteral(str)));
        unhexExpr.setFn(unhex);
        unhexExpr.setType(unhex.getReturnType());

        Function fn = Expr.getBuiltinFunction("hll_deserialize", new Type[] {Type.VARCHAR},
                Function.CompareMode.IS_IDENTICAL);
        FunctionCallExpr fe = new FunctionCallExpr("hll_deserialize", Lists.newArrayList(unhexExpr));
        fe.setFn(fn);
        fe.setType(fn.getReturnType());
        return fe;
    }

    public static Expr nowFn() {
        Function fn = Expr.getBuiltinFunction(FunctionSet.NOW, new Type[] {}, Function.CompareMode.IS_IDENTICAL);
        FunctionCallExpr fe = new FunctionCallExpr("now", Lists.newArrayList());
        fe.setType(fn.getReturnType());
        return fe;
    }

    public static String fullAnalyzeGetDataSize(Column column) {
        if (column.getPrimitiveType().isCharFamily()) {
            return "IFNULL(SUM(CHAR_LENGTH(" + StatisticUtils.quoting(column.getName()) + ")), 0)";
        }
        long typeSize = column.getType().getTypeSize();
        return "COUNT(1) * " + typeSize;
    }
}
