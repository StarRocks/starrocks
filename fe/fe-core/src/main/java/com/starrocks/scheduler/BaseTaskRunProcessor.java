// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.Config;
import com.starrocks.common.NotImplementedException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.ResourceGroupMetricMgr;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.SqlDigestBuilder;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public abstract class BaseTaskRunProcessor implements TaskRunProcessor {
    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        throw new NotImplementedException("Method processTaskRun need to implement");
    }

    protected void auditAfterExec(TaskRunContext context, StatementBase parsedStmt, PQueryStatistics statistics, boolean isMv) {
        String origStmt = context.getDefinition();
        ConnectContext ctx = context.getCtx();

        // slow query
        long endTime = System.currentTimeMillis();
        long elapseMs = endTime - ctx.getStartTime();

        ctx.getAuditEventBuilder().setEventType(AuditEvent.EventType.AFTER_QUERY)
                .setState(ctx.getState().toString()).setErrorCode(ctx.getErrorCode()).setQueryTime(elapseMs)
                .setScanBytes(statistics == null ? 0 : statistics.scanBytes)
                .setScanRows(statistics == null ? 0 : statistics.scanRows)
                .setCpuCostNs(statistics == null || statistics.cpuCostNs == null ? 0 : statistics.cpuCostNs)
                .setMemCostBytes(statistics == null || statistics.memCostBytes == null ? 0 : statistics.memCostBytes)
                .setReturnRows(ctx.getReturnRows())
                .setStmtId(ctx.getStmtId())
                .setQueryId(ctx.getQueryId() == null ? "NaN" : ctx.getQueryId().toString());

        if (ctx.getState().isQuery()) {
            MetricRepo.COUNTER_QUERY_ALL.increase(1L);
            ResourceGroupMetricMgr.increaseQuery(ctx, 1L);
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                // err query
                MetricRepo.COUNTER_QUERY_ERR.increase(1L);
                ResourceGroupMetricMgr.increaseQueryErr(ctx, 1L);
            } else {
                // ok query
                MetricRepo.COUNTER_QUERY_SUCCESS.increase(1L);
                MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
                if (elapseMs > Config.qe_slow_log_ms || ctx.getSessionVariable().isEnableSQLDigest()) {
                    MetricRepo.COUNTER_SLOW_QUERY.increase(1L);
                    ctx.getAuditEventBuilder().setDigest(computeStatementDigest(parsedStmt));
                }
            }
            ctx.getAuditEventBuilder().setIsQuery(true);
        } else {
            ctx.getAuditEventBuilder().setIsQuery(false);
        }
        ctx.getAuditEventBuilder().setIsMv(isMv);
        ctx.getAuditEventBuilder().setFeIp(FrontendOptions.getLocalHostAddress());

        // We put origin query stmt at the end of audit log, for parsing the log more convenient.
        if (!ctx.getState().isQuery() && (parsedStmt != null && parsedStmt.needAuditEncryption())) {
            ctx.getAuditEventBuilder().setStmt(parsedStmt.toSql());
        } else if (ctx.getState().isQuery() && containsComment(origStmt)) {
            // avoid audit log can't replay
            ctx.getAuditEventBuilder().setStmt(origStmt);
        } else {
            ctx.getAuditEventBuilder().setStmt(origStmt.replace("\n", " "));
        }

        GlobalStateMgr.getCurrentAuditEventProcessor().handleAuditEvent(ctx.getAuditEventBuilder().build());
    }

    private String computeStatementDigest(StatementBase queryStmt) {
        if (queryStmt == null) {
            return "";
        }
        String digest;
        if (queryStmt instanceof QueryStmt) {
            digest = ((QueryStmt) queryStmt).toDigest();
        } else {
            digest = SqlDigestBuilder.build(queryStmt);
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.reset();
            md.update(digest.getBytes());
            return Hex.encodeHexString(md.digest());
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

    private boolean containsComment(String sql) {
        return (sql.contains("--")) || sql.contains("#");
    }

}
