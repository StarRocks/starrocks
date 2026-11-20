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

package com.starrocks.common.util;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.StarRocksException;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.AuditEventProcessor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetailQueue;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.core.impl.ThrowableProxy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class LogUtilTest {
    @Test
    public void testGetCurrentStackTrace() {
        String trace = LogUtil.getCurrentStackTrace();
        Assertions.assertTrue(trace.startsWith("\n        "));
        Assertions.assertTrue(trace.contains("java.lang.Thread.getStackTrace"));
    }

    @Test
    public void testGetCurrentStackTraceToList() {
        List<String> trace = LogUtil.getCurrentStackTraceToList();
        System.out.println(trace);
        Assertions.assertTrue(trace.get(0).contains("java.lang.Thread.getStackTrace"));
    }

    @Test
    public void testLogConnectionInfoToAuditLogAndQueryQueue() {
        Config.audit_log_modules = new String[] {"slow_query", "query", "connection"};
        LogUtil.logConnectionInfoToAuditLogAndQueryQueue(new ConnectContext(), null);
        Assertions.assertFalse(QueryDetailQueue.getQueryDetailsAfterTime(0L).isEmpty());
    }

    // A failed login reports its message in both ErrorCode (historically) and ErrorMessage.
    @Test
    public void testLogConnectionInfoSanitizesErrorFields() {
        Config.audit_log_modules = new String[] {"slow_query", "query", "connection"};
        List<AuditEvent> events = Lists.newArrayList();
        new MockUp<AuditEventProcessor>() {
            @Mock
            public void handleAuditEvent(AuditEvent event) {
                events.add(event);
            }
        };

        ConnectContext ctx = new ConnectContext();
        ctx.getState().setError("Access denied for user 'u'\n|State=OK");
        LogUtil.logConnectionInfoToAuditLogAndQueryQueue(ctx, null);

        Assertions.assertEquals(1, events.size());
        Assertions.assertEquals("Access denied for user 'u' State=OK", events.get(0).errorCode);
        Assertions.assertEquals("Access denied for user 'u' State=OK", events.get(0).errorMessage);
    }

    @Test
    public void testGetUnwoundExceptionMessage() {
        // single exception with no cause chain: plain message, no class name
        RuntimeException e = new RuntimeException("hello");
        String output = LogUtil.getUnwoundExceptionMessage(e);
        Assertions.assertEquals("hello", output);
    }

    @Test
    public void testGetUnwoundExceptionMessageNested() {
        // nested chain: outermost layer is plain (no class name), deeper layers include the class name
        RuntimeException inner = new RuntimeException("inner cause");
        RuntimeException outer = new RuntimeException("outer msg", inner);
        String output = LogUtil.getUnwoundExceptionMessage(outer);
        Assertions.assertTrue(output.contains("outer msg"));
        Assertions.assertFalse(output.contains("RuntimeException: outer msg"));
        Assertions.assertTrue(output.contains("RuntimeException: inner cause"));
    }

    @Test
    public void testGetUnwoundExceptionMessageNullMessage() {
        // single exception with null message and no cause chain: null, no NPE
        RuntimeException e = new RuntimeException((String) null);
        String output = LogUtil.getUnwoundExceptionMessage(e);
        Assertions.assertNull(output);
    }

    @Test
    public void testGetUnwoundExceptionMessageUnpeelsContentFreeWrapper() {
        // mirrors ErrorReport.wrapWithRuntimeException: `new RuntimeException(cause)` with no message
        // of its own, so its message defaults to `cause.toString()` (JDK default) and adds nothing new
        DdlException ddl = new DdlException("table already exists");
        RuntimeException wrapper = new RuntimeException(ddl);
        String output = LogUtil.getUnwoundExceptionMessage(wrapper);
        Assertions.assertEquals("table already exists", output);
    }

    @Test
    public void testGetUnwoundExceptionMessageUnpeelsNullMessageWrapper() {
        // a layer with an explicit null message (e.g. `new Foo(null, cause)`) carries no information
        // of its own either, so it should be unpeeled just like the cause.toString()-default case
        RuntimeException cause = new RuntimeException("root cause");
        RuntimeException wrapper = new RuntimeException((String) null, cause);
        String output = LogUtil.getUnwoundExceptionMessage(wrapper);
        Assertions.assertEquals("root cause", output);
    }

    @Test
    public void testGetUnwoundExceptionMessageCircularReference() {
        // Covers the safety guard: when getCause() returns self, the loop breaks after the first layer
        Throwable circular = new RuntimeException("circular") {
            @Override
            public Throwable getCause() {
                return this;
            }
        };
        String output = LogUtil.getUnwoundExceptionMessage(circular);
        Assertions.assertTrue(output.contains("circular"));
        Assertions.assertFalse(output.contains("\n")); // only one layer, not an infinite chain
    }

    @Test
    public void testDesensitizeDropsMessagesKeepsTypesAndFrames() {
        boolean prev = Config.enable_sql_desensitize_in_log;
        Exception e = new IllegalStateException("outer SECRET", new DdlException("inner SECRET"));
        try {
            Config.enable_sql_desensitize_in_log = false;
            Assertions.assertSame(e, LogUtil.desensitize(e));
            Assertions.assertEquals("SECRET", LogUtil.desensitizeText("SECRET"));

            Config.enable_sql_desensitize_in_log = true;
            Assertions.assertFalse(LogUtil.desensitizeText("SECRET").contains("SECRET"));
            Throwable d = LogUtil.desensitize(e);
            // log4j renders a throwable from its class and message, so the original type must be in the message
            Assertions.assertEquals(IllegalStateException.class.getName(), d.getMessage());
            Assertions.assertArrayEquals(e.getStackTrace(), d.getStackTrace());
            Assertions.assertEquals(DdlException.class.getName(), d.getCause().getMessage());
            String rendered = new ThrowableProxy(d).getExtendedStackTraceAsString();
            Assertions.assertTrue(rendered.contains(IllegalStateException.class.getName()), rendered);
            Assertions.assertTrue(rendered.contains(DdlException.class.getName()), rendered);
            Assertions.assertFalse(rendered.contains("SECRET"), rendered);
        } finally {
            Config.enable_sql_desensitize_in_log = prev;
        }
    }

    @Test
    public void testDesensitizeKeepsSuppressed() {
        boolean prev = Config.enable_sql_desensitize_in_log;
        // e.g. a planning failure carrying the lock re-acquire timeout; the suppressed one refers back to it
        Exception e = new IllegalStateException("plan SECRET");
        Exception suppressed = new IllegalArgumentException("lock SECRET");
        e.addSuppressed(suppressed);
        suppressed.initCause(e);
        try {
            Config.enable_sql_desensitize_in_log = true;
            Throwable d = LogUtil.desensitize(e);
            Assertions.assertEquals(1, d.getSuppressed().length);
            Throwable s = d.getSuppressed()[0];
            Assertions.assertEquals(IllegalArgumentException.class.getName(), s.getMessage());
            Assertions.assertArrayEquals(suppressed.getStackTrace(), s.getStackTrace());
            Assertions.assertNull(s.getCause());
            String rendered = new ThrowableProxy(d).getExtendedStackTraceAsString();
            Assertions.assertTrue(rendered.contains("Suppressed:"), rendered);
            Assertions.assertTrue(rendered.contains(IllegalArgumentException.class.getName()), rendered);
            Assertions.assertFalse(rendered.contains("SECRET"), rendered);
        } finally {
            Config.enable_sql_desensitize_in_log = prev;
        }
    }

    @Test
    public void testDesensitizeMessageKeepsTypeAndErrorCode() {
        boolean prev = Config.enable_sql_desensitize_in_log;
        StarRocksException internal = new StarRocksException(InternalErrorCode.REPLICA_FEW_ERR, "tablet SECRET");
        StarRocksException coded = new StarRocksException(ErrorCode.ERR_BAD_TABLE_ERROR, "SECRET_T");
        ErrorReportException reported = ErrorReportException.report(ErrorCode.ERR_BAD_TABLE_ERROR, "SECRET_T");
        Exception plain = new IllegalStateException("plain SECRET");
        try {
            Config.enable_sql_desensitize_in_log = false;
            Assertions.assertEquals("tablet SECRET", LogUtil.desensitizeMessage(internal));
            Assertions.assertEquals(reported.getMessage(), LogUtil.desensitizeMessage(reported));
            Assertions.assertEquals(plain.toString(), LogUtil.desensitizeToString(plain));

            Config.enable_sql_desensitize_in_log = true;
            Assertions.assertEquals(StarRocksException.class.getName() + "(REPLICA_FEW_ERR): <desensitized>",
                    LogUtil.desensitizeMessage(internal));
            Assertions.assertEquals(StarRocksException.class.getName() + "(ERR_BAD_TABLE_ERROR): <desensitized>",
                    LogUtil.desensitizeMessage(coded));
            Assertions.assertEquals(ErrorReportException.class.getName() + "(ERR_BAD_TABLE_ERROR): <desensitized>",
                    LogUtil.desensitizeMessage(reported));
            Assertions.assertEquals(IllegalStateException.class.getName() + ": <desensitized>",
                    LogUtil.desensitizeMessage(plain));
            Assertions.assertEquals(IllegalStateException.class.getName() + ": <desensitized>",
                    LogUtil.desensitizeToString(plain));
        } finally {
            Config.enable_sql_desensitize_in_log = prev;
        }
    }

    @Test
    public void testRedactSqlForLog() throws Exception {
        boolean prev = Config.enable_sql_desensitize_in_log;
        String sql = "SELECT * FROM FILES(\"path\"=\"s3://b/f.parquet\", \"aws.s3.secret_key\"=\"SK_SECRET\") "
                + "WHERE c = 'LIT_SECRET'";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        try {
            Config.enable_sql_desensitize_in_log = false;
            Assertions.assertEquals(SqlCredentialRedactor.redact(sql), LogUtil.redactSqlForLog(sql, stmt));
            Assertions.assertEquals(sql, LogUtil.desensitizeSql(sql, stmt));

            Config.enable_sql_desensitize_in_log = true;
            String logged = LogUtil.redactSqlForLog(sql, stmt);
            Assertions.assertTrue(logged.contains("= ?"), logged);
            Assertions.assertFalse(logged.contains("SECRET"), logged);
            Assertions.assertEquals("<desensitized>", LogUtil.redactSqlForLog(sql, null));

            // formatting runs on failure paths and must not replace the original failure with its own
            new MockUp<AstToSQLBuilder>() {
                @Mock
                public String toDesensitizedSQL(StatementBase statement) {
                    throw new AssertionError("format failed");
                }
            };
            Assertions.assertEquals("<desensitized>", LogUtil.redactSqlForLog(sql, stmt));
        } finally {
            Config.enable_sql_desensitize_in_log = prev;
        }
    }
}
