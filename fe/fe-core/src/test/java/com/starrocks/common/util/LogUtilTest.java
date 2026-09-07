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
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.AuditEventProcessor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetailQueue;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.summary.AuditLoaderMgr;
import mockit.Mock;
import mockit.MockUp;
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
    public void testAuditLoaderDoesNotFeedQueryDetailQueue() {
        // Enabling the audit loader makes the connection audit event be generated even when the
        // "connection" audit log module is off, but it must not also start pushing connection
        // entries into the query detail queue behind /api/query_detail.
        String[] origModules = Config.audit_log_modules;
        boolean origLoader = Config.enable_audit_loader;
        try {
            Config.audit_log_modules = new String[] {"slow_query", "query"};
            Config.enable_audit_loader = true;
            long before = QueryDetailQueue.getTotalQueriesCount();
            LogUtil.logConnectionInfoToAuditLogAndQueryQueue(new ConnectContext(), null);
            Assertions.assertEquals(before, QueryDetailQueue.getTotalQueriesCount());
        } finally {
            Config.audit_log_modules = origModules;
            Config.enable_audit_loader = origLoader;
        }
    }

    @Test
    public void testShouldSkipConnectionEventGatesOnConflict() {
        // AuditEventProcessor fans every audit event out to ALL active AUDIT plugins, not just the
        // builtin loader, so the connection event must not be generated when an external AUDIT
        // plugin is present (the builtin loader disabled by conflict), even with
        // enable_audit_loader on: otherwise that external plugin would get events the operator
        // never asked for via audit_log_modules.
        boolean origLoader = Config.enable_audit_loader;
        AuditLoaderMgr mgr = GlobalStateMgr.getCurrentState().getAuditLoaderMgr();
        Assertions.assertNotNull(mgr);
        boolean origDisabledByConflict = mgr.isDisabledByConflict();
        try {
            Config.enable_audit_loader = false;
            mgr.setDisabledByConflict(false);
            Assertions.assertTrue(LogUtil.shouldSkipConnectionEvent(false),
                    "must skip when neither connection logging nor the builtin loader is enabled");

            Config.enable_audit_loader = true;
            Assertions.assertFalse(LogUtil.shouldSkipConnectionEvent(false),
                    "must generate the event when the builtin loader is enabled and will consume it");

            mgr.setDisabledByConflict(true);
            Assertions.assertTrue(LogUtil.shouldSkipConnectionEvent(false),
                    "must not generate the event once an external AUDIT plugin has disabled the builtin loader");

            Assertions.assertFalse(LogUtil.shouldSkipConnectionEvent(true),
                    "the \"connection\" audit log module alone must still generate the event either way");
        } finally {
            Config.enable_audit_loader = origLoader;
            mgr.setDisabledByConflict(origDisabledByConflict);
        }
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
}
