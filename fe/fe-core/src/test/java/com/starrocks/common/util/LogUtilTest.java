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

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetailQueue;
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
