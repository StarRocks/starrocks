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
import org.junit.Assert;
import org.junit.Test;

public class LogUtilTest {
    @Test
    public void testGetCurrentStackTrace() {
        String trace = LogUtil.getCurrentStackTrace();
        Assert.assertTrue(trace.startsWith("\n        "));
        Assert.assertTrue(trace.contains("java.lang.Thread.getStackTrace"));
    }

    @Test
    public void testLogConnectionInfoToAuditLogAndQueryQueue() {
        Config.audit_log_modules = new String[] {"slow_query", "query", "connection"};
        LogUtil.logConnectionInfoToAuditLogAndQueryQueue(new ConnectContext(), null);
        Assert.assertTrue(QueryDetailQueue.getQueryDetailsAfterTime(0L).size() > 0);
    }
}
