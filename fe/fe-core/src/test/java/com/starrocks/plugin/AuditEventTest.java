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

package com.starrocks.plugin;

import com.starrocks.server.RunMode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuditEventTest {
    @Test
    public void testAuditEvent() {
        AuditEvent.AuditEventBuilder builder = new AuditEvent.AuditEventBuilder()
                .setEventType(AuditEvent.EventType.CONNECTION)
                .setUser("user")
                .setAuthorizedUser("authorizedUser")
                .setClientIp("clientIp")
                .setFeIp("feIp")
                .setDb("db")
                .setState("state")
                .setBigQueryLogCPUSecondThreshold(1)
                .setCatalog("catalog")
                .setQueryId("queryId")
                .setStmtId(123)
                .setStmt("stmt")
                .setDigest("digest")
                .setErrorCode("errorCode")
                .setIsQuery(true)
                .setWarehouse("wh")
                .setSessionId("sessionId")
                .setCustomQueryId("customQueryId")
                .setCNGroup("test_cngroup")
                .addReadLocalCnt(100)
                .addReadRemoteCnt(100);

        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedNothingMode() {
                return false;
            }
        };
        AuditEvent event = builder.build();

        Assertions.assertEquals(AuditEvent.EventType.CONNECTION, event.type);
        Assertions.assertEquals("user", event.user);
        Assertions.assertEquals("authorizedUser", event.authorizedUser);
        Assertions.assertEquals("clientIp", event.clientIp);
        Assertions.assertEquals("feIp", event.feIp);
        Assertions.assertEquals("db", event.db);
        Assertions.assertEquals("state", event.state);
        Assertions.assertEquals(1, event.bigQueryLogCPUSecondThreshold);
        Assertions.assertEquals("catalog", event.catalog);
        Assertions.assertEquals("queryId", event.queryId);
        Assertions.assertEquals(123, event.stmtId);
        Assertions.assertEquals("stmt", event.stmt);
        Assertions.assertEquals("digest", event.digest);
        Assertions.assertEquals("errorCode", event.errorCode);
        Assertions.assertEquals(true, event.isQuery);
        Assertions.assertEquals("wh", event.warehouse);
        Assertions.assertEquals("sessionId", event.sessionId);
        Assertions.assertEquals("customQueryId", event.customQueryId);
        Assertions.assertEquals("test_cngroup", event.cnGroup);
        Assertions.assertEquals("50.0%", event.cacheHitRatio);
        Assertions.assertEquals((float) 50, event.getCacheMissRatio());
    }
}
