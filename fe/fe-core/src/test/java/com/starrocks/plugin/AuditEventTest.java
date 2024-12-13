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

import org.junit.Assert;
import org.junit.Test;

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
                .setDigest("digest")
                .setErrorCode("errorCode")
                .setIsQuery(true)
                .setWarehouse("wh");
        AuditEvent event = builder.build();

        Assert.assertEquals(AuditEvent.EventType.CONNECTION, event.type);
        Assert.assertEquals("user", event.user);
        Assert.assertEquals("authorizedUser", event.authorizedUser);
        Assert.assertEquals("clientIp", event.clientIp);
        Assert.assertEquals("feIp", event.feIp);
        Assert.assertEquals("db", event.db);
        Assert.assertEquals("state", event.state);
        Assert.assertEquals(1, event.bigQueryLogCPUSecondThreshold);
        Assert.assertEquals("catalog", event.catalog);
        Assert.assertEquals("errorCode", event.errorCode);
        Assert.assertEquals(true, event.isQuery);
        Assert.assertEquals("wh", event.warehouse);
    }
}
