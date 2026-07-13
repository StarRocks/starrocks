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

package com.starrocks.summary;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.AuditEvent.EventType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class AuditLoaderTest {

    private static AuditEvent baseEvent() {
        AuditEvent event = new AuditEvent();
        event.type = EventType.AFTER_QUERY;
        event.queryId = "qid-1";
        event.timestamp = 1000L;
        event.user = "alice";
        event.db = "db1";
        event.stmt = "SELECT 1";
        event.isQuery = true;
        event.queryTime = 1;
        event.queriedRelations = List.of("db1.t1", "db1.t2");
        return event;
    }

    @Test
    public void testFormatRowJsonBasic() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        JsonObject obj = JsonParser.parseString(mgr.formatRowJson(baseEvent())).getAsJsonObject();

        Assertions.assertEquals("qid-1", obj.get("queryId").getAsString());
        Assertions.assertEquals("alice", obj.get("user").getAsString());
        Assertions.assertEquals("db1", obj.get("db").getAsString());
        Assertions.assertEquals("SELECT 1", obj.get("stmt").getAsString());
        // isQuery is stored as 1/0.
        Assertions.assertEquals(1, obj.get("isQuery").getAsInt());
        // QueriedRelations is a JSON array.
        JsonArray relations = obj.get("QueriedRelations").getAsJsonArray();
        Assertions.assertEquals(2, relations.size());
        Assertions.assertEquals("db1.t1", relations.get(0).getAsString());
    }

    @Test
    public void testQueryTypeConnection() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        event.type = EventType.CONNECTION;
        JsonObject obj = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject();
        Assertions.assertEquals("connection", obj.get("queryType").getAsString());
    }

    @Test
    public void testQueryTypeSlowVsNormal() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();

        AuditEvent normal = baseEvent();
        normal.queryTime = 0;
        Assertions.assertEquals("query",
                JsonParser.parseString(mgr.formatRowJson(normal)).getAsJsonObject().get("queryType").getAsString());

        AuditEvent slow = baseEvent();
        slow.queryTime = Config.qe_slow_log_ms + 1;
        Assertions.assertEquals("slow_query",
                JsonParser.parseString(mgr.formatRowJson(slow)).getAsJsonObject().get("queryType").getAsString());
    }

    @Test
    public void testTruncateToBytesAscii() {
        Assertions.assertEquals("01234", AuditLoaderMgr.truncateToBytes("0123456789", 5));
        Assertions.assertEquals("abc", AuditLoaderMgr.truncateToBytes("abc", 10));
        Assertions.assertEquals("", AuditLoaderMgr.truncateToBytes(null, 5));
    }

    @Test
    public void testTruncateToBytesNoSplitMultibyte() {
        // "你好" is 6 UTF-8 bytes (3 per char). Truncating to 4 bytes must not split the 2nd char.
        String r = AuditLoaderMgr.truncateToBytes("你好", 4);
        Assertions.assertEquals("你", r);
        Assertions.assertTrue(r.getBytes(StandardCharsets.UTF_8).length <= 4);
    }

    @Test
    public void testFieldsTruncatedToColumnWidth() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        // Oversized variable-length fields must be truncated to their column byte width,
        // otherwise one row would fail the whole stream-load batch and wedge the pipeline.
        event.candidateMvs = "m".repeat(70000);
        event.db = "d".repeat(200);
        JsonObject obj = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject();
        Assertions.assertEquals(65533, obj.get("candidateMVs").getAsString().length());
        Assertions.assertEquals(96, obj.get("db").getAsString().length());
    }

    @Test
    public void testOfferEventByteCapAndClearBuffer() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            mgr.offerEvent(baseEvent());
            long oneRowBytes = mgr.bufferedBytes();
            Assertions.assertTrue(oneRowBytes > 0);
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.droppedEvents());

            // Shrink the cap so a second identical row exceeds it: the event must be dropped and
            // counted, leaving the queue and the byte counter untouched.
            Config.audit_loader_batch_max_bytes = oneRowBytes + 1;
            mgr.offerEvent(baseEvent());
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(oneRowBytes, mgr.bufferedBytes());
            Assertions.assertEquals(1, mgr.droppedEvents());

            // clearBuffer drains the queue and subtracts exactly the drained bytes.
            mgr.clearBuffer();
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

}
