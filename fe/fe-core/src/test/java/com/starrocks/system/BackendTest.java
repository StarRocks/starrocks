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

package com.starrocks.system;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;

public class BackendTest {

    @Test
    public void testSetHeartbeatPort() {
        Backend be = new Backend();
        be.setHeartbeatPort(1000);
        Assertions.assertTrue(be.getHeartbeatPort() == 1000);
    }

    @Test
    public void cpuCoreUpdate() {
        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, 8);
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(4, BackendResourceStat.getInstance().getDefaultDOP(DEFAULT_WAREHOUSE_ID));

        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, 16);
        Assertions.assertEquals(16, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getDefaultDOP(DEFAULT_WAREHOUSE_ID));

        // add new backend 2
        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 2, 8);
        Assertions.assertEquals(12, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(6, BackendResourceStat.getInstance().getDefaultDOP(DEFAULT_WAREHOUSE_ID));

        // remove new backend 2
        BackendResourceStat.getInstance().removeBe(DEFAULT_WAREHOUSE_ID, 2);
        Assertions.assertEquals(16, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getDefaultDOP(DEFAULT_WAREHOUSE_ID));
    }

    @Test
    public void defaultSinkDopTest() {
        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, 8);
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(2, BackendResourceStat.getInstance().getSinkDefaultDOP(DEFAULT_WAREHOUSE_ID));

        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, 16);
        Assertions.assertEquals(16, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(5, BackendResourceStat.getInstance().getSinkDefaultDOP(DEFAULT_WAREHOUSE_ID));

        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, 24);
        Assertions.assertEquals(24, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getSinkDefaultDOP(DEFAULT_WAREHOUSE_ID));

        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, 32);
        Assertions.assertEquals(32, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getSinkDefaultDOP(DEFAULT_WAREHOUSE_ID));

        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, 48);
        Assertions.assertEquals(48, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(12, BackendResourceStat.getInstance().getSinkDefaultDOP(DEFAULT_WAREHOUSE_ID));

        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, 64);
        Assertions.assertEquals(64, BackendResourceStat.getInstance().getAvgNumCoresOfBe());
        Assertions.assertEquals(16, BackendResourceStat.getInstance().getSinkDefaultDOP(DEFAULT_WAREHOUSE_ID));
    }

    @Test
    public void backendStatusJsonPreservesShowBackendsContract() {
        // SHOW BACKENDS / SHOW PROC '/backends' surfaces BackendStatus as a JSON
        // string in the "Status" column. The documented shape is
        //   {"lastSuccessReportTabletsTime":"<formatted>"} or "N/A" sentinel
        // independent of how the timestamp is stored internally. Assertions are
        // structural via JsonParser so a future change to Gson's whitespace or
        // key ordering does not falsely fail this test - what matters is the
        // key name, value type, and sentinel.
        Backend.BackendStatus neverReported = new Backend.BackendStatus();
        JsonObject neverReportedJson =
                JsonParser.parseString(neverReported.toShowBackendsJson()).getAsJsonObject();
        Assertions.assertEquals("N/A",
                neverReportedJson.get("lastSuccessReportTabletsTime").getAsString());

        Backend.BackendStatus reported = new Backend.BackendStatus();
        // 2015-03-12 10:00:00 +08:00 -> 1426125600000L
        reported.lastSuccessReportTabletsTimeMs = 1426125600000L;
        JsonObject reportedJson =
                JsonParser.parseString(reported.toShowBackendsJson()).getAsJsonObject();
        Assertions.assertTrue(reportedJson.has("lastSuccessReportTabletsTime"),
                "JSON key must remain the documented name");
        // Don't pin the exact wall-clock - it follows the caller's session
        // timezone via TimeUtils.longToTimeString(long). What matters for the
        // SQL-surface contract is that the value is a JSON string, not a number.
        Assertions.assertTrue(
                reportedJson.get("lastSuccessReportTabletsTime").getAsJsonPrimitive().isString(),
                "JSON value must be a string, not a number");
    }
}
