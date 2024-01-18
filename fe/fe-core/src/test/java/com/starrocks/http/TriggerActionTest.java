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

package com.starrocks.http;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TriggerActionTest extends StarRocksHttpTestCase {
    private static final String TRIGGER_EXECUTE_API = "/api/trigger?type=dynamic_partition&db=test_trigger&tbl=site_access";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @Override
    @Before
    public void setUp() {
        MetricRepo.init();
        ExecuteEnv.setup();
        FeConstants.runningUnitTest = true;
        Config.dynamic_partition_enable = true;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        try {
            UtFrameUtils.addMockBackend(10002);
            connectContext = UtFrameUtils.createDefaultCtx();
            starRocksAssert = new StarRocksAssert(connectContext);

            starRocksAssert.withDatabase("test_trigger").useDatabase("test_trigger")
                    .withTable("CREATE TABLE site_access(\n" +
                            "event_day DATE,\n" +
                            "site_id INT DEFAULT '10',\n" +
                            "city_code VARCHAR(100),\n" +
                            "user_name VARCHAR(32) DEFAULT '',\n" +
                            "pv BIGINT DEFAULT '0'\n" +
                            ")\n" +
                            "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                            "PARTITION BY RANGE(event_day)(\n" +
                            "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                            "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                            "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                            "PARTITION p20200324 VALUES LESS THAN (\"2020-03-25\")\n" +
                            ")\n" +
                            "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                            "PROPERTIES(\n" +
                            "    \"replication_num\" = \"1\",\n" +
                            "    \"dynamic_partition.enable\" = \"true\",\n" +
                            "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                            "    \"dynamic_partition.start\" = \"-1\",\n" +
                            "    \"dynamic_partition.end\" = \"3\",\n" +
                            "    \"dynamic_partition.prefix\" = \"p\",\n" +
                            "    \"dynamic_partition.history_partition_num\" = \"0\"\n" +
                            ");");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testTriggerDynamicFailed() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/trigger?type=dynamic_partition")
                .build();
        Response response = networkClient.newCall(request).execute();

        assertFalse(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        Assert.assertEquals("Missing params. Need database name", respStr);

        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/trigger?type=abc")
                .build();
        response = networkClient.newCall(request).execute();
        assertFalse(response.isSuccessful());
        Assert.assertNotNull(response.body());
        respStr = response.body().string();
        Assert.assertNotNull(respStr);
        Assert.assertEquals("trigger type: abc is invalid!only support dynamic_partition", respStr);


        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/trigger?type=dynamic_partition&db=test_not_exist")
                .build();
        response = networkClient.newCall(request).execute();
        assertFalse(response.isSuccessful());
        Assert.assertNotNull(response.body());
        respStr = response.body().string();
        Assert.assertNotNull(respStr);
        Assert.assertEquals("Database[test_not_exist] does not exist", respStr);


        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/trigger?type=dynamic_partition&db=test_trigger&tbl=table_not_exist")
                .build();
        response = networkClient.newCall(request).execute();
        assertFalse(response.isSuccessful());
        Assert.assertNotNull(response.body());
        respStr = response.body().string();
        Assert.assertNotNull(respStr);
        Assert.assertEquals("Table[table_not_exist] does not exist", respStr);
    }

    @Test
    public void testTriggerDynamicSuccess() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + TRIGGER_EXECUTE_API)
                .build();
        Response response = networkClient.newCall(request).execute();

        assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        Assert.assertEquals("Success", respStr);
    }

}
