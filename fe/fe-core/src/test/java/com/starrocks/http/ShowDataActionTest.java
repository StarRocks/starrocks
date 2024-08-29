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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertTrue;

public class ShowDataActionTest extends StarRocksHttpTestCase {

    private static final String SHOW_DATA_URI = "/show_data";
    private static final String SHOW_DATA_DB_NAME = "showdatadb";

    private static final String SHOW_DATA_TABLE_NAME_SECOND = "ShowDataTable2";

    private static final String SHOW_DATA_TABLE_NAME = "ShowDataTable";
    private long expectedShowDataTableSize = 0;

    @Override
    public void doSetUp() {
        Database db = new Database(1000 + testDbId, SHOW_DATA_DB_NAME);
        OlapTable table = newTable(SHOW_DATA_TABLE_NAME);
        db.registerTableUnlocked(table);

        OlapTable olapTable = newTable(SHOW_DATA_TABLE_NAME_SECOND);
        db.registerTableUnlocked(olapTable);

        expectedShowDataTableSize = olapTable.getDataSize();

        Database db1 = new Database(1000 + testDbId, SHOW_DATA_DB_NAME + "!");

        db1.registerTableUnlocked(olapTable);


        // inject our test db
        ConcurrentHashMap<String, Database> fullNameToDb = GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getFullNameToDb();
        fullNameToDb.put(SHOW_DATA_DB_NAME, db);
        fullNameToDb.put(SHOW_DATA_DB_NAME + "1", db1);
    }

    @Test
    public void testGetShowData() throws IOException {
        Config.http_slow_request_threshold_ms = 0;
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api" + SHOW_DATA_URI + "?db=" + SHOW_DATA_DB_NAME)
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
            Assert.assertNotNull(response.body());
            String respStr = response.body().string();
            Assert.assertNotNull(respStr);
            Assert.assertEquals(String.valueOf(expectedShowDataTableSize), respStr);
        }
    }

    @Test
    public void testShowDbAndTableSize() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api" + SHOW_DATA_URI + "?db=" + SHOW_DATA_DB_NAME + "&table=" + SHOW_DATA_TABLE_NAME_SECOND)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
            Assert.assertNotNull(response.body());
            String respStr = response.body().string();
            Assert.assertNotNull(respStr);
            Assert.assertEquals(String.valueOf(expectedShowDataTableSize), respStr);
        }
    }

    @Test
    public void testShowTableSize() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api" + SHOW_DATA_URI + "?table=" + SHOW_DATA_TABLE_NAME_SECOND)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
            Assert.assertNotNull(response.body());
            String respStr = response.body().string();
            Assert.assertNotNull(respStr);
            Assert.assertEquals(String.valueOf(expectedShowDataTableSize * 2), respStr);
        }
    }

    @Test
    public void testShowAllSize() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api" + SHOW_DATA_URI + "?db=" + SHOW_DATA_DB_NAME)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
            Assert.assertNotNull(response.body());
            String respStr = response.body().string();
            Assert.assertNotNull(respStr);
            Assert.assertEquals(String.valueOf(expectedShowDataTableSize), respStr);
        }
    }
}
