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
    private long expectedSize = 0;

    @Override
    public void doSetUp() {
        Database db = new Database(1000 + testDbId, SHOW_DATA_DB_NAME);
        OlapTable table = newTable("ShowDataTable");
        db.registerTableUnlocked(table);
        expectedSize = table.getDataSize();

        // inject our test db
        ConcurrentHashMap<String, Database> fullNameToDb = GlobalStateMgr.getCurrentState().getFullNameToDb();
        fullNameToDb.put(SHOW_DATA_DB_NAME, db);
    }

    @Test
    public void testGetShowData() throws IOException {
        Config.http_slow_request_threshold_ms = 0;
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api" + SHOW_DATA_URI + "?db=" + SHOW_DATA_DB_NAME)
                .build();
        Response response = networkClient.newCall(request).execute();
        assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        Assert.assertEquals(String.valueOf(expectedSize), respStr);
    }
}
