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

<<<<<<< HEAD
import com.google.common.collect.Lists;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
import mockit.Expectations;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import okhttp3.Request;
import okhttp3.Response;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertTrue;

public class ShowMetaInfoActionTest extends StarRocksHttpTestCase {

<<<<<<< HEAD
=======
    private static final long DB_ID = 1000 + testDbId;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    private static final String DB_NAME = "TEST_DB";
    private static final String TABLE_NAME = "TEST_TABLE";
    private static final long EXPECTED_SINGLE_REPLICA_SIZE = 1024L;
    private static final int HTTP_SLOW_REQUEST_THRESHOLD_MS = Config.http_slow_request_threshold_ms;

    @Override
    public void doSetUp() {
<<<<<<< HEAD
        Database db = new Database(1000 + TEST_DB_ID, DB_NAME);
        OlapTable table = newTable(TABLE_NAME, EXPECTED_SINGLE_REPLICA_SIZE);
        db.registerTableUnlocked(table);

        ConcurrentHashMap<String, Database> fullNameToDb = GlobalStateMgr.getCurrentState().getFullNameToDb();
        fullNameToDb.put(DB_NAME, db);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getDbNames();
                minTimes = 0;
                result = Lists.newArrayList(DB_NAME);

                GlobalStateMgr.getCurrentState().getDb(DB_NAME);
                minTimes = 0;
                result = db;

                db.getTables();
                minTimes = 0;
                result = Lists.newArrayList(table);
            }
        };
=======
        Database db = new Database(DB_ID, DB_NAME);
        OlapTable table = newTable(TABLE_NAME, EXPECTED_SINGLE_REPLICA_SIZE);
        db.registerTableUnlocked(table);

        // inject our test db
        ConcurrentHashMap<String, Database> fullNameToDb = GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getFullNameToDb();
        fullNameToDb.put(DB_NAME, db);

        ConcurrentHashMap<Long, Database> idToDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb();
        idToDb.put(DB_ID, db);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @After
    public void tearDown() {
        Config.http_slow_request_threshold_ms = HTTP_SLOW_REQUEST_THRESHOLD_MS;
    }

    @Test
    public void testShowDBSize() throws IOException {
<<<<<<< HEAD
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        Config.http_slow_request_threshold_ms = 0;
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/show_meta_info?action=SHOW_DB_SIZE")
                .build();
        Response response = networkClient.newCall(request).execute();
        assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        Gson gson = new Gson();
        Map<String, Long> dbSizeResult = gson.fromJson(respStr, new TypeToken<HashMap<String, Long>>() {
        }.getType());
        // SHOW_DB_SIZE only considers table size with one single replica
        Assert.assertEquals(Optional.of(EXPECTED_SINGLE_REPLICA_SIZE).get(), dbSizeResult.get(DB_NAME));
    }

    @Test
    public void testShowFullDBSize() throws IOException {
        Config.http_slow_request_threshold_ms = 0;
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/show_meta_info?action=SHOW_FULL_DB_SIZE")
                .build();
        Response response = networkClient.newCall(request).execute();
        assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        Gson gson = new Gson();
        Map<String, Long> dbSizeResult = gson.fromJson(respStr, new TypeToken<HashMap<String, Long>>() {
        }.getType());
        // SHOW_FULL_DB_SIZE considers table size with all replicas
        Assert.assertEquals(Optional.of(EXPECTED_SINGLE_REPLICA_SIZE * 3).get(), dbSizeResult.get(DB_NAME));
    }
<<<<<<< HEAD
}
=======
}
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
