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
import com.starrocks.common.StarRocksException;
import com.starrocks.http.rest.ActionStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Expectations;
import mockit.Mocked;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CancelStreamLoadActionTest extends StarRocksHttpTestCase {

    private static final String OK = ActionStatus.OK.name();
    private static final String FAILED = ActionStatus.FAILED.name();

    @Mocked
    private GlobalTransactionMgr globalTransactionMgr;

    @Test
    public void testNormal() throws IOException, StarRocksException {
        // POST /api/{db}/{table}/_cancel?label={label} is working
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        String label = UUID.randomUUID().toString();
        new Expectations() {
            {
                globalTransactionMgr.abortTransaction(anyLong, anyString, anyString);
                times = 1;
            }
        };
        Request request = new Request.Builder()
                .post(RequestBody.create(JSON, "[]"))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + String.format("/api/%s/tbl1/_cancel?label=%s", db.getFullName(), label))
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            Map<String, Object> body = parseResponseBody(response);
            assertEquals(OK, body.get("status"));
        }
    }

    @Test
    public void testLabelNotExist() throws IOException, StarRocksException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        String label = UUID.randomUUID().toString();
        new Expectations() {
            {
                globalTransactionMgr.abortTransaction(anyLong, anyString, anyString);
                times = 1;
                result = new StarRocksException("label not exist");
            }
        };
        Request request = new Request.Builder()
                .post(RequestBody.create(JSON, "[]"))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + String.format("/api/%s/tbl1/_cancel?label=%s", db.getFullName(), label))
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            Map<String, Object> body = parseResponseBody(response);
            assertEquals(FAILED, body.get("status"));
            assertEquals("label not exist", body.get("msg"));
        }
    }
}
