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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionStateSnapshot;
import com.starrocks.transaction.TransactionStatus;
import mockit.Expectations;
import mockit.Mocked;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class GetStreamLoadStateTest  extends StarRocksHttpTestCase {

    private final OkHttpClient client = new OkHttpClient.Builder()
            .readTimeout(100, TimeUnit.SECONDS)
            .followRedirects(true)
            .build();

    @Mocked
    private GlobalTransactionMgr globalTransactionMgr;

    @Test
    public void testSuccessLoad() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        String label = UUID.randomUUID().toString();
        new Expectations() {
            {
                globalTransactionMgr.getLabelStatus(db.getId(), label);
                times = 1;
                result = new TransactionStateSnapshot(TransactionStatus.PREPARE, "");
            }
        };
        verifyResult(label, TransactionStatus.PREPARE, "");

        new Expectations() {
            {
                globalTransactionMgr.getLabelStatus(db.getId(), label);
                times = 1;
                result = new TransactionStateSnapshot(TransactionStatus.PREPARED, "");
            }
        };
        verifyResult(label, TransactionStatus.PREPARED, "");

        new Expectations() {
            {
                globalTransactionMgr.getLabelStatus(db.getId(), label);
                times = 1;
                result = new TransactionStateSnapshot(TransactionStatus.COMMITTED, "");
            }
        };
        verifyResult(label, TransactionStatus.COMMITTED, "");

        new Expectations() {
            {
                globalTransactionMgr.getLabelStatus(db.getId(), label);
                times = 1;
                result = new TransactionStateSnapshot(TransactionStatus.VISIBLE, "");
            }
        };
        verifyResult(label, TransactionStatus.VISIBLE, "");
    }

    @Test
    public void testAbortedState() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        String label = UUID.randomUUID().toString();
        new Expectations() {
            {
                globalTransactionMgr.getLabelStatus(db.getId(), label);
                times = 1;
                result = new TransactionStateSnapshot(TransactionStatus.ABORTED, "artificial failure");
            }
        };
        verifyResult(label, TransactionStatus.ABORTED, "artificial failure");
    }

    @Test
    public void testUnknownState() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        String label = UUID.randomUUID().toString();
        new Expectations() {
            {
                globalTransactionMgr.getLabelStatus(db.getId(), label);
                times = 1;
                result = new TransactionStateSnapshot(TransactionStatus.UNKNOWN, null);
            }
        };
        verifyResult(label, TransactionStatus.UNKNOWN, null);
    }

    private void verifyResult(String label, TransactionStatus expectedStatus, String expectedReason) throws Exception {
        Request request = new Request.Builder()
                .addHeader("Authorization", rootAuth)
                .url(String.format("%s/api/%s/get_load_state?label=%s", BASE_URL, DB_NAME, label))
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertEquals(200, response.code());
            Map<String, Object> result = parseResponseBody(response);
            assertEquals("0", result.get("code"));
            assertEquals("OK", result.get("status"));
            assertEquals("OK", result.get("message"));
            assertEquals("Success", result.get("msg"));
            assertEquals(expectedStatus.name(), result.get("state"));
            assertEquals(expectedReason, result.get("reason"));
        }
    }
}
