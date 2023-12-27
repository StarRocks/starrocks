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

import com.starrocks.common.DdlException;
import com.starrocks.lake.StarMgrMetaSyncer;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class SyncCloudTableMetaTest extends StarRocksHttpTestCase {
    @Test
    public void testSyncCloudTableMeta() throws IOException {
        new MockUp<StarMgrMetaSyncer>() {
            @Mock
            public void syncTableMeta(String dbName, String tableName, boolean force) throws DdlException {
            }
        };

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/test_db/test_table/_sync_meta?force=true")
                .build();
        Response response = networkClient.newCall(request).execute();
        assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        Assert.assertEquals("OK", respStr);
    }
}
