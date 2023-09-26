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
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class IndexActionTest extends StarRocksHttpTestCase {

    private String sendHttp() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT)
                .build();
        Response response = networkClient.newCall(request).execute();
        assertTrue(response.isSuccessful());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        return respStr;
    }

    @Test
    public void testGetSystemInfo() throws IOException {
        Config.http_web_page_display_hardware = true;
        String before = sendHttp();
        Assert.assertTrue(before.contains("Uptime"));
        Config.http_web_page_display_hardware = false;
        String after = sendHttp();
        Assert.assertTrue(after.contains("Hardware info is disabled"));
    }

}
