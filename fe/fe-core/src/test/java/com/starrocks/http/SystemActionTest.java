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

import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.http.action.WebBaseAction;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SystemActionTest extends StarRocksHttpTestCase {

    private static final String QUERY_PLAN_URI = "/system";

    private void sendHttp(String path) throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI + "?path=" + path)
                .build();
        Response response = networkClient.newCall(request).execute();
        assertTrue(response.isSuccessful());
        String respStr = response.body().string();
        Assertions.assertNotNull(respStr);
    }

    @Test
    public void testGetSystemInfo() throws IOException {
        sendHttp("");
        sendHttp("/");
        sendHttp("//");
        sendHttp("///");
        sendHttp("backends");
        sendHttp("/backends");
        sendHttp("//backends");
        sendHttp("///backends");
        sendHttp("backends/aaa");
        sendHttp("backends//aaa");
        sendHttp("backends///aaa");
        sendHttp("/backends/aaa");
        sendHttp("/backends//aaa");
        sendHttp("/backends///aaa");
        sendHttp("//backends/aaa");
        sendHttp("//backends//aaa");
        sendHttp("//backends///aaa");
        sendHttp("//backends/aaa/aaa");
        sendHttp("//backends//aaa////");
        sendHttp("//backends///aaa/aaa///");
    }

    private String getPage(String path) throws IOException {
        HttpUrl url = HttpUrl.parse("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI).newBuilder()
                .addQueryParameter("path", path)
                .build();
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(url)
                .build();
        Response response = networkClient.newCall(request).execute();
        String body = response.body().string();
        assertTrue(response.isSuccessful(), body);
        return body;
    }

    @Test
    public void testPathIsHtmlEncoded() throws IOException {
        String payload = "/<img src=x onerror=\"alert(1)\">";
        String body = getPage(payload);
        Assertions.assertFalse(body.contains(payload), body);
        // shown in "Current path" and in the "No such proc path" error
        Assertions.assertTrue(body.contains("/&lt;img src=x onerror=&#34;alert(1)&#34;&gt;"), body);
    }

    @Test
    public void testProcPathIsLookedUpUnencoded() throws IOException {
        List<String> lookedUpPaths = new ArrayList<>();
        new MockUp<WebBaseAction>() {
            @Mock
            protected ProcNodeInterface getProcNode(String path) {
                lookedUpPaths.add(path);
                return null;
            }
        };

        String path = "/dbs/10001/tbl_a&b";
        String body = getPage(path);
        Assertions.assertEquals(List.of(path), lookedUpPaths);
        Assertions.assertTrue(body.contains("No such proc path[/dbs/10001/tbl_a&amp;b]"), body);
        Assertions.assertTrue(body.contains("<a href=\"?path=/dbs/10001\""), body);
    }
}
