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

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.owasp.encoder.Encode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProcProfileActionTest extends StarRocksHttpTestCase {

    private final List<String> fetchedUrls = new ArrayList<>();
    private String beResponse = "{\"profiles\":[]}";

    @BeforeEach
    public void mockBeHttp() {
        fetchedUrls.clear();
        new MockUp<HttpUtils>() {
            @Mock
            public String get(String uri, Map<String, String> header) {
                fetchedUrls.add(uri);
                return beResponse;
            }
        };
    }

    private String getPage(String node) throws IOException {
        HttpUrl url = HttpUrl.parse(BASE_URL + "/proc_profile").newBuilder()
                .addQueryParameter("node", node)
                .build();
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(url)
                .build();
        Response response = networkClient.newCall(request).execute();
        String body = response.body().string();
        Assertions.assertEquals(200, response.code(), body);
        return body;
    }

    @Test
    public void testInvalidNodeParameterIsHtmlEncoded() throws IOException {
        String body = getPage("<img src=x onerror=\"alert(1)\">");
        Assertions.assertFalse(body.contains("<img src=x"), body);
        Assertions.assertTrue(
                body.contains("Invalid node parameter: &lt;img src=x onerror=&#34;alert(1)&#34;&gt;"), body);
    }

    @Test
    public void testUnknownBackendIsNotFetched() throws IOException {
        String body = getPage("BE:attacker.example.com:80");
        Assertions.assertTrue(body.contains("Invalid node parameter: BE:attacker.example.com:80"), body);
        Assertions.assertTrue(fetchedUrls.isEmpty(), fetchedUrls.toString());
    }

    @Test
    public void testBackendProfileListIsEncoded() throws IOException {
        beResponse = "{\"profiles\":[{\"filename\":\"cpu-profile-\\\"><img src=x>.tar.gz\","
                + "\"type\":\"<b>CPU</b>\",\"timestamp\":\"20261014-120000\",\"size\":1}]}";
        // A backend registered by StarRocksHttpTestCase.
        Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(testBackendId1);
        String nodeId = "BE:" + backend.getHost() + ":" + backend.getHttpPort();
        String body = getPage(nodeId);

        Assertions.assertEquals(
                List.of("http://" + backend.getHost() + ":" + backend.getHttpPort() + "/api/proc_profile/list"), fetchedUrls);
        Assertions.assertFalse(body.contains("<img src=x>"), body);
        Assertions.assertFalse(body.contains("<b>CPU</b>"), body);
        Assertions.assertTrue(body.contains("<td>&lt;b&gt;CPU&lt;/b&gt;</td>"), body);
        Assertions.assertTrue(body.contains("<a href=\"/proc_profile/file?node=" + Encode.forUriComponent(nodeId)
                + "&amp;filename=cpu-profile-%22%3E%3Cimg%20src%3Dx%3E.tar.gz\" target=\"_blank\">View</a>"), body);
    }
}
