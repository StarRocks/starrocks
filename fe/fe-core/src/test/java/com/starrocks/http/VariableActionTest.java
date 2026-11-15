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
import com.starrocks.common.ConfigBase;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

public class VariableActionTest extends StarRocksHttpTestCase {

    // '$1' also checks that the page title is substituted literally rather than as a regex replacement.
    private static final String CLUSTER_NAME = "<b>$1 cluster</b>";
    private static final String CLUSTER_NAME_HTML = "&lt;b&gt;$1 cluster&lt;/b&gt;";

    private final String originalClusterName = Config.cluster_name;

    @AfterEach
    public void restoreClusterName() {
        Config.cluster_name = originalClusterName;
    }

    @Test
    public void testConfigValuesAndTitleAreHtmlEncoded() throws IOException {
        Config.cluster_name = CLUSTER_NAME;
        // Config.dump() fails on some unset array configs in the UT environment, so serve a fixed dump.
        new MockUp<ConfigBase>() {
            @Mock
            public HashMap<String, String> dump() {
                HashMap<String, String> conf = new HashMap<>();
                conf.put("cluster_name", Config.cluster_name);
                return conf;
            }
        };
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/variable")
                .build();
        Response response = networkClient.newCall(request).execute();
        String body = response.body().string();
        Assertions.assertEquals(200, response.code(), body);

        Assertions.assertFalse(body.contains(CLUSTER_NAME), body);
        Assertions.assertTrue(body.contains("<title>" + CLUSTER_NAME_HTML + "</title>"), body);
        Assertions.assertTrue(body.contains("cluster_name=" + CLUSTER_NAME_HTML), body);
    }
}
