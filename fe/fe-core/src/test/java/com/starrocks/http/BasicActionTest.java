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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BasicActionTest {

    @Test
    public void testParseAuthInfo() {
        BaseAction.ActionAuthorizationInfo authInfo =
                BaseAction.parseAuthInfo("abc", "123", "127.0.0.1");
        verifyAuthInfo(BaseAction.ActionAuthorizationInfo.of(
                "abc", "123", "127.0.0.1"), authInfo);

        authInfo = BaseAction.parseAuthInfo("test@cluster_id", "", "192.168.19.10");
        verifyAuthInfo(BaseAction.ActionAuthorizationInfo.of(
                "test", "", "192.168.19.10"), authInfo);
    }

    private void verifyAuthInfo(BaseAction.ActionAuthorizationInfo expect, BaseAction.ActionAuthorizationInfo actual) {
        assertEquals(expect.fullUserName, actual.fullUserName);
        assertEquals(expect.remoteIp, actual.remoteIp);
        assertEquals(expect.password, actual.password);
    }

    @Test
    public void testSanitizeHttpReqUri() throws Exception {
        String uri = "/api/bootstrap?cluster_id=123124&token=98sdf-80sdf";
        String result = WebUtils.sanitizeHttpReqUri(uri);
        System.out.println(result);
        Assert.assertTrue(result.contains("token=*"));
        Assert.assertTrue(result.contains("cluster_id=*"));

        uri = "/api/get_small_file?file_id=123124";
        result = WebUtils.sanitizeHttpReqUri(uri);
        Assert.assertTrue(result.contains("file_id=*"));

        uri = "/api/metrics?all=true";
        result = WebUtils.sanitizeHttpReqUri(uri);
        Assert.assertEquals(uri, result);
    }
}
