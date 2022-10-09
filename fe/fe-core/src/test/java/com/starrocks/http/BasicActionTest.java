// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.http;

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
}
