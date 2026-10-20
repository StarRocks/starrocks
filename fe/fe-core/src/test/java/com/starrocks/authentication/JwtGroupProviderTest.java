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

package com.starrocks.authentication;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class JwtGroupProviderTest {

    private static final String TEST_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gR"
            + "G9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTc4NzMyMDEwMSwiY2xhaW1MaXN0IjpbInJvbGUxIiwicm9sZTIiXSwiY2xhaW1TaW5nbGUiOiJyb2xlMy"
            + "IsImRlZXAiOnsiY2xhaW0iOlsicm9sZTQiXX19.H4CJBofuNvdz_SRNNhl4BCOyqSWK_1xoOMeBg2Qg6HM";

    @Test
    public void testListClaim() throws DdlException {
        ConnectContext.get().setAuthToken(TEST_TOKEN);

        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String groupName = "jwt_group_provider";
        Map<String, String> properties = Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "jwt",
                JWTGroupProvider.JWT_CLAIM, "claimList");

        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        JWTGroupProvider jwtGroupProvider = (JWTGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Set<String> groups = jwtGroupProvider.getGroup(new UserIdentity("harbor", "%"), "harbor");
        Assertions.assertTrue(groups.contains("role1"));
        Assertions.assertTrue(groups.contains("role2"));
    }

    @Test
    public void testSingleClaim() throws DdlException {
        ConnectContext.get().setAuthToken(TEST_TOKEN);

        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String groupName = "jwt_group_provider";
        Map<String, String> properties = Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "jwt",
                JWTGroupProvider.JWT_CLAIM, "claimSingle");

        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        JWTGroupProvider jwtGroupProvider = (JWTGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Set<String> groups = jwtGroupProvider.getGroup(new UserIdentity("harbor", "%"), "harbor");
        Assertions.assertTrue(groups.contains("role3"));
    }

    @Test
    public void testDeepClaim() throws DdlException {
        ConnectContext.get().setAuthToken(TEST_TOKEN);

        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String groupName = "jwt_group_provider";
        Map<String, String> properties = Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "jwt",
                JWTGroupProvider.JWT_CLAIM, "deep.claim");

        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        JWTGroupProvider jwtGroupProvider = (JWTGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Set<String> groups = jwtGroupProvider.getGroup(new UserIdentity("harbor", "%"), "harbor");
        Assertions.assertTrue(groups.contains("role4"));
    }
}
