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

package com.starrocks.epack.http.rest;

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.epack.system.LicenseInfo;
import com.starrocks.epack.system.LicenseMgr;
import com.starrocks.http.StarRocksHttpTestCase;
import com.starrocks.qe.ConnectContext;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class LicenseListActionTest extends StarRocksHttpTestCase {

    private boolean savedEnableHttpAuth;

    @BeforeEach
    public void saveFlag() {
        savedEnableHttpAuth = Config.enable_http_auth;
    }

    @AfterEach
    public void restoreFlag() {
        Config.enable_http_auth = savedEnableHttpAuth;
    }

    private void mockLicenseList() {
        new MockUp<LicenseMgr>() {
            @Mock
            public List<LicenseInfo> getAllLicenseInfo() {
                return List.of(new LicenseInfo("system-id-1"));
            }
        };
    }

    private Request get(String userName) {
        Request.Builder builder = new Request.Builder().get().url(BASE_URL + LicenseListAction.URI);
        if (userName != null) {
            builder.addHeader("Authorization", Credentials.basic(userName, ""));
        }
        return builder.build();
    }

    @Test
    public void testListUnauthorized() throws IOException {
        // No credentials -> Basic auth always required (needAuth() == true), regardless of the flag.
        try (Response response = networkClient.newCall(get(null)).execute()) {
            Assertions.assertEquals(401, response.code());
        }
    }

    @Test
    public void testListFlagOffAllowsAnyAuthenticatedUser() throws IOException {
        // Backward compatible: when enable_http_auth is off, no RBAC is enforced.
        Config.enable_http_auth = false;
        mockLicenseList();
        try (Response response = networkClient.newCall(get("root")).execute()) {
            Assertions.assertEquals(200, response.code());
        }
    }

    @Test
    public void testListFlagOnGroupDerivedClusterAdminAllowed() throws IOException {
        // The cluster_admin role is granted via a group mapping: it lands on the context's
        // currentRoleIds (as AuthenticationHandler.setCurrentRoleIds(user, groups) does) even though
        // it is not directly owned by the user. The check must honor the context roles.
        Config.enable_http_auth = true;
        mockLicenseList();
        new MockUp<AuthenticationHandler>() {
            @Mock
            public static UserIdentity authenticate(ConnectContext context, String user, String remoteHost,
                                                    byte[] authResponse) throws AuthenticationException {
                UserIdentity userIdentity = new UserIdentity("user1", "%");
                context.setCurrentUserIdentity(userIdentity);
                context.setGroups(Set.of("ldap_cluster_admins"));
                context.setCurrentRoleIds(Set.of(PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_ID));
                return userIdentity;
            }
        };
        try (Response response = networkClient.newCall(get("user1")).execute()) {
            Assertions.assertEquals(200, response.code());
        }
    }

    @Test
    public void testListFlagOnNonAdminDenied() throws IOException {
        Config.enable_http_auth = true;
        mockLicenseList();
        new MockUp<AuthenticationHandler>() {
            @Mock
            public static UserIdentity authenticate(ConnectContext context, String user, String remoteHost,
                                                    byte[] authResponse) throws AuthenticationException {
                UserIdentity userIdentity = new UserIdentity("user1", "%");
                context.setCurrentUserIdentity(userIdentity);
                context.setCurrentRoleIds(Set.of());
                return userIdentity;
            }
        };
        try (Response response = networkClient.newCall(get("user1")).execute()) {
            Assertions.assertEquals(401, response.code());
        }
    }
}
