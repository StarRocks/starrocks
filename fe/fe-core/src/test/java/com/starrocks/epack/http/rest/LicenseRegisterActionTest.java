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
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.epack.system.InvalidLicenseException;
import com.starrocks.epack.system.LicenseMgr;
import com.starrocks.http.StarRocksHttpTestCase;
import com.starrocks.qe.ConnectContext;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;

public class LicenseRegisterActionTest extends StarRocksHttpTestCase {

    private Request buildPost(String url, String body, String userName) {
        RequestBody requestBody = RequestBody.create(body == null ? "" : body, JSON);
        Request.Builder builder = new Request.Builder().post(requestBody).url(url);
        if (userName != null) {
            builder.addHeader("Authorization", Credentials.basic(userName, ""));
        }
        return builder.build();
    }

    private void mockRootRole() {
        new MockUp<AuthorizationMgr>() {
            @Mock
            public static Set<Long> getOwnedRolesByUser(UserIdentity userIdentity) throws PrivilegeException {
                return Set.of(PrivilegeBuiltinConstants.ROOT_ROLE_ID);
            }
        };
    }

    @Test
    public void testRegisterLicenseMissingBody() throws IOException {
        mockRootRole();
        Request request = buildPost(BASE_URL + LicenseRegisterAction.URI, "", "root");
        try (Response response = networkClient.newCall(request).execute()) {
            Assertions.assertEquals(400, response.code());
            String resp = response.body().string();
            Assertions.assertTrue(resp.contains("license is required"));
        }
    }

    @Test
    public void testRegisterLicenseSuccess() throws Exception {
        mockRootRole();

        new MockUp<LicenseMgr>() {
            @Mock
            public void registerLicense(String license) throws InvalidLicenseException {
                // no-op for success
            }
        };

        Request request = buildPost(BASE_URL + LicenseRegisterAction.URI, "dummy-license", "root");
        try (Response response = networkClient.newCall(request).execute()) {
            Assertions.assertEquals(200, response.code());
        }
    }

    @Test
    public void testRegisterLicenseInvalid() throws Exception {
        mockRootRole();

        new MockUp<LicenseMgr>() {
            @Mock
            public void registerLicense(String license) throws InvalidLicenseException {
                throw new InvalidLicenseException("bad license");
            }
        };

        Request request = buildPost(BASE_URL + LicenseRegisterAction.URI, "invalid-license", "root");
        try (Response response = networkClient.newCall(request).execute()) {
            Assertions.assertEquals(400, response.code());
            String resp = response.body().string();
            Assertions.assertTrue(resp.contains("bad license"));
        }
    }

    @Test
    public void testRegisterLicenseUnauthorized() throws IOException {
        Request request = buildPost(BASE_URL + LicenseRegisterAction.URI, "some-license", null);
        try (Response response = networkClient.newCall(request).execute()) {
            Assertions.assertEquals(401, response.code());
        }
    }

    @Test
    public void testRegisterLicenseUsingClusterAdminRole() throws Exception {
        new MockUp<AuthorizationMgr>() {
            @Mock
            public static Set<Long> getOwnedRolesByUser(UserIdentity userIdentity) throws PrivilegeException {
                return Set.of(PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_ID);
            }
        };

        new MockUp<LicenseMgr>() {
            @Mock
            public void registerLicense(String license) throws InvalidLicenseException {
                // no-op for success
            }
        };

        new MockUp<AuthenticationHandler>() {
            @Mock
            public static UserIdentity authenticate(ConnectContext context, String user, String remoteHost, byte[] authResponse)
                    throws AuthenticationException {
                UserIdentity userIdentity = new UserIdentity("user1", "%");
                context.setCurrentUserIdentity(userIdentity);
                return userIdentity;
            }
        };

        Request request = buildPost(BASE_URL + LicenseRegisterAction.URI, "some-license", "user1");
        try (Response response = networkClient.newCall(request).execute()) {
            Assertions.assertEquals(200, response.code());
        }
    }

    @Test
    public void testRegisterLicenseUsingDbAdminRole() throws Exception {
        new MockUp<AuthorizationMgr>() {
            @Mock
            public static Set<Long> getOwnedRolesByUser(UserIdentity userIdentity) throws PrivilegeException {
                return Set.of(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID);
            }
        };

        new MockUp<LicenseMgr>() {
            @Mock
            public void registerLicense(String license) throws InvalidLicenseException {
                // no-op for success
            }
        };

        new MockUp<AuthenticationHandler>() {
            @Mock
            public static UserIdentity authenticate(ConnectContext context, String user, String remoteHost, byte[] authResponse)
                    throws AuthenticationException {
                UserIdentity userIdentity = new UserIdentity("user1", "%");
                context.setCurrentUserIdentity(userIdentity);
                return userIdentity;
            }
        };

        Request request = buildPost(BASE_URL + LicenseRegisterAction.URI, "some-license", "user1");
        try (Response response = networkClient.newCall(request).execute()) {
            Assertions.assertEquals(401, response.code());
        }
    }

    @Test
    public void testRegisterLicensePrivilegeException() throws Exception {
        new MockUp<AuthorizationMgr>() {
            @Mock
            public static Set<Long> getOwnedRolesByUser(UserIdentity userIdentity) throws PrivilegeException {
                throw new PrivilegeException("User does not exist");
            }
        };

        new MockUp<LicenseMgr>() {
            @Mock
            public void registerLicense(String license) throws InvalidLicenseException {
                // no-op for success
            }
        };

        new MockUp<AuthenticationHandler>() {
            @Mock
            public static UserIdentity authenticate(ConnectContext context, String user, String remoteHost, byte[] authResponse)
                    throws AuthenticationException {
                UserIdentity userIdentity = new UserIdentity("user1", "%");
                context.setCurrentUserIdentity(userIdentity);
                return userIdentity;
            }
        };

        Request request = buildPost(BASE_URL + LicenseRegisterAction.URI, "some-license", "user1");
        try (Response response = networkClient.newCall(request).execute()) {
            Assertions.assertEquals(401, response.code());
        }
    }
}
