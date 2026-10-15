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

import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * /dump, /dump_starmgr and /service_id are called by operators, not by peer FEs, so they skip the client-is-FE check
 * and must require an authenticated admin instead.
 */
public class MetaServiceDumpAuthTest extends StarRocksHttpTestCase {

    private static final String NON_ADMIN_USER = "dump_non_admin";
    private static final String NON_ADMIN_PASSWORD = "dump_non_admin_pwd";

    private final AtomicInteger dumpCount = new AtomicInteger();
    private final String nonAdminAuth = Credentials.basic(NON_ADMIN_USER, NON_ADMIN_PASSWORD);
    // Whether the non-admin user holds SYSTEM NODE, the privilege needAdmin() checks first.
    private volatile boolean nonAdminHasNode = false;

    @BeforeEach
    public void mockDumpAndNonAdminUser() {
        dumpCount.set(0);
        nonAdminHasNode = false;
        new MockUp<GlobalStateMgr>() {
            @Mock
            public String dumpImage() {
                dumpCount.incrementAndGet();
                return "/mock/image.1";
            }
        };

        // Authenticate the non-admin user with valid credentials; every other login goes through the real path.
        new MockUp<AuthenticationHandler>() {
            @Mock
            public UserIdentity authenticateWithClearPassword(Invocation inv, ConnectContext context, String user,
                                                              String remoteHost, String password) {
                if (NON_ADMIN_USER.equals(user) && NON_ADMIN_PASSWORD.equals(password)) {
                    return UserIdentity.createAnalyzedUserIdentWithIp(NON_ADMIN_USER, "%");
                }
                return inv.proceed();
            }
        };

        // The non-admin user owns no admin role and holds NODE only when nonAdminHasNode is set.
        new MockUp<Authorizer>() {
            @Mock
            public void checkSystemAction(Invocation inv, ConnectContext context, PrivilegeType privilegeType)
                    throws AccessDeniedException {
                if (NON_ADMIN_USER.equals(context.getCurrentUserIdentity().getUser())) {
                    if (privilegeType == PrivilegeType.NODE && nonAdminHasNode) {
                        return;
                    }
                    throw new AccessDeniedException("NODE on SYSTEM required");
                }
                inv.proceed();
            }
        };
        new MockUp<BaseAction>() {
            @Mock
            public void checkUserOwnsAdminRole(Invocation inv, UserIdentity currentUser) throws AccessDeniedException {
                if (NON_ADMIN_USER.equals(currentUser.getUser())) {
                    throw new AccessDeniedException();
                }
                inv.proceed();
            }
        };
    }

    private Response get(String path, String authorization) throws IOException {
        Request.Builder builder = new Request.Builder().get().url(BASE_URL + path);
        if (authorization != null) {
            builder.addHeader("Authorization", authorization);
        }
        return networkClient.newCall(builder.build()).execute();
    }

    @Test
    public void testDumpRequiresAuthentication() throws IOException {
        try (Response response = get("/dump", null)) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }
        try (Response response = get("/dump", Credentials.basic("root", "wrong_password"))) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }
        Assertions.assertEquals(0, dumpCount.get());

        try (Response response = get("/dump", rootAuth)) {
            String body = response.body().string();
            Assertions.assertEquals(200, response.code(), body);
            Assertions.assertTrue(body.contains("dump finished. /mock/image.1"), body);
        }
        Assertions.assertEquals(1, dumpCount.get());
    }

    @Test
    public void testDumpStarMgrRequiresAuthentication() throws IOException {
        try (Response response = get("/dump_starmgr", null)) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }
        try (Response response = get("/dump_starmgr", rootAuth)) {
            String body = response.body().string();
            Assertions.assertEquals(200, response.code(), body);
            // The UT cluster runs in shared-nothing mode.
            Assertions.assertTrue(body.contains("does not support dump starmgr meta"), body);
        }
    }

    @Test
    public void testServiceIdRequiresAuthentication() throws IOException {
        try (Response response = get("/service_id", null)) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }
        try (Response response = get("/service_id", rootAuth)) {
            String body = response.body().string();
            // The UT cluster runs in shared-nothing mode, so an authenticated admin gets 404 rather than an id.
            Assertions.assertEquals(404, response.code(), body);
            Assertions.assertTrue(body.contains("does not support service id"), body);
        }
    }

    @Test
    public void testNonAdminUserIsRejected() throws IOException {
        // Valid credentials of a user without NODE privilege or an admin role must not pass needAdmin().
        try (Response response = get("/dump", nonAdminAuth)) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }
        Assertions.assertEquals(0, dumpCount.get());
        try (Response response = get("/dump_starmgr", nonAdminAuth)) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }
        try (Response response = get("/service_id", nonAdminAuth)) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }

        // The same credentials pass once the user holds NODE, so the rejection above came from the admin check.
        nonAdminHasNode = true;
        try (Response response = get("/dump", nonAdminAuth)) {
            Assertions.assertEquals(200, response.code(), response.body().string());
        }
        Assertions.assertEquals(1, dumpCount.get());
        try (Response response = get("/dump_starmgr", nonAdminAuth)) {
            Assertions.assertEquals(200, response.code(), response.body().string());
        }
        try (Response response = get("/service_id", nonAdminAuth)) {
            // Past authentication; 404 only because the UT cluster is shared-nothing.
            Assertions.assertEquals(404, response.code(), response.body().string());
        }
    }
}
