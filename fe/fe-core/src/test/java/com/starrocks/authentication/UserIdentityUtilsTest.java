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
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TUserIdentity;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

/**
 * A context rebuilt from Thrift must carry the caller's groups.
 *
 * <p>TAuthInfo/TUserIdentity ship the identity and the role ids but no group membership, and the handlers
 * that serve information_schema and the sys tables build a fresh ConnectContext from them before calling the
 * authorizer. When the group set stayed empty, the external access controller (Ranger) received no groups and
 * refused every policy item whose subject is a group - a user holding access purely through a group saw those
 * tables as empty while SHOW TABLES and SELECT on the same objects succeeded.
 */
public class UserIdentityUtilsTest {
    private static final String GROUP_PROVIDER_NAME = "file_group_provider";
    /** Present in test resource auth/file_group as a member of both group1 and group2. */
    private static final String USER_IN_GROUPS = "harbor";

    private String[] savedGroupProvider;
    /**
     * Surefire runs a module's test classes in one JVM (the default {@code forkCount=1},
     * {@code reuseForks=true}), so a provider registered into the shared manager would outlive this class
     * and stay visible to whatever runs next. The manager installed below is this class's own; this holds
     * the original so {@link #tearDown} can put it back.
     */
    private AuthenticationMgr savedAuthenticationMgr;

    @BeforeEach
    public void setUp() throws DdlException {
        savedGroupProvider = Config.group_provider;
        savedAuthenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        new MockUp<FileGroupProvider>() {
            @Mock
            public InputStream getPath(String groupFileUrl) throws IOException {
                String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath() + "/" + "file_group";
                return new FileInputStream(path);
            }
        };

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        authenticationMgr.replayCreateGroupProvider(GROUP_PROVIDER_NAME,
                Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "file",
                        FileGroupProvider.GROUP_FILE_URL, "file_group"));
        Config.group_provider = new String[] {GROUP_PROVIDER_NAME};
    }

    @AfterEach
    public void tearDown() {
        Config.group_provider = savedGroupProvider;
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(savedAuthenticationMgr);
    }

    private static TUserIdentity thriftIdentity(String user) {
        TUserIdentity tUserIdent = new TUserIdentity();
        tUserIdent.setUsername(user);
        tUserIdent.setHost("%");
        tUserIdent.setIs_domain(false);
        return tUserIdent;
    }

    @Test
    public void testGroupsResolvedFromTUserIdentity() {
        ConnectContext context = new ConnectContext();
        Assertions.assertTrue(context.getGroups().isEmpty());

        UserIdentityUtils.setAuthInfoFromThrift(context, thriftIdentity(USER_IN_GROUPS));

        Assertions.assertEquals(Set.of("group1", "group2"), context.getGroups());
        Assertions.assertEquals(USER_IN_GROUPS, context.getCurrentUserIdentity().getUser());
    }

    @Test
    public void testGroupsResolvedFromTAuthInfoWithIdentity() {
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setCurrent_user_ident(thriftIdentity(USER_IN_GROUPS));

        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, authInfo);

        Assertions.assertEquals(Set.of("group1", "group2"), context.getGroups());
    }

    /**
     * The fallback branch, taken when the request carries only a user name and an IP. It used to be inlined
     * in every handler, so it was the copy most likely to be missed.
     */
    @Test
    public void testGroupsResolvedFromTAuthInfoWithoutIdentity() {
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setUser(USER_IN_GROUPS);
        authInfo.setUser_ip("127.0.0.1");

        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, authInfo);

        Assertions.assertEquals(Set.of("group1", "group2"), context.getGroups());
    }

    @Test
    public void testGroupsResolvedFromUserAndIpOverload() {
        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, USER_IN_GROUPS, "127.0.0.1");

        Assertions.assertEquals(Set.of("group1", "group2"), context.getGroups());
    }

    /**
     * Some callers pass a live session context whose groups were resolved at login, possibly through a
     * security integration's own provider that this code path cannot see. Those must survive untouched.
     */
    @Test
    public void testExistingGroupsAreNotOverwritten() {
        ConnectContext context = new ConnectContext();
        context.setGroups(Set.of("groups-from-the-session"));

        UserIdentityUtils.setAuthInfoFromThrift(context, thriftIdentity(USER_IN_GROUPS));

        Assertions.assertEquals(Set.of("groups-from-the-session"), context.getGroups());
    }

    @Test
    public void testUserInNoGroupLeavesGroupsEmpty() {
        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, thriftIdentity("user_that_is_in_no_group"));

        Assertions.assertTrue(context.getGroups().isEmpty());
    }

    /**
     * Resolution must not be able to fail the request it decorates: no groups denies rather than grants, so a
     * broken provider has to leave the identity usable rather than propagate out of the metadata scan.
     */
    @Test
    public void testProviderFailureLeavesIdentityIntact() {
        new MockUp<AuthenticationHandler>() {
            @Mock
            public Set<String> getGroups(UserIdentity userIdentity, String distinguishedName,
                                         java.util.List<String> groupProviderList) {
                throw new RuntimeException("group provider is down");
            }
        };

        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, thriftIdentity(USER_IN_GROUPS));

        Assertions.assertTrue(context.getGroups().isEmpty());
        Assertions.assertEquals(USER_IN_GROUPS, context.getCurrentUserIdentity().getUser());
    }
}
