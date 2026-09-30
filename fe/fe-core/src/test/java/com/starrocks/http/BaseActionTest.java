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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * Covers the two BaseAction entry points that carry an authenticated context, rather than rebuilding
 * authorization from the returned identity alone.
 */
public class BaseActionTest {

    private static UserIdentity webUser;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withUser("base_action_web_user");
        webUser = UserIdentity.createAnalyzedUserIdentWithIp("base_action_web_user", "%");
    }

    private static BaseAction newAction() {
        return new BaseAction(null) {
            @Override
            public void execute(BaseRequest request, BaseResponse response) {
            }
        };
    }

    @Test
    public void testAdminRoleCheckAcceptsRoleIdsResolvedByAuthentication() throws Exception {
        // The point of the overload: a user owning no admin role is still an administrator when authentication
        // resolved those roles from its groups. Passing them in is what lets an ephemeral security integration
        // user through, since it owns no stored roles at all.
        Assertions.assertDoesNotThrow(() -> newAction().checkUserOwnsAdminRole(webUser,
                Set.of(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID, PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID)));
    }

    @Test
    public void testAdminRoleCheckDeniesWithoutResolvedRoleIds() {
        // Same user, no resolved roles: the stored roles alone do not make it an administrator.
        Assertions.assertThrows(AccessDeniedException.class,
                () -> newAction().checkUserOwnsAdminRole(webUser, null));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> newAction().checkUserOwnsAdminRole(webUser, Set.of()));
    }

    @Test
    public void testAdminRoleCheckStillAcceptsRoot() throws Exception {
        Assertions.assertDoesNotThrow(() -> newAction().checkUserOwnsAdminRole(UserIdentity.ROOT, null));
        Assertions.assertDoesNotThrow(() -> newAction().checkUserOwnsAdminRole(UserIdentity.ROOT, Set.of(1L)));
    }

    @Test
    public void testCheckPasswordPopulatesTheCallerContext() throws Exception {
        // The context-taking overload exists so the caller can authorize on what authentication resolved; the
        // legacy overload discards it.
        ConnectContext context = new ConnectContext();
        BaseAction.ActionAuthorizationInfo authInfo =
                BaseAction.parseAuthInfo("root", "", "127.0.0.1");

        UserIdentity authenticated = BaseAction.checkPassword(authInfo, context);

        Assertions.assertEquals(UserIdentity.ROOT, authenticated);
        Assertions.assertEquals(UserIdentity.ROOT, context.getCurrentUserIdentity());
        Assertions.assertNotNull(context.getCurrentRoleIds());
    }

    @Test
    public void testCheckPasswordRejectsUnknownUser() {
        BaseAction.ActionAuthorizationInfo authInfo =
                BaseAction.parseAuthInfo("no_such_user", "whatever", "127.0.0.1");
        Assertions.assertThrows(AccessDeniedException.class,
                () -> BaseAction.checkPassword(authInfo, new ConnectContext()));
    }
}
