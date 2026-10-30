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

import com.starrocks.authentication.AuthCredentialKind;
import com.starrocks.authentication.AuthCredentials;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.http.action.WebBaseAction;
import com.starrocks.qe.ConnectContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Credentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The Web UI authenticates onto the very context the page then runs on. That is what keeps the roles
 * a directory group maps to: authenticating onto a throwaway context and copying the UserIdentity
 * across would drop them, and an LDAP or security-integration admin would be told they are not one.
 */
public class WebBaseActionAuthTest {

    /** A page that records the context it was rendered with. */
    static class ProbePage extends WebBaseAction {
        ConnectContext observed;

        ProbePage() {
            super(null);
        }

        @Override
        public boolean needAdmin() {
            return false;   // NODE privilege is not what this test is about
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            observed = ConnectContext.get();
        }
    }

    private static BaseRequest requestWith(String authorization) {
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add("Authorization", authorization);
        // the page reads the client address off the channel
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 34567));
        return new BaseRequest(ctx, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/system", headers));
    }

    @Test
    public void testGroupRolesSurviveIntoThePageForBasicAndBearer() throws Exception {
        // TC-L3-46-10: whatever the credential kind, the roles the provider resolved onto the context
        // (here: 11 and 22, as a group mapping would) are the roles the page sees.
        new MockUp<AuthenticationHandler>() {
            @Mock
            public UserIdentity authenticate(ConnectContext ctx, AuthCredentials cred) throws AuthenticationException {
                UserIdentity who = cred.kind() == AuthCredentialKind.BEARER_TOKEN
                        ? UserIdentity.createEphemeralUserIdent("harbor", cred.remoteHost())
                        : UserIdentity.createEphemeralUserIdent(cred.userDeclared(), cred.remoteHost());
                ctx.setCurrentUserIdentity(who);
                ctx.setQualifiedUser(who.getUser());
                ctx.setCurrentRoleIds(Set.of(11L, 22L));
                return who;
            }
        };

        ProbePage basicPage = new ProbePage();
        basicPage.execute(requestWith(Credentials.basic("alice", "pw")), new BaseResponse());
        Assertions.assertNotNull(basicPage.observed, "the page must run after a Basic login");
        Assertions.assertEquals("alice", basicPage.observed.getQualifiedUser());
        Assertions.assertEquals(Set.of(11L, 22L), basicPage.observed.getCurrentRoleIds());

        ProbePage bearerPage = new ProbePage();
        bearerPage.execute(requestWith("Bearer h.p.s"), new BaseResponse());
        Assertions.assertNotNull(bearerPage.observed, "the page must run after a Bearer login");
        Assertions.assertEquals("harbor", bearerPage.observed.getQualifiedUser());
        Assertions.assertEquals(Set.of(11L, 22L), bearerPage.observed.getCurrentRoleIds());
    }
}
