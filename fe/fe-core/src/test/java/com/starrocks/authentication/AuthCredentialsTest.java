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
import com.starrocks.mysql.MysqlPassword;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class AuthCredentialsTest {

    @Test
    public void testFactoryMethodsCarryShapeAndUser() {
        AuthCredentials mysql = AuthCredentials.ofNativeScramble("alice", new byte[] {1, 2, 3}, "127.0.0.1");
        Assertions.assertEquals(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE, mysql.kind());
        Assertions.assertEquals("alice", mysql.userDeclared());
        Assertions.assertEquals("127.0.0.1", mysql.remoteHost());

        AuthCredentials basic = AuthCredentials.ofPassword("bob", "secret", AuthChannel.HTTP, "10.0.0.1");
        Assertions.assertEquals(AuthCredentialKind.PLAIN_PASSWORD, basic.kind());
        Assertions.assertEquals("bob", basic.userDeclared());
        Assertions.assertArrayEquals("secret".getBytes(StandardCharsets.UTF_8), basic.rawSecret());

        AuthCredentials bearer = AuthCredentials.ofBearer("header.payload.sig", AuthChannel.HTTP, "10.0.0.2");
        Assertions.assertEquals(AuthCredentialKind.BEARER_TOKEN, bearer.kind());
        Assertions.assertNull(bearer.userDeclared(), "Bearer carries no declared user");
        Assertions.assertEquals("header.payload.sig", bearer.tokenString());
    }

    @Test
    public void testSupportsCredentialKindDefaultsToNothing() {
        // A provider that does not say what it reads is offered nothing by the chain. There is no
        // kind that is safe to assume for an unknown provider, so the default is not "MySQL" any
        // more - every provider declares.
        AuthenticationProvider provider = (authContext, userIdentity, cred) -> userIdentity;
        for (AuthCredentialKind kind : AuthCredentialKind.values()) {
            Assertions.assertFalse(provider.supportsCredentialKind(kind), kind.name());
        }
    }

    @Test
    public void testProviderRejectsUnsupportedKind() {
        // PlainPasswordAuthenticationProvider handles only MYSQL_NATIVE_SCRAMBLE today; a Bearer
        // credential must be rejected.
        PlainPasswordAuthenticationProvider provider =
                new PlainPasswordAuthenticationProvider(MysqlPassword.EMPTY_PASSWORD);
        AuthCredentials bearer = AuthCredentials.ofBearer("header.payload.sig", AuthChannel.HTTP, "127.0.0.1");

        Assertions.assertThrows(AuthenticationException.class,
                () -> provider.authenticate(null,
                        UserIdentity.createEphemeralUserIdent("dave", "127.0.0.1"), bearer));
    }

    @Test
    public void testNonMysqlFactoriesNeverProduceAMysqlKind() {
        // TC-L1-41-09 / INV-14: the two factories every non-MySQL entry point uses fix the kind, so a
        // MySQL-only kind can only ever come out of the MySQL entry point. The names say which are
        // MySQL-only.
        for (AuthChannel channel : AuthChannel.values()) {
            if (channel == AuthChannel.MYSQL) {
                continue;
            }
            Assertions.assertEquals(AuthCredentialKind.PLAIN_PASSWORD,
                    AuthCredentials.ofPassword("alice", "pw", channel, "127.0.0.1").kind(), channel.name());
            Assertions.assertEquals(AuthCredentialKind.BEARER_TOKEN,
                    AuthCredentials.ofBearer("h.p.s", channel, "127.0.0.1").kind(), channel.name());
        }
        Assertions.assertEquals(AuthChannel.MYSQL,
                AuthCredentials.ofNativeScramble("alice", new byte[20], "127.0.0.1").channel());
        for (AuthCredentialKind kind : AuthCredentialKind.values()) {
            boolean mysqlOnly = kind == AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE
                    || kind == AuthCredentialKind.MYSQL_OAUTH2_CLIENT;
            Assertions.assertEquals(mysqlOnly, kind.name().startsWith("MYSQL_"),
                    "the MYSQL_ prefix marks exactly the kinds only the MySQL handshake can produce: " + kind);
        }
    }

    @Test
    public void testWireNameIsTheOnlyPredicateLeftOnTheKind() {
        // TC-L1-41-10 / TC-L2-410-01 / TC-L2-410-02: the thrift auth_type. Unset, empty and "basic"
        // are one and the same - that equivalence is what lets a BE without the field talk to a new
        // FE - and only "bearer" (any case, surrounding blanks ignored) selects a token.
        for (String basic : new String[] {null, "", "  ", "basic", "BASIC", " Basic "}) {
            Assertions.assertEquals(AuthCredentialKind.PLAIN_PASSWORD, AuthCredentialKind.fromWireName(basic),
                    "auth_type=" + basic);
            Assertions.assertFalse(AuthCredentialKind.isBearer(basic), "auth_type=" + basic);
        }
        for (String bearer : new String[] {"bearer", "Bearer", "BEARER", " bearer "}) {
            Assertions.assertEquals(AuthCredentialKind.BEARER_TOKEN, AuthCredentialKind.fromWireName(bearer));
            Assertions.assertTrue(AuthCredentialKind.isBearer(bearer));
        }
        // An unknown value is not an error and not a token: an old FE would have read it as a
        // password login too.
        Assertions.assertEquals(AuthCredentialKind.PLAIN_PASSWORD, AuthCredentialKind.fromWireName("token"));
        Assertions.assertFalse(AuthCredentialKind.isBearer("token"));

        // No semantic predicates on the enum: "is this a password" is each provider's call.
        for (java.lang.reflect.Method m : AuthCredentialKind.class.getDeclaredMethods()) {
            Assertions.assertFalse(m.getName().equals("isPasswordBased"), "isPasswordBased must stay deleted");
            if (m.getName().equals("isBearer")) {
                Assertions.assertTrue(java.lang.reflect.Modifier.isStatic(m.getModifiers()),
                        "isBearer exists only as the static thrift-wire helper");
            }
        }
    }

    @Test
    public void testAMysqlHandshakeKindCannotClaimAnotherChannel() {
        // The OAuth2 provider answers a MYSQL_NATIVE_SCRAMBLE with "the handshake will finish this
        // login later", which is only true on MySQL: ConnectProcessor#checkLoginSuccess gates the
        // connection's first statement there, and no other channel has such a gate. So the pairing
        // is enforced at construction, where every entry point and every third-party provider has
        // to go through it - not left to the factories, which are only a convention.
        for (AuthCredentialKind mysqlOnly : new AuthCredentialKind[] {
                AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE, AuthCredentialKind.MYSQL_OAUTH2_CLIENT}) {
            Assertions.assertTrue(mysqlOnly.isMysqlProtocolOnly(), mysqlOnly.name());
            for (AuthChannel channel : AuthChannel.values()) {
                if (channel == AuthChannel.MYSQL) {
                    Assertions.assertNotNull(new AuthCredentials("u", new byte[1], mysqlOnly, channel, "h"));
                    continue;
                }
                IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                        () -> new AuthCredentials("u", new byte[1], mysqlOnly, channel, "h"),
                        mysqlOnly + " on " + channel);
                Assertions.assertTrue(e.getMessage().contains(channel.name()), e.getMessage());
            }
        }

        // Everything else is the same credential on any channel, so no pairing is imposed. A
        // Kerberos token in particular is deliberately not restricted: it is the same token on any
        // transport and no provider treats it as a deferred login.
        for (AuthCredentialKind portable : new AuthCredentialKind[] {AuthCredentialKind.PLAIN_PASSWORD,
                AuthCredentialKind.BEARER_TOKEN, AuthCredentialKind.GSSAPI_TOKEN}) {
            Assertions.assertFalse(portable.isMysqlProtocolOnly(), portable.name());
            for (AuthChannel channel : AuthChannel.values()) {
                Assertions.assertNotNull(new AuthCredentials("u", new byte[1], portable, channel, "h"));
            }
        }

        // The three factories can only produce legal pairings, and the kind and channel are
        // mandatory rather than defaulted.
        Assertions.assertEquals(AuthChannel.MYSQL,
                AuthCredentials.ofNativeScramble("u", new byte[1], "h").channel());
        Assertions.assertThrows(NullPointerException.class,
                () -> new AuthCredentials("u", new byte[1], null, AuthChannel.HTTP, "h"));
        Assertions.assertThrows(NullPointerException.class,
                () -> new AuthCredentials("u", new byte[1], AuthCredentialKind.PLAIN_PASSWORD, null, "h"));
    }
}
