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

import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.privilege.AuthPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/**
 * The MySQL entry point turns a handshake frame into a protocol-neutral credential. These pin the
 * edges of that translation: what happens to the bytes, and what happens when the frame does not
 * match the plugin the client claimed. The happy path per plugin is in
 * {@link BearerRoutingTest#testMysqlHandshakeIsNormalisedByNegotiatedPlugin}.
 */
public class MysqlCredentialNormalizationTest {
    private static final String HOST = "127.0.0.1";

    private static AuthCredentials frame(String plugin, byte[] bytes) throws AuthenticationException {
        return MysqlProto.handshakeCredentials("alice", bytes, plugin, HOST);
    }

    @Test
    public void testOpaqueFramesAreHandedOverByteForByte() throws Exception {
        // TC-L1-41-05 (rawSecret half) / TC-L2-49-12: a scramble and a GSSAPI token are the provider's
        // to interpret; the entry point only names them.
        byte[] scramble = new byte[20];
        for (int i = 0; i < scramble.length; i++) {
            scramble[i] = (byte) (i * 7);
        }
        AuthCredentials scrambled = frame(AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString(), scramble);
        Assertions.assertEquals(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE, scrambled.kind());
        Assertions.assertArrayEquals(scramble, scrambled.rawSecret());

        // The OAuth2 plugin sends no credential: the login is decided by the browser callback.
        AuthCredentials oauth2 = frame(AuthPlugin.Client.AUTHENTICATION_OAUTH2_CLIENT.toString(), new byte[0]);
        Assertions.assertEquals(AuthCredentialKind.MYSQL_OAUTH2_CLIENT, oauth2.kind());
        Assertions.assertEquals(0, oauth2.rawSecret().length);
    }

    @Test
    public void testUnknownOrMissingPluginFallsBackToTheNativeScramble() throws Exception {
        // TC-L1-41-06: an old client sends no plugin name, a future client may send one we do not
        // know. Both are treated as the native-password scramble, and neither is an error here - if
        // the bytes are not a scramble the provider will say so.
        byte[] bytes = {1, 2, 3, 4, 5};
        for (String plugin : new String[] {null, "", "authentication_from_the_future"}) {
            AuthCredentials cred = frame(plugin, bytes);
            Assertions.assertEquals(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE, cred.kind(), "plugin: " + plugin);
            Assertions.assertArrayEquals(bytes, cred.rawSecret(), "plugin: " + plugin);
            Assertions.assertEquals(AuthChannel.MYSQL, cred.channel());
        }
    }

    @Test
    public void testFrameThatDoesNotMatchThePluginIsAnAuthenticationFailureNotACrash() {
        // TC-L1-41-07: a client that declared the OpenID Connect plugin but then sent something else
        // (a stock client whose plugin switch was skipped sends its 20-byte scramble; a broken client
        // can send a length prefix longer than the frame). Whatever comes back, it must be the kind
        // of failure the caller can report as "authentication failed", never an unchecked exception
        // that surfaces as an HTTP 500 or a raw protocol error.
        String oidc = AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString();
        byte[] lengthPrefixLongerThanFrame = {0, (byte) 0xfc, 0x10, 0x00};      // lenenc says 16 bytes, has none
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                () -> frame(oidc, lengthPrefixLongerThanFrame));
        Assertions.assertEquals(AuthFailureReason.MALFORMED, e.getReason());

        byte[] stockScramble = new byte[20];
        Assertions.assertDoesNotThrow(() -> {
            try {
                AuthCredentials cred = frame(oidc, stockScramble);
                // Decoded as a token: it is garbage, and the verifier will refuse it as malformed.
                Assertions.assertEquals(AuthCredentialKind.BEARER_TOKEN, cred.kind());
            } catch (AuthenticationException ok) {
                // Or refused right here. Either is a clean authentication failure.
            }
        });
    }

    @Test
    public void testClearPasswordDropsExactlyTheFrameTerminator() throws Exception {
        // TC-L1-41-08: the trailing NUL belongs to the frame, not the password, so exactly one is
        // removed. A password that itself ends in NUL keeps its own; an empty password is an empty
        // secret, not a missing one.
        String clear = AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString();
        Assertions.assertEquals("pw\0", secret(frame(clear, "pw\0\0".getBytes(StandardCharsets.UTF_8))));
        Assertions.assertEquals("", secret(frame(clear, "\0".getBytes(StandardCharsets.UTF_8))));
        Assertions.assertEquals("pw", secret(frame(clear, "pw".getBytes(StandardCharsets.UTF_8))),
                "a frame without the terminator is taken as is, nothing is invented");
        Assertions.assertEquals("", secret(frame(clear, new byte[0])));
    }

    private static String secret(AuthCredentials cred) {
        Assertions.assertEquals(AuthCredentialKind.PLAIN_PASSWORD, cred.kind());
        return new String(cred.rawSecret(), StandardCharsets.UTF_8);
    }
}
