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

/**
 * How a provider must read {@link AuthCredentials#rawSecret()} - a decoding recipe, not a claim
 * about what the credential means. Reused across protocols (MySQL handshake, HTTP, Arrow Flight,
 * BE-FE Thrift); the wire-level {@code auth_type} string used by Thrift is a projection of it.
 *
 * <p>Deliberately offers no semantic predicates such as {@code isPasswordBased()}. A MySQL
 * handshake frame may hold a scramble or a GSSAPI token, so "might be a password" and "might be a
 * token" would both be true of it - two predicates that overlap cannot route anything. Each
 * provider instead enumerates what it can decode, in
 * {@link AuthenticationProvider#supportsCredentialKind}.
 */
public enum AuthCredentialKind {
    // ---- Inherently MySQL-only: the MYSQL_ prefix means the kind cannot arise on another channel.

    /**
     * The 20-byte {@code mysql_native_password} scramble: a challenge-response against the salt the
     * MySQL server issued in its greeting. Inherently MySQL-only - no other channel issues a salt -
     * and inherently native-only, since checking it needs the password hash stored on the user
     * row. Also what an old client that names no plugin is speaking - and what a stock client sends
     * when the server wants a plugin it cannot speak: MySQL skips the switch for StarRocks custom
     * plugins, so the client falls back to this. That is how the OAuth2 provider recognises a
     * client that cannot do the browser flow. Has no wire name: it never crosses the BE-to-FE RPC.
     *
     * <p>Every other MySQL frame is unwrapped at the entry point ({@code
     * MysqlProto.handshakeCredentials}) into the kind the bytes actually are, so this is the only
     * value that still carries MySQL framing.
     */
    MYSQL_NATIVE_SCRAMBLE(null),
    /**
     * A MySQL client that declared the {@code authentication_oauth2_client} plugin. There are no
     * credential bytes: on MySQL the OAuth2 token never travels in the protocol, it arrives through
     * the browser callback ({@code OAuth2Action}) whichever client is connecting. What this kind
     * records is that the client can take part in that flow - it received the authorization
     * endpoint in the handshake and can open the browser itself, so the server waits for the
     * callback before answering. A client without the plugin sends an ordinary
     * {@link #MYSQL_NATIVE_SCRAMBLE} frame instead, and the OAuth2 provider answers that handshake
     * immediately and lets the first statement carry the URL.
     *
     * <p>Its own value because the OAuth2 provider has to tell the two clients apart - hold the
     * handshake for the callback, or answer it and let the first statement carry the URL - and the
     * entry point can only ever label the positive case: a frame from the OAuth2 plugin is
     * unmistakable, while a scramble from a client that merely lacks the plugin looks exactly like
     * any password login and stays a {@link #MYSQL_NATIVE_SCRAMBLE}.
     */
    MYSQL_OAUTH2_CLIENT(null),

    // ---- Channel neutral: produced by whichever entry point carries these bytes.

    /**
     * A GSSAPI (Kerberos) token, handed to {@code GSSContext.acceptSecContext} as-is. Channel
     * neutral: today only the MySQL {@code authentication_kerberos_client} plugin delivers one, but
     * HTTP has a standard carrier for exactly these bytes ({@code Authorization: Negotiate}, RFC
     * 4559), and a future entry for it would produce this same kind with nothing downstream to
     * change. No wire name yet: the BE-to-FE RPC does not carry it.
     */
    GSSAPI_TOKEN(null),
    /** The bytes are the password itself. */
    PLAIN_PASSWORD("basic"),
    /** The bytes are the token string itself. */
    BEARER_TOKEN("bearer");

    private final String wireName;

    AuthCredentialKind(String wireName) {
        this.wireName = wireName;
    }

    /**
     * The value carried by the Thrift {@code auth_type} field, or {@code null} for a kind that
     * never travels over that RPC. Keeping the spelling here means the strings exist in exactly one
     * place; the BE writes the same two values from {@code auth_info.h}.
     */
    public String wireName() {
        return wireName;
    }

    /**
     * Whether this kind is a frame of the MySQL protocol and therefore cannot appear on another
     * channel. A provider may answer such a frame with "the handshake will finish this login later"
     * because MySQL has a later ({@code ConnectProcessor#checkLoginSuccess} gates the connection's
     * first statement); HTTP, Arrow Flight and the Thrift load RPCs serve the request immediately.
     *
     * <p>This is a statement about the wire format, not a semantic predicate on the credential:
     * the two values below are named after the MySQL client plugin that produces them.
     * {@link AuthCredentials} enforces the pairing at construction.
     *
     * <p>{@link #GSSAPI_TOKEN} is deliberately not on this list. A Kerberos token is the same token
     * on any transport, and nothing today lets a provider treat it as a deferred login.
     */
    public boolean isMysqlProtocolOnly() {
        return this == MYSQL_NATIVE_SCRAMBLE || this == MYSQL_OAUTH2_CLIENT;
    }

    /**
     * Whether a Thrift {@code auth_type} names the bearer kind.
     *
     * <p>This one is well defined where the instance-level question is not: the Thrift channel
     * never carries a MySQL handshake frame, so over that wire "basic or bearer" really is a
     * two-way split. Follows {@link #fromWireName} exactly, including its treatment of unset and
     * unrecognised values.
     */
    public static boolean isBearer(String authType) {
        return fromWireName(authType) == BEARER_TOKEN;
    }

    /**
     * Read a Thrift {@code auth_type} back into a kind.
     *
     * <p>Unset, empty and {@code "basic"} all mean the same thing. That equivalence is what lets a
     * BE that predates this field talk to a new FE, so it has to stay.
     *
     * <p>An unrecognised value also reads as a password rather than throwing. A newer BE sending a
     * kind this FE has never heard of should fail as an ordinary authentication failure, not as a
     * broken RPC.
     */
    public static AuthCredentialKind fromWireName(String authType) {
        return BEARER_TOKEN.wireName.equalsIgnoreCase(authType == null ? "" : authType.trim())
                ? BEARER_TOKEN : PLAIN_PASSWORD;
    }
}
