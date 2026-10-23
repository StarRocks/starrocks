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

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;

/**
 * The client-side credential of a single authentication request. It is the request-side counterpart
 * of {@link UserAuthenticationInfo} (the server-side persisted record): one is a per-request value
 * that is discarded after use, the other is the server-side truth.
 *
 * <p>Each access entry point (MySQL / HTTP / Arrow Flight / Thrift) constructs an instance, and
 * {@code AuthenticationHandler} dispatches on its {@link #kind}.
 *
 * @param userDeclared the user the client declared (MySQL handshake user / HTTP Basic user);
 *                     {@code null} when a Bearer request carries no explicit user
 * @param rawSecret the raw credential bytes, interpreted according to {@code kind}: the MySQL
 *                  handshake frame, the plaintext password, or the token string
 * @param kind how a provider must read {@code rawSecret}
 * @param channel the access channel the credential arrived on; mandatory, no default, so a
 *                     new entry point has to declare itself instead of inheriting a wrong label
 * @param remoteHost the client IP
 */
public record AuthCredentials(String userDeclared, byte[] rawSecret,
                              AuthCredentialKind kind, AuthChannel channel, String remoteHost) {

    /**
     * Rejects a kind that names the MySQL protocol on any other channel. This is not tidiness: a
     * provider is allowed to treat such a kind as "the MySQL handshake will finish this login
     * later", because on MySQL there is a later - {@code ConnectProcessor#checkLoginSuccess} gates
     * the first statement of the connection. No other channel has that gate; it serves the request
     * immediately. {@code OAuth2AuthenticationProvider} does exactly this for
     * {@link AuthCredentialKind#MYSQL_NATIVE_SCRAMBLE} (a stock client that could not speak the
     * OAuth2 plugin), so a scramble arriving on HTTP would be an authentication bypass rather than
     * a mislabelled metric.
     *
     * <p>The factories below already pair the two correctly. The check is here because the record's
     * canonical constructor is public API: {@code fe-spi} is what third-party providers and future
     * entry points compile against, and an entry point that reuses a MySQL kind for its own frames
     * has to fail loudly at construction rather than quietly reach that branch.
     */
    public AuthCredentials {
        Objects.requireNonNull(kind, "credential kind is mandatory");
        Objects.requireNonNull(channel, "credential channel is mandatory");
        if (kind.isMysqlProtocolOnly() && channel != AuthChannel.MYSQL) {
            throw new IllegalArgumentException(kind + " is a MySQL handshake frame and cannot arrive on " + channel);
        }
    }

    /** The {@code mysql_native_password} scramble from a MySQL handshake, as sent. */
    public static AuthCredentials ofNativeScramble(String user, byte[] authResponse, String host) {
        return new AuthCredentials(user, authResponse, AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE,
                AuthChannel.MYSQL, host);
    }

    /** Plaintext-password credential: HTTP Basic / Arrow Flight Basic / Thrift user+passwd. */
    public static AuthCredentials ofPassword(String user, String password, AuthChannel channel, String host) {
        return new AuthCredentials(user, password.getBytes(StandardCharsets.UTF_8),
                AuthCredentialKind.PLAIN_PASSWORD, channel, host);
    }

    /** Bearer-token credential; no explicit user (it is resolved from the token claim). */
    public static AuthCredentials ofBearer(String token, AuthChannel channel, String host) {
        return new AuthCredentials(null, token.getBytes(StandardCharsets.UTF_8),
                AuthCredentialKind.BEARER_TOKEN, channel, host);
    }

    /**
     * Value of the {@code channel} label on the authentication metrics: which way the credential
     * came in.
     *
     * <p>Kept as a separate label from {@link #kindLabel()} rather than flattened into one
     * value, so each dimension aggregates on its own: sum by channel to see whether HTTP or Arrow
     * Flight is failing, sum by kind to see whether Bearer as a whole is healthy.
     *
     * <p>Both label values are external contract - alerting rules are written against them, so
     * renaming one breaks users.
     */
    public String channelLabel() {
        return channel.name().toLowerCase(Locale.ROOT);
    }

    /** Value of the {@code kind} label: how the credential had to be read. */
    public String kindLabel() {
        return kind.name().toLowerCase(Locale.ROOT);
    }

    /** Decode {@link #rawSecret} as a UTF-8 string (for token kinds). */
    public String tokenString() {
        return new String(rawSecret, StandardCharsets.UTF_8);
    }
}
