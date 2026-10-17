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

public interface AuthenticationProvider {

    /**
     * Attempt to authenticate, returning the authenticated {@link UserIdentity}.
     *
     * <ul>
     *   <li>For paths with an explicit user (native / HTTP Basic / MySQL handshake): the passed-in
     *       {@code userIdentity} is returned.</li>
     *   <li>For Bearer ({@code userIdentity == null}): the provider parses the principal claim out
     *       of the token, builds an ephemeral {@link UserIdentity} and returns it &mdash; this
     *       return value is the only channel by which a Bearer user name flows back from the
     *       provider to the handler.</li>
     * </ul>
     *
     * @param authContext authentication context
     * @param userIdentity best matched user, or {@code null} for Bearer (no declared user)
     * @param cred the client credential, interpreted by its {@link AuthCredentialKind kind}
     * @return the authenticated user identity
     * @throws AuthenticationException when authentication fails
     */
    UserIdentity authenticate(AccessControlContext authContext, UserIdentity userIdentity, AuthCredentials cred)
            throws AuthenticationException;

    /**
     * Whether the security integration chain may offer this provider a credential of this kind.
     * That is the whole question: the native path selects its provider from the user row and never
     * asks, so a provider may handle more kinds in {@link #authenticate} than it declares here.
     *
     * <p>Defaults to accepting nothing. Every provider states what it reads; there is no kind that
     * is safe to assume.
     */
    default boolean supportsCredentialKind(AuthCredentialKind kind) {
        return false;
    }

    /**
     * Some special Authentication Methods need to pass more information, and authMoreDataPacket is a unified interface.
     * <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_more_data.html">...</a>
     */
    default byte[] authMoreDataPacket(AccessControlContext authContext, String user, String host)
            throws AuthenticationException {
        return null;
    }

    /**
     * Authentication method Switch Request Packet
     * If both server and the client support CLIENT_PLUGIN_AUTH capability,
     * server can send this packet tp ask client to use another authentication method.
     * <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html">...</a>
     */
    default byte[] authSwitchRequestPacket(AccessControlContext authContext, String user, String host)
            throws AuthenticationException {
        return null;
    }

    default void checkLoginSuccess(int connectionId, AccessControlContext authContext) throws AuthenticationException {
        // do nothing
    }
}
