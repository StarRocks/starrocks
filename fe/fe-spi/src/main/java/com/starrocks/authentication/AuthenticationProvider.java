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
     * Whether the configured group providers take part in this login's group set.
     *
     * <p>Only a provider that can resolve groups by itself has a reason to say no, and then they are
     * not merely dropped from the result - they are not called at all: calling and discarding would
     * pay for a read we said we do not want, and a broken group provider would still be able to
     * affect a mode that explicitly excludes it.
     */
    default boolean isGroupProviderUsed() {
        return true;
    }

    /**
     * Whether this provider resolved groups by itself while authenticating, and they take part in
     * this login's group set. The counterpart of {@link #isGroupProviderUsed()}: both sides of the
     * union are gated by the configured group source, so neither can silently keep contributing
     * after the other is turned off.
     *
     * <p>A provider that says true writes them with
     * {@code AccessControlContext#setAuthenticatorGroups} during {@link #authenticate}.
     */
    default boolean isAuthenticatorGroupsUsed() {
        return false;
    }

    /**
     * True when this login's groups cannot be known yet: they come from a credential that reaches
     * the connection only after the handshake, as in the OAuth 2.0 authorization-code flow over
     * MySQL, where the token arrives through the browser callback. Deciding the groups - and with
     * them the {@code permitted_groups} gate - on the handshake would then decide them on nothing.
     *
     * <p>A provider that says true receives the group resolution through
     * {@link #deferGroupResolution} and must run it before the first statement is allowed to
     * execute ({@link #checkLoginSuccess}), refusing the statement - and every later one - if it
     * throws.
     */
    default boolean isGroupResolutionDeferred() {
        return false;
    }

    /**
     * Hand over the group resolution a provider asked to defer. Only called when
     * {@link #isGroupResolutionDeferred()} is true; a provider that never defers has nothing to run.
     */
    default void deferGroupResolution(DeferredGroupResolution resolution) {
        throw new IllegalStateException(getClass().getSimpleName() + " does not defer group resolution");
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
