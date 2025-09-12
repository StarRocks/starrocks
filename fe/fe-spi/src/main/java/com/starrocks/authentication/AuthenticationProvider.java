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
     * Attempt to authentication
     *
     * @param authContext  authentication context
     * @param userIdentity best matched user
     * @param authResponse auth response received by mysql protocol
     * @throws AuthenticationException when authentication fail
     */
    void authenticate(AccessControlContext authContext, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException;

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
