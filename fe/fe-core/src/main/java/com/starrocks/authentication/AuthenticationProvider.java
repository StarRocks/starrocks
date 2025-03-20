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

import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;

public interface AuthenticationProvider {

    /**
     * valid authentication info, and initialize the UserAuthenticationInfo structure
     * used when creating a user or modifying user's authentication information
     */
    UserAuthenticationInfo analyzeAuthOption(
            UserIdentity userIdentity, UserAuthOption userAuthOption) throws AuthenticationException;

    /**
     * login authentication
     */
    void authenticate(
            String user,
            String host,
            byte[] password,
            byte[] randomString,
            UserAuthenticationInfo authenticationInfo) throws AuthenticationException;

    /**
     * Some special Authentication Methods need to pass more information, and authMoreDataPacket is a unified interface.
     * <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_more_data.html">...</a>
     */
    default byte[] authMoreDataPacket(String user, String host) throws AuthenticationException {
        return null;
    }
}
