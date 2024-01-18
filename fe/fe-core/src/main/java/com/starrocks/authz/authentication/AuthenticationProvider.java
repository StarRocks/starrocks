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


package com.starrocks.authz.authentication;

import com.starrocks.mysql.privilege.Password;
import com.starrocks.sql.ast.UserIdentity;

public interface AuthenticationProvider {

    /**
     * valid authentication info, and initialize the UserAuthenticationInfo structure
     * used when creating a user or modifying user's authentication information
     */
    UserAuthenticationInfo validAuthenticationInfo(
            UserIdentity userIdentity,
            String password,
            String textForAuthPlugin) throws AuthenticationException;

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
     * upgraded from 2.x
     **/
    UserAuthenticationInfo upgradedFromPassword(UserIdentity userIdentity, Password password)
            throws AuthenticationException;
}
