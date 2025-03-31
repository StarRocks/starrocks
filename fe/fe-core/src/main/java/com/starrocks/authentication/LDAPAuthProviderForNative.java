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

import com.google.common.base.Strings;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.security.LdapSecurity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LDAPAuthProviderForNative implements AuthenticationProvider {
    @Override
    public UserAuthenticationInfo analyzeAuthOption(UserIdentity userIdentity, UserAuthOption userAuthOption)
            throws AuthenticationException {
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setAuthPlugin(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.toString());
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        info.setOrigUserHost(userIdentity.getUser(), userIdentity.getHost());
        info.setAuthString(userAuthOption == null ? null : userAuthOption.getAuthString());
        return info;
    }

    @Override
    public void authenticate(ConnectContext context, String user, String host, byte[] remotePassword, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        String userForAuthPlugin = authenticationInfo.getAuthString();
        //clear password terminate string
        byte[] clearPassword = remotePassword;
        if (remotePassword[remotePassword.length - 1] == 0) {
            clearPassword = Arrays.copyOf(remotePassword, remotePassword.length - 1);
        }
        if (!Strings.isNullOrEmpty(userForAuthPlugin)) {
            if (!LdapSecurity.checkPassword(userForAuthPlugin, new String(clearPassword, StandardCharsets.UTF_8))) {
                throw new AuthenticationException("Failed to authenticate for [user: " + user + "] by ldap");
            }
        } else {
            if (!LdapSecurity.checkPasswordByRoot(user, new String(clearPassword, StandardCharsets.UTF_8))) {
                throw new AuthenticationException("Failed to authenticate for [user: " + user + "] by ldap");
            }
        }
    }
}
