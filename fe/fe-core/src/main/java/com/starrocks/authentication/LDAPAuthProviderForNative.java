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

import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.privilege.Password;
import com.starrocks.mysql.security.LdapSecurity;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.parquet.Strings;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LDAPAuthProviderForNative implements AuthenticationProvider {
    public static final String PLUGIN_NAME = AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name();

    @Override
    public UserAuthenticationInfo validAuthenticationInfo(UserIdentity userIdentity,
                                                          String password, String textForAuthPlugin) {
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        info.setTextForAuthPlugin(textForAuthPlugin);
        return info;
    }

    @Override
    public void authenticate(String user, String host, byte[] remotePassword, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        String userForAuthPlugin = authenticationInfo.getTextForAuthPlugin();
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

    @Override
    public UserAuthenticationInfo upgradedFromPassword(UserIdentity userIdentity, Password password)
            throws AuthenticationException {
        UserAuthenticationInfo ret = new UserAuthenticationInfo();
        ret.setPassword(password.getPassword());
        ret.setAuthPlugin(PLUGIN_NAME);
        ret.setOrigUserHost(userIdentity.getUser(), userIdentity.getHost());
        ret.setTextForAuthPlugin(password.getUserForAuthPlugin());
        return ret;
    }
}
