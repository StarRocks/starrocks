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
import com.starrocks.mysql.privilege.Password;
import com.starrocks.mysql.security.LdapSecurity;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.parquet.Strings;

import static com.starrocks.mysql.privilege.AuthPlugin.AUTHENTICATION_LDAP_SIMPLE;

public class LDAPAuthenticationProvider implements AuthenticationProvider {
    public static final String PLUGIN_NAME = AUTHENTICATION_LDAP_SIMPLE.name();

    @Override
    public UserAuthenticationInfo validAuthenticationInfo(UserIdentity userIdentity, String password, String textForAuthPlugin) {
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        info.setTextForAuthPlugin(textForAuthPlugin);
        return info;
    }

    @Override
    public void authenticate(String user, String host, byte[] password, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        String userForAuthPlugin = authenticationInfo.getTextForAuthPlugin();
        if (!Strings.isNullOrEmpty(userForAuthPlugin)) {
            LdapSecurity.checkPassword(userForAuthPlugin, new String(password));
        } else {
            LdapSecurity.checkPasswordByRoot(user, new String(password));
        }
    }

    @Override
    public UserAuthenticationInfo upgradedFromPassword(UserIdentity userIdentity, Password password)
            throws AuthenticationException {
        UserAuthenticationInfo ret = new UserAuthenticationInfo();
        ret.setPassword(password.getPassword());
        ret.setAuthPlugin(PLUGIN_NAME);
        ret.setOrigUserHost(userIdentity.getQualifiedUser(), userIdentity.getHost());
        ret.setTextForAuthPlugin(password.getUserForAuthPlugin());
        return ret;
    }
}
