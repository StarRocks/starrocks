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

import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.privilege.Password;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.commons.lang3.StringUtils;

public class LDAPAuthProviderForExternal implements AuthenticationProvider {
    public static final String PLUGIN_NAME = AuthPlugin.AUTHENTICATION_LDAP_SIMPLE_FOR_EXTERNAL.name();

    @Override
    public UserAuthenticationInfo validAuthenticationInfo(UserIdentity userIdentity,
                                                          String password, String textForAuthPlugin)
            throws AuthenticationException {
        throw new AuthenticationException("unsupported");
    }

    public static boolean authenticate(String username, String password,
                                       String host, String port,
                                       String baseDn, String searchAttr,
                                       String rootDn, String rootPwd) throws Exception {
        return true;
    }

    @Override
    public void authenticate(String user, String host, byte[] password, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        LDAPSecurityIntegration ldapSecurityIntegration =
                (LDAPSecurityIntegration) authenticationInfo.extraInfo.get(PLUGIN_NAME);
        try {
            boolean authenticated = LDAPAuthProviderForExternal.authenticate(
                    user, StringUtils.stripEnd(new String(password), "\0"),
                    ldapSecurityIntegration.getLdapServerHost(),
                    ldapSecurityIntegration.getLdapServerPort(),
                    ldapSecurityIntegration.getLdapBindBaseDn(),
                    ldapSecurityIntegration.getLdapUserSearchAttr(),
                    ldapSecurityIntegration.getLdapBindRootDn(),
                    ldapSecurityIntegration.getLdapBindRootPwd());
            if (!authenticated) {
                throw new AuthenticationException(String.format(
                                "external ldap authentication failure for user %s@%s", user, host));
            }
        } catch (Exception e) {
            throw new AuthenticationException(String.format(
                    "external ldap authentication failure for user %s@%s with exception, error: %s",
                    user, host, e.getMessage()),
                    e);
        }
    }

    @Override
    public UserAuthenticationInfo upgradedFromPassword(UserIdentity userIdentity, Password password)
            throws AuthenticationException {
        throw new AuthenticationException("unsupported");
    }
}
