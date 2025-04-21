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
import com.starrocks.mysql.security.LdapSecurity;
import com.starrocks.qe.ConnectContext;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LDAPAuthProviderForNative implements AuthenticationProvider {
    private final String ldapServerHost;
    private final int ldapServerPort;
    private final String ldapBindRootDN;
    private final String ldapBindRootPwd;
    private final String ldapBindBaseDN;
    private final String ldapSearchFilter;
    private final String ldapUserDN;

    public LDAPAuthProviderForNative(String ldapServerHost,
                                     int ldapServerPort,
                                     String ldapBindRootDN,
                                     String ldapBindRootPwd,
                                     String ldapBindBaseDN,
                                     String ldapSearchFilter,
                                     String ldapUserDN) {
        this.ldapServerHost = ldapServerHost;
        this.ldapServerPort = ldapServerPort;
        this.ldapBindRootDN = ldapBindRootDN;
        this.ldapBindRootPwd = ldapBindRootPwd;
        this.ldapBindBaseDN = ldapBindBaseDN;
        this.ldapSearchFilter = ldapSearchFilter;
        this.ldapUserDN = ldapUserDN;
    }

    @Override
    public void authenticate(ConnectContext context, String user, String host, byte[] authResponse)
            throws AuthenticationException {
        //clear password terminate string
        byte[] clearPassword = authResponse;
        if (authResponse[authResponse.length - 1] == 0) {
            clearPassword = Arrays.copyOf(authResponse, authResponse.length - 1);
        }
        if (!Strings.isNullOrEmpty(ldapUserDN)) {
            if (!LdapSecurity.checkPassword(ldapUserDN, new String(clearPassword, StandardCharsets.UTF_8),
                    ldapServerHost, ldapServerPort)) {
                throw new AuthenticationException("Failed to authenticate for [user: " + user + "] by ldap");
            }
        } else {
            if (!LdapSecurity.checkPasswordByRoot(user, new String(clearPassword, StandardCharsets.UTF_8),
                    ldapServerHost, ldapServerPort, ldapBindRootDN, ldapBindRootPwd, ldapBindBaseDN, ldapSearchFilter)) {
                throw new AuthenticationException("Failed to authenticate for [user: " + user + "] by ldap");
            }
        }
    }
}
