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

import com.starrocks.common.conf.Config;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.privilege.Password;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;

import java.lang.reflect.Method;

public class KerberosAuthenticationProvider implements AuthenticationProvider {
    public static final String PLUGIN_NAME = AuthPlugin.AUTHENTICATION_KERBEROS.name();

    @Override
    public UserAuthenticationInfo validAuthenticationInfo(UserIdentity userIdentity, String password,
                                                          String textForAuthPlugin) throws AuthenticationException {
        if (!GlobalStateMgr.getCurrentState().getAuthenticationMgr().isSupportKerberosAuth()) {
            throw new AuthenticationException("Not support kerberos authentication");
        }

        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        if (textForAuthPlugin == null) {
            info.setTextForAuthPlugin(Config.authentication_kerberos_service_principal.split("@")[1]);
        } else {
            info.setTextForAuthPlugin(textForAuthPlugin);
        }
        return info;
    }

    @Override
    public void authenticate(String user, String host, byte[] password, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        try {
            String userForAuthPlugin = authenticationInfo.getTextForAuthPlugin();

            Class<?> authClazz = GlobalStateMgr.getCurrentState().getAuthenticationMgr().getAuthClazz();
            Method method = authClazz.getMethod("authenticate",
                    String.class, String.class, String.class, byte[].class);
            boolean result = (boolean) method.invoke(null,
                    Config.authentication_kerberos_service_principal,
                    Config.authentication_kerberos_service_key_tab,
                    user + "@" + userForAuthPlugin, password);
            if (!result) {
                throw new AuthenticationException("Failed to authenticate for [user: " + user + "] by kerberos");
            }
        } catch (Exception e) {
            throw new AuthenticationException("Failed to authenticate for [user: " + user + "] " +
                    "by kerberos, msg: " + e.getMessage());
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
