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
import com.starrocks.sql.ast.UserIdentity;

public abstract class CustomAuthenticationProvider implements AuthenticationProvider {

    public static final String PLUGIN_NAME = AuthPlugin.AUTHENTICATION_CUSTOM.name();

    @Override
    public UserAuthenticationInfo validAuthenticationInfo(UserIdentity userIdentity,
                                                          String password, String textForAuthPlugin) {
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        info.setTextForAuthPlugin(textForAuthPlugin);
        return info;
    }

    /**
     * user can override this method for custom login
     * if auth checked failed, throw an AuthenticationException, otherwise success
     */
    @Override
    public abstract void authenticate(
            String user,
            String host,
            byte[] password,
            byte[] randomString,
            UserAuthenticationInfo authenticationInfo) throws AuthenticationException;

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
