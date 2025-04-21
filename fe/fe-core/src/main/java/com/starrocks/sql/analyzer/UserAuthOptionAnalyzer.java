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

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Arrays;

public class UserAuthOptionAnalyzer {
    public static UserAuthenticationInfo analyzeAuthOption(UserIdentity userIdentity, UserAuthOption userAuthOption) {
        try {
            String authPluginUsing;
            if (userAuthOption == null || userAuthOption.getAuthPlugin() == null) {
                authPluginUsing = AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString();
            } else {
                authPluginUsing = userAuthOption.getAuthPlugin();
            }

            try {
                AuthPlugin.Server.valueOf(authPluginUsing);
            } catch (IllegalArgumentException e) {
                throw new SemanticException(
                        "Cannot find " + authPluginUsing + " from " + Arrays.toString(AuthPlugin.Client.values()));
            }

            UserAuthenticationInfo info = new UserAuthenticationInfo();
            info.setAuthPlugin(authPluginUsing);
            if (authPluginUsing.equalsIgnoreCase(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString())) {
                byte[] passwordScrambled = MysqlPassword.EMPTY_PASSWORD;
                if (userAuthOption != null) {
                    passwordScrambled = scramblePassword(userAuthOption.getAuthString(), userAuthOption.isPasswordPlain());
                }
                info.setPassword(passwordScrambled);
                info.setAuthString(null);
            } else {
                info.setPassword(MysqlPassword.EMPTY_PASSWORD);
                info.setAuthString(userAuthOption == null ? null : userAuthOption.getAuthString());
            }

            info.setOrigUserHost(userIdentity.getUser(), userIdentity.getHost());

            return info;
        } catch (AuthenticationException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    /**
     * Get scrambled password from plain password
     */
    private static byte[] scramblePassword(String originalPassword, boolean isPasswordPlain) {
        if (Strings.isNullOrEmpty(originalPassword)) {
            return MysqlPassword.EMPTY_PASSWORD;
        }
        if (isPasswordPlain) {
            return MysqlPassword.makeScrambledPassword(originalPassword);
        } else {
            return MysqlPassword.checkPassword(originalPassword);
        }
    }
}
