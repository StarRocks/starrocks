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

import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;

import java.util.Arrays;

public class UserAuthOptionAnalyzer {
    public static void analyzeAuthOption(UserRef user, UserAuthOption userAuthOption) {
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

        if (authPluginUsing.equalsIgnoreCase(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString())) {
            if (userAuthOption != null && !userAuthOption.isPasswordPlain()) {
                MysqlPassword.checkPassword(userAuthOption.getAuthString());
            }
        }

        PatternMatcher.createMysqlPattern(user.getUser(), CaseSensibility.USER.getCaseSensibility());
        PatternMatcher.createMysqlPattern(user.getHost(), CaseSensibility.HOST.getCaseSensibility());
    }
}
