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

import com.starrocks.mysql.privilege.AuthPlugin;

public class AuthenticationProviderFactory {
    private AuthenticationProviderFactory() {
    }

    public static AuthenticationProvider create(String plugin) {
        if (plugin == null) {
            return null;
        }

        try {
            AuthPlugin.Server authPlugin = AuthPlugin.Server.valueOf(plugin);
            return authPlugin.getProvider();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
