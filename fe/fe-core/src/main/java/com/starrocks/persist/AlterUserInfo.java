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


package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.sql.ast.UserIdentity;

import java.io.IOException;
import java.util.Map;

public class AlterUserInfo implements Writable, GsonPostProcessable {
    @SerializedName(value = "u")
    UserIdentity userIdentity;
    @SerializedName(value = "a")
    UserAuthenticationInfo authenticationInfo;

    @SerializedName(value = "p")
    Map<String, String> properties;

    public AlterUserInfo(UserIdentity userIdentity, UserAuthenticationInfo authenticationInfo) {
        this.userIdentity = userIdentity;
        this.authenticationInfo = authenticationInfo;
    }

    public AlterUserInfo(UserIdentity userIdentity, UserAuthenticationInfo authenticationInfo, Map<String, String> properties) {
        this(userIdentity, authenticationInfo);
        this.properties = properties;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public UserAuthenticationInfo getAuthenticationInfo() {
        return authenticationInfo;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        try {
            authenticationInfo.analyze();
        } catch (AuthenticationException e) {
            throw new IOException(e);
        }
    }
}
