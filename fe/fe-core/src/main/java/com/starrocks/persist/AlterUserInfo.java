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
import com.starrocks.authz.authentication.AuthenticationException;
import com.starrocks.authz.authentication.UserAuthenticationInfo;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.UserIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlterUserInfo implements Writable {
    @SerializedName(value = "u")
    UserIdentity userIdentity;
    @SerializedName(value = "a")
    UserAuthenticationInfo authenticationInfo;

    public AlterUserInfo(UserIdentity userIdentity, UserAuthenticationInfo authenticationInfo) {
        this.userIdentity = userIdentity;
        this.authenticationInfo = authenticationInfo;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public UserAuthenticationInfo getAuthenticationInfo() {
        return authenticationInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterUserInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        AlterUserInfo ret = GsonUtils.GSON.fromJson(json, AlterUserInfo.class);
        try {
            ret.authenticationInfo.analyze();
        } catch (AuthenticationException e) {
            throw new IOException(e);
        }
        return ret;
    }
}
