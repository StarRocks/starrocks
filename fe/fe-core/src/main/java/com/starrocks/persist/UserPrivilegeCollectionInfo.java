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
import com.starrocks.authz.authorization.UserPrivilegeCollectionV2;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.UserIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPrivilegeCollectionInfo implements Writable {

    @SerializedName(value = "i")
    public short pluginId;
    @SerializedName(value = "v")
    public short pluginVersion;
    @SerializedName(value = "u")
    private UserIdentity userIdentity;

    @SerializedName(value = "p")
    private UserPrivilegeCollectionV2 privilegeCollection;

    public UserPrivilegeCollectionInfo(
            UserIdentity userIdentity,
            UserPrivilegeCollectionV2 userPrivilegeCollection,
            short pluginId,
            short pluginVersion) {
        this.userIdentity = userIdentity;
        this.privilegeCollection = userPrivilegeCollection;
        this.pluginId = pluginId;
        this.pluginVersion = pluginVersion;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public UserPrivilegeCollectionV2 getPrivilegeCollection() {
        return privilegeCollection;
    }

    public short getPluginId() {
        return pluginId;
    }

    public short getPluginVersion() {
        return pluginVersion;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static UserPrivilegeCollectionInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, UserPrivilegeCollectionInfo.class);
    }
}
