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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.privilege.RolePrivilegeCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class RolePrivilegeCollectionInfo implements Writable {

    @SerializedName(value = "i")
    private short pluginId;
    @SerializedName(value = "v")
    private short pluginVersion;
    @SerializedName(value = "r")
    private Map<Long, RolePrivilegeCollection> rolePrivCollectionModified;

    public RolePrivilegeCollectionInfo(
            Map<Long, RolePrivilegeCollection> rolePrivCollectionModified,
            short pluginId,
            short pluginVersion) {
        this.rolePrivCollectionModified = rolePrivCollectionModified;
        this.pluginId = pluginId;
        this.pluginVersion = pluginVersion;
    }

    public Map<Long, RolePrivilegeCollection> getRolePrivCollectionModified() {
        return rolePrivCollectionModified;
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

    public static RolePrivilegeCollectionInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, RolePrivilegeCollectionInfo.class);
    }
}
