// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.privilege.UserPrivilegeCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPrivilegeCollectionInfo implements Writable  {

    @SerializedName(value = "i")
    public short pluginId;
    @SerializedName(value = "v")
    public short pluginVersion;
    @SerializedName(value = "u")
    private UserIdentity userIdentity;

    @SerializedName(value = "p")
    private UserPrivilegeCollection privilegeCollection;

    public UserPrivilegeCollectionInfo(
            UserIdentity userIdentity,
            UserPrivilegeCollection userPrivilegeCollection,
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

    public UserPrivilegeCollection getPrivilegeCollection() {
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
