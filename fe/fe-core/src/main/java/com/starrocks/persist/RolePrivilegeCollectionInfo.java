// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.privilege.RolePrivilegeCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RolePrivilegeCollectionInfo implements Writable {

    @SerializedName(value = "i")
    private short pluginId;
    @SerializedName(value = "v")
    private short pluginVersion;
    @SerializedName(value = "r")
    private Map<Long, RolePrivilegeCollection> rolePrivilegeCollectionMap;

    public RolePrivilegeCollectionInfo(
            long roleId,
            RolePrivilegeCollection collection,
            short pluginId,
            short pluginVersion) {
        this.rolePrivilegeCollectionMap = new HashMap<>();
        this.rolePrivilegeCollectionMap.put(roleId, collection);
        this.pluginId = pluginId;
        this.pluginVersion = pluginVersion;
    }

    public void add(long roleId, RolePrivilegeCollection collection) {
        this.rolePrivilegeCollectionMap.put(roleId, collection);
    }

    public Map<Long, RolePrivilegeCollection> getRolePrivilegeCollectionMap() {
        return rolePrivilegeCollectionMap;
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
