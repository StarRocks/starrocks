package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.privilege.RolePrivilegeCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RolePrivilegeCollectionInfo implements Writable {

    @SerializedName(value = "i")
    public short pluginId;
    @SerializedName(value = "v")
    public short pluginVersion;
    @SerializedName(value = "ri")
    private long roleId;
    @SerializedName(value = "p")
    private RolePrivilegeCollection privilegeCollection;

    public RolePrivilegeCollectionInfo(
            long roleId,
            RolePrivilegeCollection rolePrivilegeCollectionInfo,
            short pluginId,
            short pluginVersion) {
        this.roleId = roleId;
        this.privilegeCollection = rolePrivilegeCollectionInfo;
        this.pluginId = pluginId;
        this.pluginVersion = pluginVersion;
    }

    public long getRoleId() {
        return roleId;
    }

    public RolePrivilegeCollection getPrivilegeCollection() {
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

    public static RolePrivilegeCollectionInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, RolePrivilegeCollectionInfo.class);
    }
}
