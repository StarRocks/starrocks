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
import com.starrocks.privilege.ObjectTypeDeprecate;
import com.starrocks.privilege.PrivilegeEntry;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.RolePrivilegeCollection;
import com.starrocks.privilege.RolePrivilegeCollectionV2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RolePrivilegeCollectionInfo implements Writable {

    @SerializedName(value = "i")
    private short pluginId;
    @SerializedName(value = "v")
    private short pluginVersion;

    //Deprecated attribute, can be removed in version 3.2
    @Deprecated
    @SerializedName(value = "r")
    private Map<Long, RolePrivilegeCollection> rolePrivCollectionModified;

    @SerializedName(value = "r2")
    private Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModifiedV2;

    public RolePrivilegeCollectionInfo(
            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified,
            short pluginId,
            short pluginVersion) {
        this.rolePrivCollectionModifiedV2 = rolePrivCollectionModified;
        this.pluginId = pluginId;
        this.pluginVersion = pluginVersion;
    }

    public Map<Long, RolePrivilegeCollectionV2> getRolePrivCollectionModified() throws PrivilegeException {
        if (rolePrivCollectionModifiedV2 == null) {
            Map<Long, RolePrivilegeCollectionV2> rolePrivMap = new HashMap<>();
            for (Map.Entry<Long, RolePrivilegeCollection> rolePrivilegeCollectionEntry : rolePrivCollectionModified.entrySet()) {
                RolePrivilegeCollection collectionDeprecate = rolePrivilegeCollectionEntry.getValue();

                RolePrivilegeCollectionV2 rolePrivilegeCollection;
                if (collectionDeprecate.isRemovable() && collectionDeprecate.isMutable()) {
                    rolePrivilegeCollection = new RolePrivilegeCollectionV2(collectionDeprecate.getName(),
                            collectionDeprecate.getComment(),
                            RolePrivilegeCollectionV2.RoleFlags.REMOVABLE,
                            RolePrivilegeCollectionV2.RoleFlags.MUTABLE);
                } else if (collectionDeprecate.isRemovable()) {
                    rolePrivilegeCollection = new RolePrivilegeCollectionV2(collectionDeprecate.getName(),
                            collectionDeprecate.getComment(),
                            RolePrivilegeCollectionV2.RoleFlags.REMOVABLE);
                } else if (collectionDeprecate.isMutable()) {
                    rolePrivilegeCollection = new RolePrivilegeCollectionV2(collectionDeprecate.getName(),
                            collectionDeprecate.getComment(),
                            RolePrivilegeCollectionV2.RoleFlags.MUTABLE);
                } else {
                    rolePrivilegeCollection = new RolePrivilegeCollectionV2(collectionDeprecate.getName(),
                            collectionDeprecate.getComment());
                }

                for (Long r : collectionDeprecate.getParentRoleIds()) {
                    rolePrivilegeCollection.addParentRole(r);
                }

                for (Long r : collectionDeprecate.getSubRoleIds()) {
                    rolePrivilegeCollection.addSubRole(r);
                }

                Map<ObjectTypeDeprecate, List<PrivilegeEntry>> m = collectionDeprecate.getTypeToPrivilegeEntryList();
                for (Map.Entry<ObjectTypeDeprecate, List<PrivilegeEntry>> e : m.entrySet()) {
                    rolePrivilegeCollection.getTypeToPrivilegeEntryList().put(e.getKey().toObjectType(), e.getValue());
                }

                rolePrivMap.put(rolePrivilegeCollectionEntry.getKey(), rolePrivilegeCollection);
            }
            return rolePrivMap;
        } else {
            return rolePrivCollectionModifiedV2;
        }
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
