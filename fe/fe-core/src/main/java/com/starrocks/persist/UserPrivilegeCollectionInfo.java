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
import com.starrocks.privilege.UserPrivilegeCollection;
import com.starrocks.privilege.UserPrivilegeCollectionV2;
import com.starrocks.sql.ast.UserIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

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
        try {
            return GsonUtils.GSON.fromJson(json, UserPrivilegeCollectionInfo.class);
        } catch (Exception e) {
            /*
             * Because 3.0.3 introduced a bug, the upgrade of UserPrivilegeCollectionV2
             * was not compatible and the original SerializedName "p" was used directly,
             * resulting in inconsistent types after the upgrade.
             * Fault tolerance code has been added here.
             * If UserPrivilegeCollectionV2 cannot be parsed correctly,
             * need to use UserPrivilegeCollectionInfoDeprecated to parse.
             */
            UserPrivilegeCollectionInfoDeprecated deprecated
                    = GsonUtils.GSON.fromJson(json, UserPrivilegeCollectionInfoDeprecated.class);

            UserPrivilegeCollection collectionDeprecate = deprecated.getPrivilegeCollection();
            UserPrivilegeCollectionV2 collection = new UserPrivilegeCollectionV2();
            collection.grantRoles(collectionDeprecate.getAllRoles());
            collection.setDefaultRoleIds(collectionDeprecate.getDefaultRoleIds());

            Map<ObjectTypeDeprecate, List<PrivilegeEntry>> m = collectionDeprecate.getTypeToPrivilegeEntryList();
            for (Map.Entry<ObjectTypeDeprecate, List<PrivilegeEntry>> entry : m.entrySet()) {
                collection.getTypeToPrivilegeEntryList().put(entry.getKey().toObjectType(), entry.getValue());
            }

            return new UserPrivilegeCollectionInfo(deprecated.getUserIdentity(), collection,
                    deprecated.pluginId, deprecated.pluginVersion);
        }
    }
}
