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
import com.starrocks.authz.authentication.UserProperty;
import com.starrocks.authz.authorization.ObjectTypeDeprecate;
import com.starrocks.authz.authorization.PrivilegeEntry;
import com.starrocks.authz.authorization.UserPrivilegeCollection;
import com.starrocks.authz.authorization.UserPrivilegeCollectionV2;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.UserIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CreateUserInfo implements Writable {
    @SerializedName(value = "u")
    UserIdentity userIdentity;
    @SerializedName(value = "a")
    UserAuthenticationInfo authenticationInfo;
    @SerializedName(value = "p")
    UserProperty userProperty;

    //Deprecated attribute, can be removed in version 3.2
    @SerializedName(value = "c")
    @Deprecated
    UserPrivilegeCollection userPrivilegeCollection;

    @SerializedName(value = "c2")
    UserPrivilegeCollectionV2 userPrivilegeCollectionV2;

    @SerializedName(value = "i")
    short pluginId;

    @SerializedName(value = "v")
    short pluginVersion;

    public CreateUserInfo(
            UserIdentity userIdentity,
            UserAuthenticationInfo authenticationInfo,
            UserProperty userProperty,
            UserPrivilegeCollectionV2 privilegeCollection,
            short pluginId,
            short pluginVersion) {
        this.userIdentity = userIdentity;
        this.authenticationInfo = authenticationInfo;
        this.userProperty = userProperty;
        this.userPrivilegeCollectionV2 = privilegeCollection;
        this.pluginId = pluginId;
        this.pluginVersion = pluginVersion;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public UserAuthenticationInfo getAuthenticationInfo() {
        return authenticationInfo;
    }

    public UserProperty getUserProperty() {
        return userProperty;
    }

    public UserPrivilegeCollectionV2 getUserPrivilegeCollection() {
        if (userPrivilegeCollectionV2 == null) {
            UserPrivilegeCollectionV2 collection = new UserPrivilegeCollectionV2();
            collection.grantRoles(userPrivilegeCollection.getAllRoles());
            collection.setDefaultRoleIds(userPrivilegeCollection.getDefaultRoleIds());

            Map<ObjectTypeDeprecate, List<PrivilegeEntry>> m = userPrivilegeCollection.getTypeToPrivilegeEntryList();
            for (Map.Entry<ObjectTypeDeprecate, List<PrivilegeEntry>> e : m.entrySet()) {
                collection.getTypeToPrivilegeEntryList().put(e.getKey().toObjectType(), e.getValue());
            }
            return collection;
        } else {
            return userPrivilegeCollectionV2;
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

    public static CreateUserInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        CreateUserInfo ret = GsonUtils.GSON.fromJson(json, CreateUserInfo.class);
        try {
            ret.authenticationInfo.analyze();
        } catch (AuthenticationException e) {
            throw new IOException(e);
        }
        return ret;
    }

}
