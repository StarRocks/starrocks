// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authentication.UserProperty;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.privilege.UserPrivilegeCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateUserInfo  implements Writable {
    @SerializedName(value = "u")
    UserIdentity userIdentity;
    @SerializedName(value = "a")
    UserAuthenticationInfo authenticationInfo;
    @SerializedName(value = "p")
    UserProperty userProperty;

    @SerializedName(value = "c")
    UserPrivilegeCollection userPrivilegeCollection;

    public CreateUserInfo(
            UserIdentity userIdentity,
            UserAuthenticationInfo authenticationInfo,
            UserProperty userProperty,
            UserPrivilegeCollection privilegeCollection) {
        this.userIdentity = userIdentity;
        this.authenticationInfo = authenticationInfo;
        this.userProperty = userProperty;
        this.userPrivilegeCollection = privilegeCollection;
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

    public UserPrivilegeCollection getUserPrivilegeCollection() {
        return userPrivilegeCollection;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CreateUserInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CreateUserInfo.class);
    }

}
