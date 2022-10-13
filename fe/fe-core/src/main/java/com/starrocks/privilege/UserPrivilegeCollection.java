// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;

import java.util.HashSet;
import java.util.Set;

public class UserPrivilegeCollection extends PrivilegeCollection {
    @SerializedName(value = "r")
    private Set<Long> roleIds = null;

    public void grantRole(Long roleId) {
        if (roleIds == null) {
            roleIds = new HashSet<>();
        }
        roleIds.add(roleId);
    }

    public void revokeRole(Long roleId) {
        roleIds.remove(roleId);
    }

    public Set<Long> getAllRoles() {
        return roleIds;
    }
}