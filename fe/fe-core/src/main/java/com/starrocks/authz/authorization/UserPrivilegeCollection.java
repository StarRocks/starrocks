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
package com.starrocks.authz.authorization;

import com.google.gson.annotations.SerializedName;

import java.util.HashSet;
import java.util.Set;

/**
 * Deprecated class, can be removed in version 3.2
 */
@Deprecated
public class UserPrivilegeCollection extends PrivilegeCollection {
    @SerializedName(value = "r")
    private Set<Long> roleIds;

    @SerializedName(value = "d")
    private Set<Long> defaultRoleIds;

    public UserPrivilegeCollection() {
        super();
        roleIds = new HashSet<>();
        defaultRoleIds = new HashSet<>();
    }

    public void grantRole(Long roleId) {
        roleIds.add(roleId);
    }

    public void grantRoles(Set<Long> roleIds) {
        this.roleIds.addAll(roleIds);
    }

    public void revokeRole(Long roleId) {
        roleIds.remove(roleId);
        defaultRoleIds.remove(roleId);
    }

    public Set<Long> getAllRoles() {
        return roleIds;
    }

    public void setDefaultRoleIds(Set<Long> defaultRoleIds) {
        this.defaultRoleIds = defaultRoleIds;
    }

    public Set<Long> getDefaultRoleIds() {
        if (defaultRoleIds == null) {
            return new HashSet<>();
        } else {
            return defaultRoleIds;
        }
    }
}
