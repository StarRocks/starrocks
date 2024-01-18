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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/RoleManager.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql.privilege;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.common.FeConstants;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.io.Writable;
import com.starrocks.mysql.privilege.Auth.PrivLevel;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RoleManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(RoleManager.class);
    protected Map<String, Role> roles = Maps.newHashMap();

    public RoleManager() {
        roles.put(Role.OPERATOR.getRoleName(), Role.OPERATOR);
        roles.put(Role.ADMIN.getRoleName(), Role.ADMIN);
    }

    public Role getRole(String role) {
        return roles.get(role);
    }

    public Role addRole(Role newRole, boolean errOnExist) throws DdlException {
        Role existingRole = roles.get(newRole.getRoleName());
        if (existingRole != null) {
            if (errOnExist) {
                throw new DdlException("Role " + newRole + " already exists");
            }
            // merge
            existingRole.merge(newRole);
            return existingRole;
        } else {
            roles.put(newRole.getRoleName(), newRole);
            return newRole;
        }
    }

    public void dropRole(String qualifiedRole, boolean errOnNonExist) throws DdlException {
        if (!roles.containsKey(qualifiedRole)) {
            if (errOnNonExist) {
                throw new DdlException("Role " + qualifiedRole + " does not exist");
            }
            return;
        }

        // we just remove the role from this map and remain others unchanged(privs, etc..)
        roles.remove(qualifiedRole);
    }

    public Role revokePrivs(String role, TablePattern tblPattern, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        Role existingRole = roles.get(role);
        if (existingRole == null) {
            if (errOnNonExist) {
                throw new DdlException("Role " + role + " does not exist");
            }
            return null;
        }

        Map<TablePattern, PrivBitSet> map = existingRole.getTblPatternToPrivs();
        PrivBitSet existingPriv = map.get(tblPattern);
        if (existingPriv == null) {
            if (errOnNonExist) {
                throw new DdlException(tblPattern + " does not exist in role " + role);
            }
            return null;
        }

        existingPriv.remove(privs);
        return existingRole;
    }

    public Role revokePrivs(String role, ResourcePattern resourcePattern, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        Role existingRole = roles.get(role);
        if (existingRole == null) {
            if (errOnNonExist) {
                throw new DdlException("Role " + role + " does not exist");
            }
            return null;
        }

        Map<ResourcePattern, PrivBitSet> map = existingRole.getResourcePatternToPrivs();
        PrivBitSet existingPriv = map.get(resourcePattern);
        if (existingPriv == null) {
            if (errOnNonExist) {
                throw new DdlException(resourcePattern + " does not exist in role " + role);
            }
            return null;
        }

        existingPriv.remove(privs);
        return existingRole;
    }

    public Role revokePrivs(String role, UserIdentity securedUser) throws DdlException {
        Role existingRole = roles.get(role);
        if (existingRole == null) {
            throw new DdlException("Role " + role + " does not exist");
        }
        existingRole.getImpersonateUsers().remove(securedUser);
        return existingRole;
    }

    public void dropUser(UserIdentity userIdentity) {
        for (Role role : roles.values()) {
            role.dropUser(userIdentity);
        }
    }

    public List<String> getRoleNamesByUser(UserIdentity userIdentity) {
        List<String> ret = new ArrayList<>();
        for (Role role : roles.values()) {
            if (role.getUsers().contains(userIdentity)) {
                ret.add(role.getRoleName());
            }
        }
        return ret;
    }

    public void getRoleInfo(List<List<String>> results) {
        for (Role role : roles.values()) {
            List<String> info = Lists.newArrayList();
            info.add(role.getRoleName());
            info.add(Joiner.on(", ").join(role.getUsers()));

            // global
            boolean hasGlobal = false;
            for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                if (entry.getKey().getPrivLevel() == PrivLevel.GLOBAL) {
                    hasGlobal = true;
                    info.add(entry.getValue().toString());
                    // global priv should only has one
                    break;
                }
            }
            if (!hasGlobal) {
                info.add(FeConstants.NULL_STRING);
            }

            // db
            List<String> tmp = Lists.newArrayList();
            for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                if (entry.getKey().getPrivLevel() == PrivLevel.DATABASE) {
                    tmp.add(entry.getKey().toString() + ": " + entry.getValue().toString());
                }
            }
            if (tmp.isEmpty()) {
                info.add(FeConstants.NULL_STRING);
            } else {
                info.add(Joiner.on("; ").join(tmp));
            }

            // tbl
            tmp.clear();
            for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                if (entry.getKey().getPrivLevel() == PrivLevel.TABLE) {
                    tmp.add(entry.getKey().toString() + ": " + entry.getValue().toString());
                }
            }
            if (tmp.isEmpty()) {
                info.add(FeConstants.NULL_STRING);
            } else {
                info.add(Joiner.on("; ").join(tmp));
            }

            // resource
            tmp.clear();
            for (Map.Entry<ResourcePattern, PrivBitSet> entry : role.getResourcePatternToPrivs().entrySet()) {
                if (entry.getKey().getPrivLevel() == PrivLevel.RESOURCE) {
                    tmp.add(entry.getKey().toString() + ": " + entry.getValue().toString());
                }
            }
            if (tmp.isEmpty()) {
                info.add(FeConstants.NULL_STRING);
            } else {
                info.add(Joiner.on("; ").join(tmp));
            }

            results.add(info);
        }
    }

    public static RoleManager read(DataInput in) throws IOException {
        RoleManager roleManager = new RoleManager();
        roleManager.readFields(in);
        return roleManager;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // minus 2 to ignore ADMIN and OPERATOR role
        out.writeInt(roles.size() - 2);
        for (Role role : roles.values()) {
            if (role == Role.ADMIN || role == Role.OPERATOR) {
                continue;
            }
            role.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Role role = Role.read(in);
            roles.put(role.getRoleName(), role);
        }
    }

    protected void loadImpersonateRoleToUser(Map<String, Set<UserIdentity>> impersonateRoleToUser) {
        for (Map.Entry<String, Set<UserIdentity>> entry : impersonateRoleToUser.entrySet()) {
            Role role = getRole(entry.getKey());
            if (role == null) {
                LOG.error("find non-existing role {} -> impersonate users {}", entry.getKey(), entry.getValue());
                continue;
            }
            role.setImpersonateUsers(entry.getValue());
        }
    }

    protected Map<String, Set<UserIdentity>> dumpImpersonateRoleToUser() {
        Map<String, Set<UserIdentity>> ret = new HashMap<>();
        for (Map.Entry<String, Role> entry : roles.entrySet()) {
            Set<UserIdentity> users = entry.getValue().getImpersonateUsers();
            if (!users.isEmpty()) {
                ret.put(entry.getKey(), users);
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Roles: ");
        for (Role role : roles.values()) {
            sb.append(role).append("\n");
        }
        return sb.toString();
    }
}
