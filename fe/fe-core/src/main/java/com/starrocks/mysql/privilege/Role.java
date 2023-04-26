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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/Role.java

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Role implements Writable {
    // operator is responsible for operating cluster, such as add/drop node
    public static String OPERATOR_ROLE = "operator";
    // admin is like DBA, who has all privileges except for NODE privilege held by operator
    public static String ADMIN_ROLE = "admin";

    public static Role OPERATOR = new Role(OPERATOR_ROLE,
            TablePattern.ALL, PrivBitSet.of(Privilege.NODE_PRIV, Privilege.ADMIN_PRIV),
            ResourcePattern.ALL, PrivBitSet.of(Privilege.NODE_PRIV, Privilege.ADMIN_PRIV));
    public static Role ADMIN = new Role(ADMIN_ROLE,
            TablePattern.ALL, PrivBitSet.of(Privilege.ADMIN_PRIV),
            ResourcePattern.ALL, PrivBitSet.of(Privilege.ADMIN_PRIV));

    private String roleName;
    private Map<TablePattern, PrivBitSet> tblPatternToPrivs = Maps.newConcurrentMap();
    private Map<ResourcePattern, PrivBitSet> resourcePatternToPrivs = Maps.newConcurrentMap();

    // newly added in 2.3
    private Set<UserIdentity> impersonateUsers = Sets.newConcurrentHashSet();
    // users which this role
    private Set<UserIdentity> users = Sets.newConcurrentHashSet();

    private Role() {

    }

    public Role(String roleName) {
        this.roleName = roleName;
    }

    public Role(String roleName, TablePattern tablePattern, PrivBitSet privs) {
        this.roleName = roleName;
        this.tblPatternToPrivs.put(tablePattern, privs);
    }

    public Role(String roleName, ResourcePattern resourcePattern, PrivBitSet privs) {
        this.roleName = roleName;
        this.resourcePatternToPrivs.put(resourcePattern, privs);
    }

    public Role(String roleName, TablePattern tablePattern, PrivBitSet tablePrivs,
                ResourcePattern resourcePattern, PrivBitSet resourcePrivs) {
        this.roleName = roleName;
        this.tblPatternToPrivs.put(tablePattern, tablePrivs);
        this.resourcePatternToPrivs.put(resourcePattern, resourcePrivs);
    }

    public Role(String roleName, UserIdentity securedUser) {
        this.roleName = roleName;
        this.impersonateUsers.add(securedUser);
    }

    public String getRoleName() {
        return roleName;
    }

    public Map<TablePattern, PrivBitSet> getTblPatternToPrivs() {
        return tblPatternToPrivs;
    }

    public Map<ResourcePattern, PrivBitSet> getResourcePatternToPrivs() {
        return resourcePatternToPrivs;
    }

    public Set<UserIdentity> getUsers() {
        return users;
    }

    public Set<UserIdentity> getImpersonateUsers() {
        return impersonateUsers;
    }

    public void setImpersonateUsers(Set<UserIdentity> impersonateUsers) {
        this.impersonateUsers = impersonateUsers;
    }

    public void merge(Role other) {
        Preconditions.checkState(roleName.equalsIgnoreCase(other.getRoleName()));
        for (Map.Entry<TablePattern, PrivBitSet> entry : other.getTblPatternToPrivs().entrySet()) {
            if (tblPatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = tblPatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                tblPatternToPrivs.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : other.resourcePatternToPrivs.entrySet()) {
            if (resourcePatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = resourcePatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                resourcePatternToPrivs.put(entry.getKey(), entry.getValue());
            }
        }
        this.impersonateUsers.addAll(other.impersonateUsers);
    }

    public void addUser(UserIdentity userIdent) {
        users.add(userIdent);
    }

    public void dropUser(UserIdentity userIdentity) {
        Iterator<UserIdentity> iter = users.iterator();
        while (iter.hasNext()) {
            UserIdentity userIdent = iter.next();
            if (userIdent.equals(userIdentity)) {
                iter.remove();
            }
        }
    }

    public static Role read(DataInput in) throws IOException {
        Role role = new Role();
        role.readFields(in);
        return role;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, ClusterNamespace.getFullName(roleName));
        out.writeInt(tblPatternToPrivs.size());
        for (Map.Entry<TablePattern, PrivBitSet> entry : tblPatternToPrivs.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
        out.writeInt(resourcePatternToPrivs.size());
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : resourcePatternToPrivs.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
        out.writeInt(users.size());
        for (UserIdentity userIdentity : users) {
            userIdentity.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        roleName = ClusterNamespace.getNameFromFullName(Text.readString(in));
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TablePattern tblPattern = TablePattern.read(in);
            PrivBitSet privs = PrivBitSet.read(in);
            tblPatternToPrivs.put(tblPattern, privs);
        }
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_87) {
            size = in.readInt();
            for (int i = 0; i < size; i++) {
                ResourcePattern resourcePattern = ResourcePattern.read(in);
                PrivBitSet privs = PrivBitSet.read(in);
                resourcePatternToPrivs.put(resourcePattern, privs);
            }
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            UserIdentity userIdentity = UserIdentity.read(in);
            users.add(userIdentity);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("role: ").append(roleName).append(", db table privs: ").append(tblPatternToPrivs);
        sb.append(", resource privs: ").append(resourcePatternToPrivs);
        sb.append(", users: ").append(users);
        sb.append(", impersonate: ").append(impersonateUsers);
        return sb.toString();
    }
}
