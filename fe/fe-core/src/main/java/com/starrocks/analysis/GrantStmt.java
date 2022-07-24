// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/GrantStmt.java

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

package com.starrocks.analysis;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.starrocks.catalog.AccessPrivilege;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.sql.ast.AstVisitor;

import java.util.List;

// GRANT STMT
// GRANT privilege [, privilege] ON db.tbl TO user [ROLE 'role'];
// GRANT privilege [, privilege] ON RESOURCE 'resource' TO user [ROLE 'role'];
public class GrantStmt extends DdlStmt {
    private UserIdentity userIdent;
    private String role;
    private TablePattern tblPattern;
    private ResourcePattern resourcePattern;
    private List<Privilege> privileges;

    public GrantStmt(UserIdentity userIdent, String role, TablePattern tblPattern, List<AccessPrivilege> privileges) {
        this.userIdent = userIdent;
        this.role = role;
        this.tblPattern = tblPattern;
        this.resourcePattern = null;
        PrivBitSet privs = PrivBitSet.of();
        for (AccessPrivilege accessPrivilege : privileges) {
            privs.or(accessPrivilege.toPrivilege());
        }
        this.privileges = privs.toPrivilegeList();
    }

    public GrantStmt(UserIdentity userIdent, String role, ResourcePattern resourcePattern,
                     List<AccessPrivilege> privileges) {
        this.userIdent = userIdent;
        this.role = role;
        this.tblPattern = null;
        this.resourcePattern = resourcePattern;
        PrivBitSet privs = PrivBitSet.of();
        for (AccessPrivilege accessPrivilege : privileges) {
            privs.or(accessPrivilege.toPrivilege());
        }
        this.privileges = privs.toPrivilegeList();
    }

    /**
     * call by TablePrivEntry, DbPrivEntry, GlobalPrivEntry to transfer to SQL
     */
    public GrantStmt(UserIdentity userIdentity, TablePattern tblPattern, PrivBitSet bitSet) {
        this.userIdent = userIdentity;
        this.role = null;
        this.tblPattern = tblPattern;
        this.resourcePattern = null;
        this.privileges = bitSet.toPrivilegeList();
    }

    /**
     * call by ResourcePrivEntry to transfer to SQL
     */
    public GrantStmt(UserIdentity userIdentity, ResourcePattern resourcePattern, PrivBitSet bitSet) {
        this.userIdent = userIdentity;
        this.role = null;
        this.tblPattern = null;
        this.resourcePattern = resourcePattern;
        this.privileges = bitSet.toPrivilegeList();
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public TablePattern getTblPattern() {
        return tblPattern;
    }

    public ResourcePattern getResourcePattern() {
        return resourcePattern;
    }

    public boolean hasRole() {
        return !Strings.isNullOrEmpty(role);
    }

    public String getQualifiedRole() {
        return role;
    }

    public void setQualifiedRole(String role) {
        this.role = role;
    }

    public List<Privilege> getPrivileges() {
        return privileges;
    }

    public boolean hasPrivileges() {
        return !(privileges == null || privileges.isEmpty());
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT ").append(Joiner.on(", ").join(privileges));
        if (tblPattern != null) {
            sb.append(" ON ").append(tblPattern).append(" TO ");
        } else {
            sb.append(" ON RESOURCE '").append(resourcePattern).append("' TO ");
        }
        if (!Strings.isNullOrEmpty(role)) {
            sb.append(" ROLE '").append(role).append("'");
        } else {
            sb.append(userIdent);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantPrivilegeStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
