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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth.PrivLevel;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

// GRANT STMT
// GRANT privilege [, privilege] ON db.tbl TO user [ROLE 'role'];
// GRANT privilege [, privilege] ON RESOURCE 'resource' TO user [ROLE 'role'];
@Deprecated         // will remove after old analyzer died
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

    public List<Privilege> getPrivileges() {
        return privileges;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (userIdent != null) {
            userIdent.analyze();
        } else {
            FeNameFormat.checkRoleName(role, false /* can not be admin */, "Can not grant to role");
        }

        if (tblPattern != null) {
            tblPattern.analyze();
        } else {
            resourcePattern.analyze();
        }

        if (privileges == null || privileges.isEmpty()) {
            throw new AnalysisException("No privileges in grant statement.");
        }

        if (tblPattern != null) {
            checkPrivileges(analyzer, privileges, role, tblPattern);
        } else {
            checkPrivileges(analyzer, privileges, role, resourcePattern);
        }
    }

    /*
     * Rules:
     * 1. NODE_PRIV can only be granted/revoked on GLOBAL level
     * 2. ADMIN_PRIV can only be granted/revoked on GLOBAL level
     * 3. Privileges can not be granted/revoked to/from ADMIN and OPERATOR role
     * 4. Only user with GLOBAL level's GRANT_PRIV can grant/revoke privileges to/from roles.
     * 5.1 User should has GLOBAL level GRANT_PRIV
     * 5.2 or user has DATABASE/TABLE level GRANT_PRIV if grant/revoke to/from certain database or table.
     * 5.3 or user should has 'resource' GRANT_PRIV if grant/revoke to/from certain 'resource'
     */
    public static void checkPrivileges(Analyzer analyzer, List<Privilege> privileges,
                                       String role, TablePattern tblPattern) throws AnalysisException {
        // Rule 1
        if (tblPattern.getPrivLevel() != PrivLevel.GLOBAL && privileges.contains(Privilege.NODE_PRIV)) {
            throw new AnalysisException("NODE_PRIV privilege can only be granted on *.*");
        }

        // Rule 2
        if (tblPattern.getPrivLevel() != PrivLevel.GLOBAL && privileges.contains(Privilege.ADMIN_PRIV)) {
            throw new AnalysisException("ADMIN_PRIV privilege can only be granted on *.*");
        }

        if (role != null) {
            // Rule 3 and 4
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        } else {
            // Rule 5.1 and 5.2
            if (tblPattern.getPrivLevel() == PrivLevel.GLOBAL) {
                if (!GlobalStateMgr.getCurrentState().getAuth()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            } else if (tblPattern.getPrivLevel() == PrivLevel.DATABASE) {
                if (!GlobalStateMgr.getCurrentState().getAuth()
                        .checkDbPriv(ConnectContext.get(), tblPattern.getQuolifiedDb(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            } else {
                // table level
                if (!GlobalStateMgr.getCurrentState().getAuth()
                        .checkTblPriv(ConnectContext.get(), tblPattern.getQuolifiedDb(), tblPattern.getTbl(),
                                PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            }
        }
    }

    public static void checkPrivileges(Analyzer analyzer, List<Privilege> privileges,
                                       String role, ResourcePattern resourcePattern) throws AnalysisException {
        // Rule 1
        if (resourcePattern.getPrivLevel() != PrivLevel.GLOBAL && privileges.contains(Privilege.NODE_PRIV)) {
            throw new AnalysisException("NODE_PRIV privilege can only be granted on resource *");
        }

        // Rule 2
        if (resourcePattern.getPrivLevel() != PrivLevel.GLOBAL && privileges.contains(Privilege.ADMIN_PRIV)) {
            throw new AnalysisException("ADMIN_PRIV privilege can only be granted on resource *");
        }

        if (role != null) {
            // Rule 3 and 4
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        } else {
            // Rule 5.1 and 5.3
            if (resourcePattern.getPrivLevel() == PrivLevel.GLOBAL) {
                if (!GlobalStateMgr.getCurrentState().getAuth()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            } else {
                if (!GlobalStateMgr.getCurrentState().getAuth()
                        .checkResourcePriv(ConnectContext.get(), resourcePattern.getResourceName(),
                                PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            }
        }
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
}
