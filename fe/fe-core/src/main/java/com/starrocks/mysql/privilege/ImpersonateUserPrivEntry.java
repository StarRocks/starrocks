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


package com.starrocks.mysql.privilege;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRevokeClause;
import com.starrocks.sql.ast.GrantRevokePrivilegeObjects;
import com.starrocks.sql.ast.UserIdentity;

public class ImpersonateUserPrivEntry extends PrivEntry {
    @SerializedName(value = "securedUserIdentity")
    private UserIdentity securedUserIdentity;

    /**
     * Allow empty construction for Gson
     */
    public ImpersonateUserPrivEntry() {
    }

    protected ImpersonateUserPrivEntry(
            UserIdentity authorizedUser, PrivBitSet privSet, UserIdentity securedUserIdentity) {
        super(authorizedUser.getHost(), authorizedUser.getQualifiedUser(), authorizedUser.isDomain(), privSet);
        this.securedUserIdentity = securedUserIdentity;
    }

    public static ImpersonateUserPrivEntry create(
            UserIdentity authorizedUser, UserIdentity securedUserIdentity) throws AnalysisException {
        ImpersonateUserPrivEntry entry = new ImpersonateUserPrivEntry(
                authorizedUser, PrivBitSet.of(Privilege.IMPERSONATE_PRIV), securedUserIdentity);
        entry.analyse();
        return entry;
    }

    public UserIdentity getSecuredUserIdentity() {
        return securedUserIdentity;
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof ImpersonateUserPrivEntry)) {
            return false;
        }

        ImpersonateUserPrivEntry otherEntry = (ImpersonateUserPrivEntry) other;
        return origHost.equals(otherEntry.origHost) && realOrigUser.equals(otherEntry.realOrigUser)
                && securedUserIdentity.equals(otherEntry.securedUserIdentity) && isDomain == otherEntry.isDomain;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("impersonate user priv. host: ").append(origHost).append(", securedUser: ").append(securedUserIdentity);
        sb.append(", user: ").append(realOrigUser);
        sb.append(", priv: ").append(privSet).append(", set by resolver: ").append(isSetByDomainResolver);
        return sb.toString();
    }

    @Override
    public String toGrantSQL() {
        GrantRevokePrivilegeObjects grantRevokePrivilegeObjects = new GrantRevokePrivilegeObjects();
        grantRevokePrivilegeObjects.setUserPrivilegeObjectList(Lists.newArrayList(securedUserIdentity));
        GrantPrivilegeStmt stmt = new GrantPrivilegeStmt(
                Lists.newArrayList("IMPERSONATE"), "USER",
                new GrantRevokeClause(getUserIdent(), null),
                grantRevokePrivilegeObjects, false);
        stmt.setPrivBitSet(PrivBitSet.of(Privilege.IMPERSONATE_PRIV));
        return AstToStringBuilder.toString(stmt);
    }
}
