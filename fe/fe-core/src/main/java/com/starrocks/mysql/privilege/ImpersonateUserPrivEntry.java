// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mysql.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;

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
    public int compareTo(PrivEntry other) {
        if (!(other instanceof ImpersonateUserPrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        ImpersonateUserPrivEntry otherEntry = (ImpersonateUserPrivEntry) other;
        int res = origHost.compareTo(otherEntry.origHost);
        if (res != 0) {
            return -res;
        }

        res = origUser.compareTo(otherEntry.origUser);
        if (res != 0) {
            return -res;
        }

        res = securedUserIdentity.getHost().compareTo(otherEntry.securedUserIdentity.getHost());
        if (res != 0) {
            return -res;
        }

        return securedUserIdentity.getQualifiedUser().compareTo(otherEntry.securedUserIdentity.getQualifiedUser());
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof ImpersonateUserPrivEntry)) {
            return false;
        }

        ImpersonateUserPrivEntry otherEntry = (ImpersonateUserPrivEntry) other;
        return origHost.equals(otherEntry.origHost) && origUser.equals(otherEntry.origUser)
                && securedUserIdentity.equals(otherEntry.securedUserIdentity) && isDomain == otherEntry.isDomain;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("impersonate user priv. host: ").append(origHost).append(", securedUser: ").append(securedUserIdentity);
        sb.append(", user: ").append(origUser);
        sb.append(", priv: ").append(privSet).append(", set by resolver: ").append(isSetByDomainResolver);
        return sb.toString();
    }
}
