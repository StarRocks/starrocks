// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authentication;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;

import java.util.Set;

public class AuthenticationMgrEPack extends AuthenticationMgr {
    public boolean checkUserLocked(UserIdentity userIdentity) {
        readLock();
        try {
            UserAuthenticationInfo userAuthenticationInfo = getUserAuthenticationInfoByUserIdentity(userIdentity);
            // Only plain password authentication has user info
            if (userAuthenticationInfo == null) {
                return false;
            }
            return userAuthenticationInfo.isLock();
        } finally {
            readUnlock();
        }
    }

    public boolean checkUserPasswordExpired(UserIdentity userIdentity) {
        readLock();
        try {
            UserAuthenticationInfo userAuthenticationInfo = getUserAuthenticationInfoByUserIdentity(userIdentity);
            // Only plain password authentication has user info
            if (userAuthenticationInfo == null) {
                return false;
            }
            return userAuthenticationInfo.isPasswordExpired();
        } finally {
            readUnlock();
        }
    }

    public void dropSecurityIntegration(String name, boolean isReplay) throws DdlException {
        AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        Set<String> associatedRoleMappings =
                authorizationMgr.getRoleMappingMetaMgr().getRoleMappingsForIntegration(name);
        if (!associatedRoleMappings.isEmpty()) {
            throw new DdlException((associatedRoleMappings + " role mappings are currently associated with '" +
                    name + "' security integration, need to drop those role mappings first"));
        }

        super.dropSecurityIntegration(name, isReplay);
    }
}
