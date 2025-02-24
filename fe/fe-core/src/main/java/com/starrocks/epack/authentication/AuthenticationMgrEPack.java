// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authentication;

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class AuthenticationMgrEPack extends AuthenticationMgr {
    private static final Logger LOG = LogManager.getLogger(AuthenticationMgrEPack.class);

    @Override
    protected UserIdentity checkPasswordForNonNative(
            String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString, String authMechanism) {
        SecurityIntegration securityIntegration =
                nameToSecurityIntegrationMap.getOrDefault(authMechanism, null);
        if (securityIntegration == null) {
            LOG.info("'{}' authentication mechanism not found", authMechanism);
        } else {
            if (securityIntegration.getType().equals(SecurityIntegration.SECURITY_INTEGRATION_TYPE_LDAP)) {
                try {
                    AuthenticationProvider provider = securityIntegration.getAuthenticationProvider();
                    UserAuthenticationInfo userAuthenticationInfo = new UserAuthenticationInfo();
                    userAuthenticationInfo.extraInfo.put(
                            AuthPlugin.AUTHENTICATION_LDAP_SIMPLE_FOR_EXTERNAL.name(),
                            securityIntegration);
                    provider.authenticate(remoteUser, remoteHost, remotePasswd, randomString,
                            userAuthenticationInfo);

                    AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
                    Set<Long> roleIds = authorizationMgr.getRoleMappingMetaMgr()
                            .getMappedRoleIdsForLdapUser(securityIntegration.getName(), remoteUser);
                    if (roleIds.isEmpty()) {
                        LOG.info("authenticate '{}' with security integration '{}' successfully," +
                                        " but cannot map any role, will try other auth mechanisms",
                                remoteUser, securityIntegration.getName());
                    } else {
                        // the ephemeral user is identified as 'username'@'auth_mechanism'
                        UserIdentity authenticatedUser =
                                UserIdentity.createEphemeralUserIdent(remoteUser, authMechanism);
                        authenticatedUser.setMappedRoleIds(roleIds);
                        return authenticatedUser;
                    }
                } catch (AuthenticationException e) {
                    LOG.info("failed to authenticate, user: {}@{}, security integration: {}, error: {}",
                            remoteUser, remoteHost, securityIntegration, e.getMessage());
                }
            } else {
                LOG.warn("unsupported security integration type {} for auth mechanism {}",
                        securityIntegration.getType(), authMechanism);
            }
        }

        return null;
    }

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
