// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.collect.Maps;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.SecurityIntegrationFactory;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
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
                    LOG.debug("failed to authenticate, user: {}@{}, security integration: {}, error: {}",
                            remoteUser, remoteHost, securityIntegration, e.getMessage());
                }
            } else {
                LOG.warn("unsupported security integration type {} for auth mechanism {}",
                        securityIntegration.getType(), authMechanism);
            }
        }

        return null;
    }

    @Override
    public void createSecurityIntegration(String name,
                                          Map<String, String> propertyMap,
                                          boolean isReplay) throws DdlException {
        SecurityIntegration securityIntegration;
        try {
            securityIntegration =
                    SecurityIntegrationFactory.createSecurityIntegration(name, propertyMap);
        } catch (DdlException e) {
            throw new DdlException("failed to create security integration, error: " + e.getMessage(), e);
        }
        // atomic op
        SecurityIntegration result = nameToSecurityIntegrationMap.putIfAbsent(name, securityIntegration);
        if (result != null) {
            throw new DdlException("security integration '" + name + "' already exists");
        }
        if (!isReplay) {
            GlobalStateMgr.getCurrentState().getEditLog().logCreateSecurityIntegration(name, propertyMap);
            LOG.info("finished to create security integration '{}'", securityIntegration.toString());
        }
    }

    @Override
    public void alterSecurityIntegration(String name, Map<String, String> alterProps,
                                         boolean isReplay) throws DdlException {
        SecurityIntegration securityIntegration = nameToSecurityIntegrationMap.get(name);
        if (securityIntegration == null) {
            throw new DdlException("security integration '" + name + "' not found");
        } else {
            // COW
            Map<String, String> newProps = Maps.newHashMap(securityIntegration.getPropertyMap());
            // update props
            newProps.putAll(alterProps);
            SecurityIntegration newSecurityIntegration;
            try {
                newSecurityIntegration = SecurityIntegrationFactory.createSecurityIntegration(name, newProps);
            } catch (DdlException e) {
                throw new DdlException("failed to alter security integration, error: " + e.getMessage(), e);
            }
            // update map
            nameToSecurityIntegrationMap.put(name, newSecurityIntegration);
            if (!isReplay) {
                GlobalStateMgr.getCurrentState().getEditLog().logAlterSecurityIntegration(name, alterProps);
                LOG.info("finished to alter security integration '{}' with updated properties {}",
                        name, alterProps);
            }
        }
    }

    @Override
    public void dropSecurityIntegration(String name, boolean isReplay) throws DdlException {
        AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        Set<String> associatedRoleMappings =
                authorizationMgr.getRoleMappingMetaMgr().getRoleMappingsForIntegration(name);
        if (!associatedRoleMappings.isEmpty()) {
            throw new DdlException((associatedRoleMappings + " role mappings are currently associated with '" +
                    name + "' security integration, need to drop those role mappings first"));
        }

        if (!nameToSecurityIntegrationMap.containsKey(name)) {
            throw new DdlException("security integration '" + name + "' not found");
        }

        SecurityIntegration result = nameToSecurityIntegrationMap.remove(name);
        if (!isReplay && result != null) {
            GlobalStateMgr.getCurrentState().getEditLog().logDropSecurityIntegration(name);
            LOG.info("finished to drop security integration '{}'", name);
        }
    }

    @Override
    public void replayCreateSecurityIntegration(String name, Map<String, String> propertyMap)
            throws DdlException {
        createSecurityIntegration(name, propertyMap, true);
    }

    @Override
    public void replayAlterSecurityIntegration(String name, Map<String, String> alterProps)
            throws DdlException {
        alterSecurityIntegration(name, alterProps, true);
    }

    @Override
    public void replayDropSecurityIntegration(String name)
            throws DdlException {
        dropSecurityIntegration(name, true);
    }

}
