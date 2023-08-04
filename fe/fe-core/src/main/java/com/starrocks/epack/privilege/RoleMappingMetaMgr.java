// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.common.DdlException;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RoleMappingMetaMgr {
    private static final Logger LOG = LogManager.getLogger(RoleMappingMetaMgr.class);

    /**
     * role mapping name -> role mapping object
     */
    @SerializedName("nm")
    private final Map<String, RoleMapping> nameToRoleMappingMap;
    /**
     * role name -> role mapping set
     */
    @SerializedName("rm")
    private final Map<String, Set<String>> roleToRoleMappingsMap;
    /**
     * security integration name -> (group dn -> role name linked list)
     */
    @SerializedName("gm")
    private final Map<String, Map<String, List<String>>> groupToRolesMap;
    private final ReentrantReadWriteLock lock;

    public RoleMappingMetaMgr() {
        nameToRoleMappingMap = new HashMap<>();
        roleToRoleMappingsMap = new HashMap<>();
        groupToRolesMap = new HashMap<>();
        lock = new ReentrantReadWriteLock();
    }

    public Map<String, Set<String>> getRoleToRoleMappingsMap() {
        return roleToRoleMappingsMap;
    }

    public Map<String, Map<String, List<String>>> getGroupToRolesMap() {
        return groupToRolesMap;
    }

    public Set<RoleMapping> getAllRoleMappings() {
        Set<RoleMapping> roleMappings = Sets.newHashSet();
        try {
            lock.readLock().lock();
            roleMappings.addAll(nameToRoleMappingMap.values());
        } finally {
            lock.readLock().unlock();
        }

        return roleMappings;
    }

    public RoleMapping getRoleMapping(String roleMappingName) {
        try {
            lock.readLock().lock();
            return nameToRoleMappingMap.get(roleMappingName);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<String> getRoleMappingsForIntegration(String integrationName) {
        try {
            lock.readLock().lock();
            Set<String> names = Sets.newHashSet();
            for (RoleMapping roleMapping : nameToRoleMappingMap.values()) {
                if (Objects.equals(roleMapping.getIntegrationName(), integrationName)) {
                    names.add(roleMapping.getName());
                }
            }
            return names;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<String> getRoleMappingsForRole(String roleName) {
        try {
            lock.readLock().lock();
            Set<String> names = Sets.newHashSet();
            Set<String> relatedRoleMappings = roleToRoleMappingsMap.get(roleName);
            if (relatedRoleMappings != null) {
                names.addAll(relatedRoleMappings);
            }
            return names;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void addRoleMapping(RoleMapping roleMapping, String integrationType)
            throws DdlException {
        try {
            lock.writeLock().lock();

            if (nameToRoleMappingMap.containsKey(roleMapping.getName())) {
                throw new DdlException("role mapping '" + roleMapping.getName() + "' already exists");
            }

            nameToRoleMappingMap.put(roleMapping.getName(), roleMapping);
            roleToRoleMappingsMap.computeIfAbsent(roleMapping.getRoleName(),
                    k -> Sets.newHashSet()).add(roleMapping.getName());
            if (integrationType.equals(SecurityIntegration.SECURITY_INTEGRATION_TYPE_LDAP)) {
                LDAPRoleMapping ldapRoleMapping = (LDAPRoleMapping) roleMapping;
                Set<String> groups = ldapRoleMapping.getGroupSet();
                Map<String, List<String>> groupDnToRoleList =
                        groupToRolesMap.computeIfAbsent(ldapRoleMapping.getIntegrationName(),
                                k -> Maps.newHashMap());
                groups.forEach(g -> groupDnToRoleList.computeIfAbsent(g,
                        k -> Lists.newLinkedList()).add(ldapRoleMapping.getRoleName()));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void modifyRoleMapping(String name, Map<String, String> alterProps, String integrationType)
            throws DdlException, PrivilegeException {
        try {
            lock.writeLock().lock();

            if (!nameToRoleMappingMap.containsKey(name)) {
                throw new DdlException("role mapping '" + name + "' not found");
            } else {
                RoleMapping roleMapping = nameToRoleMappingMap.get(name);
                String finalIntegrationType =
                        integrationType == null ? roleMapping.getIntegrationType() : integrationType;
                Map<String, String> newProps = roleMapping.getProperties();
                // update properties
                newProps.putAll(alterProps);
                // remove it first
                removeRoleMapping(name);
                // create and add a new one
                RoleMapping newRoleMapping =
                        RoleMappingFactory.createRoleMapping(name, newProps, finalIntegrationType);
                addRoleMapping(newRoleMapping, finalIntegrationType);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeRoleMapping(String name) throws DdlException {
        try {
            lock.writeLock().lock();

            if (!nameToRoleMappingMap.containsKey(name)) {
                throw new DdlException("role mapping '" + name + "' not found");
            } else {
                RoleMapping roleMapping = nameToRoleMappingMap.get(name);

                // remove from `roleToRoleMappingsMap`
                Set<String> roleMappings = roleToRoleMappingsMap.get(roleMapping.getRoleName());
                roleMappings.remove(roleMapping.getName());
                if (roleMappings.isEmpty()) {
                    roleToRoleMappingsMap.remove(roleMapping.getRoleName());
                }

                // remove from `groupToRolesMap`
                if (roleMapping instanceof LDAPRoleMapping) {
                    LDAPRoleMapping ldapRoleMapping = (LDAPRoleMapping) roleMapping;
                    Map<String, List<String>> group2Roles = groupToRolesMap.get(roleMapping.getIntegrationName());
                    for (String group : ldapRoleMapping.getGroupSet()) {
                        List<String> roles = group2Roles.get(group);
                        roles.remove(roleMapping.getRoleName());
                        if (roles.isEmpty()) {
                            group2Roles.remove(group);
                        }
                    }
                    if (group2Roles.isEmpty()) {
                        groupToRolesMap.remove(roleMapping.getIntegrationName());
                    }
                }

                // final remove
                nameToRoleMappingMap.remove(name);

            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<String, Set<String>> getMappedGroupDNs() {
        Map<String, Set<String>> mappedGroupDNs = Maps.newHashMap();
        try {
            lock.readLock().lock();

            for (Map.Entry<String, Map<String, List<String>>> entry : groupToRolesMap.entrySet()) {
                mappedGroupDNs.put(entry.getKey(), Sets.newHashSet(entry.getValue().keySet()));
            }
        } finally {
            lock.readLock().unlock();
        }
        return mappedGroupDNs;
    }

    public Set<String> getMappedRoleNames(String integrationName, List<String> belongedGroups) {
        Set<String> roleNames = new HashSet<>();

        try {
            lock.readLock().lock();

            Map<String, List<String>> rolesMap = groupToRolesMap.get(integrationName);
            if (rolesMap != null) {
                for (String groupDn : belongedGroups) {
                    List<String> roles = rolesMap.get(groupDn);
                    if (roles != null) {
                        roleNames.addAll(roles);
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        return roleNames;
    }

    public void createRoleMapping(String name,
                                  Map<String, String> propertyMap,
                                  boolean isReplay) throws DdlException {
        RoleMapping roleMapping;
        AuthenticationMgr authenticationManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        SecurityIntegration secIntegration =
                authenticationManager.getSecurityIntegration(
                        propertyMap.get(RoleMapping.ROLE_MAPPING_PROPERTY_INTEGRATION_NAME_KEY));
        if (secIntegration == null) {
            throw new DdlException("specified security integration doesn't exist");
        }
        try {
            roleMapping =
                    RoleMappingFactory.createRoleMapping(name, propertyMap, secIntegration.getType());
        } catch (PrivilegeException e) {
            throw new DdlException("failed to create role mapping, error: " + e.getMessage(), e);
        }
        addRoleMapping(roleMapping, secIntegration.getType());
        if (!isReplay) {
            GlobalStateMgr.getCurrentState().getEditLog().logCreateRoleMapping(name, propertyMap);
            LOG.info("finished to create role mapping '{}'", roleMapping.toString());
        }
    }

    public void alterRoleMapping(String name,
                                 Map<String, String> alterProps, boolean isReplay)
            throws DdlException {
        String integrationName = alterProps.get(RoleMapping.ROLE_MAPPING_PROPERTY_INTEGRATION_NAME_KEY);
        SecurityIntegration securityIntegration = null;
        // TODO(yiming): resolve potential concurrency issue(dropping integration and create/alter role mapping)
        if (integrationName != null) {
            securityIntegration = GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                    .getSecurityIntegration(integrationName);
            if (securityIntegration == null) {
                throw new DdlException("specified security integration '" + integrationName + "' doesn't exist");
            }
        }
        try {
            modifyRoleMapping(name, alterProps,
                    securityIntegration == null ? null : securityIntegration.getType());
        } catch (PrivilegeException e) {
            throw new DdlException("failed to alter role mapping, error: " + e.getMessage(), e);
        }
        if (!isReplay) {
            GlobalStateMgr.getCurrentState().getEditLog().logAlterRoleMapping(name, alterProps);
            LOG.info("finished to alter role mapping '{}' with updated properties {}", name, alterProps);
        }
    }

    public void dropRoleMapping(String name, boolean isReplay) throws DdlException {
        removeRoleMapping(name);
        if (!isReplay) {
            GlobalStateMgr.getCurrentState().getEditLog().logDropRoleMapping(name);
            LOG.info("finished to drop role mapping '{}'", name);
        }
    }

    public void replayCreateRoleMapping(String name,
                                        Map<String, String> propertyMap) throws DdlException {
        createRoleMapping(name, propertyMap, true);
    }

    public void replayAlterRoleMapping(String name,
                                       Map<String, String> alterProps) throws DdlException {
        alterRoleMapping(name, alterProps, true);
    }

    public void replayDropRoleMapping(String name) throws DdlException {
        dropRoleMapping(name, true);
    }

    public Set<Long> getMappedRoleIdsForLdapUser(String integrationName, String username) {
        Set<Long> roleIds = new HashSet<>();
        Set<String> roleNames;

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<String> belongedGroups = globalStateMgr.getLdapGroupCacheMgr()
                .getBelongedGroupsByUsername(integrationName, username);
        if (belongedGroups == null) {
            return roleIds;
        }

        roleNames = getMappedRoleNames(integrationName, belongedGroups);
        for (String roleName : roleNames) {
            Long roleId = globalStateMgr.getAuthorizationMgr().getRoleIdByNameAllowNull(roleName);
            if (roleId != null) {
                roleIds.add(roleId);
            }
        }

        return roleIds;
    }
}
