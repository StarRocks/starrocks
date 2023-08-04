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


package com.starrocks.authentication;

import com.google.common.collect.Maps;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.privilege.AuthorizationMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;

/**
 * Periodically refresh membership for all mapped groups defined by role mappings in StarRocks.
 */
public class LDAPGroupCacheMgr extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(LDAPGroupCacheMgr.class);
    private static final long DEFAULT_RUN_INTERVAL_MS = 10000;
    private static final String SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES = "groupOfNames";
    private static final String SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES = "groupOfUniqueNames";
    private static final String SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP = "posixGroup";
    private static final Set<String> SUPPORTED_LDAP_GROUP_TYPES = new HashSet<>(Arrays.asList(
            SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES,
            SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES,
            SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP));

    /**
     * store the group dn list for each member to which it belongs,
     * classified by security integration.
     * Use concurrent map and COW to avoid lock.
     */
    private final Map<String, Map<String, List<String>>> member2Groups;
    private AuthenticationMgr authenticationManager;
    private AuthorizationMgr authorizationManager;

    public LDAPGroupCacheMgr(AuthenticationMgr authenticationManager,
                             AuthorizationMgr authorizationManager) {
        super("LDAPGroupCacheRefresher", DEFAULT_RUN_INTERVAL_MS);
        member2Groups = Maps.newConcurrentMap();
        this.authenticationManager = authenticationManager;
        this.authorizationManager = authorizationManager;
    }

    public void setAuthzManager(AuthenticationMgr authenticationManager,
                                AuthorizationMgr authorizationManager) {
        this.authenticationManager = authenticationManager;
        this.authorizationManager = authorizationManager;
    }

    public List<String> getBelongedGroupsByUsername(String integrationName, String username) {
        LDAPSecurityIntegration ldapSecurityIntegration =
                (LDAPSecurityIntegration) authenticationManager.getSecurityIntegration(integrationName);
        if (ldapSecurityIntegration == null) {
            return null;
        }

        if (member2Groups.containsKey(integrationName)) {
            return member2Groups.get(integrationName)
                    .getOrDefault(ldapSecurityIntegration.getLdapUserGroupMatchAttr() + "=" + username,
                            null);
        }

        return null;
    }

    @Override
    public void runAfterCatalogReady() {
        refreshGroupCache(false);
    }

    public synchronized void refreshGroupCache(boolean force) {
        Map<String, Set<String>> mappedGroupDNs = authorizationManager.getRoleMappingMetaMgr().getMappedGroupDNs();
        for (Map.Entry<String, Set<String>> entry : mappedGroupDNs.entrySet()) {
            SecurityIntegration securityIntegration = authenticationManager.getSecurityIntegration(entry.getKey());
            if (securityIntegration == null) {
                member2Groups.remove(entry.getKey());
                continue;
            }
            if (securityIntegration.getType().equals(SecurityIntegration.SECURITY_INTEGRATION_TYPE_LDAP)) {
                LDAPSecurityIntegration ldapSecurityIntegration = (LDAPSecurityIntegration) securityIntegration;
                long refreshInterval = ldapSecurityIntegration.getLdapCacheRefreshInterval();
                long start = System.currentTimeMillis();
                if (force || start > ldapSecurityIntegration.getLastRefreshTime() + refreshInterval * 1000L) {
                    Map<String, List<String>> result;
                    try {
                        result = getMembersToGroupsMap(entry.getValue(),
                                ldapSecurityIntegration.getLdapServerHost(),
                                ldapSecurityIntegration.getLdapServerPort(),
                                ldapSecurityIntegration.getLdapBindRootDn(),
                                ldapSecurityIntegration.getLdapBindRootPwd(),
                                ldapSecurityIntegration.getLdapUserGroupMatchAttr());
                        long end = System.currentTimeMillis();
                        member2Groups.put(ldapSecurityIntegration.getName(), result);
                        ldapSecurityIntegration.setLastRefreshTime(end);
                        LOG.info("refreshed {} groups with {} members for security integration '{}' in {}ms",
                                entry.getValue().size(), result.size(),
                                ldapSecurityIntegration.getName(), end - start);
                        LOG.debug("refresh result for groups {} with security integration '{}': {}",
                                entry.getValue(), ldapSecurityIntegration.getName(), result);
                    } catch (Exception e) {
                        LOG.info("refresh group cache failed for groups {} with security integration '{}'," +
                                        " last refresh time: {}, error: {}",
                                entry.getValue(), ldapSecurityIntegration.getName(),
                                ldapSecurityIntegration.getLastRefreshTime(), e.getMessage(), e);
                    }
                }
            }
        }
    }

    private static Map<String, List<String>> getMembersToGroupsMap(Set<String> groupDNs,
                                                                  String ldapServerHost,
                                                                  String ldapServerPort,
                                                                  String ldapRootDn,
                                                                  String ldapRootPassword,
                                                                  String ldapGroupMatchAttr) throws NamingException {
        Map<String, List<String>> memberToGroups = new HashMap<>();

        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://" + ldapServerHost + ":" + ldapServerPort);
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, ldapRootDn);
        env.put(Context.SECURITY_CREDENTIALS, ldapRootPassword);

        DirContext ctx = null;
        try {
            ctx = new InitialDirContext(env);
            SearchControls controls = new SearchControls();
            controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            for (String groupDN : groupDNs) {
                String groupType = getGroupType(ctx, groupDN);
                if (groupType == null) {
                    continue;
                }
                if (Objects.equals(groupType, SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES) ||
                        Objects.equals(groupType, SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES)) {
                    List<String> memberDNs = getMemberDNsFromGroupOfNames(ctx, groupDN);
                    updateGroupMemberShip(memberToGroups, memberDNs, ldapGroupMatchAttr, groupDN);
                } else if (Objects.equals(groupType, SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP)) {
                    List<String> memberDNs = getMemberDNsFromPosixGroup(ctx, groupDN);
                    updateGroupMemberShip(memberToGroups, memberDNs, ldapGroupMatchAttr, groupDN);
                } else {
                    throw new NamingException("unsupported group objectClass for group '" +
                            groupDN + "' with class '" + groupType +
                            "', currently supported: " + SUPPORTED_LDAP_GROUP_TYPES);
                }
            }
        } finally {
            if (ctx != null) {
                ctx.close();
            }
        }
        return memberToGroups;
    }

    private static void updateGroupMemberShip(Map<String, List<String>> membersToGroups,
                                              List<String> memberDNs, String ldapGroupMatchAttr, String groupDN) {
        memberDNs.forEach(memberDN -> Arrays.stream(memberDN.split(",\\s*"))
                .filter(part -> part.startsWith(ldapGroupMatchAttr + "="))
                .findFirst()
                .ifPresent(matchedAttr ->
                        membersToGroups.computeIfAbsent(matchedAttr, k -> new ArrayList<>()).add(groupDN)));
    }

    private static String getGroupType(DirContext ctx, String groupDN) throws NamingException {
        Attributes attrs = null;
        try {
            attrs = ctx.getAttributes(groupDN);
        } catch (NamingException e) {
            // For non-existed group, ignore it
            if (!e.getMessage().toLowerCase().contains("no such object")) {
                throw e;
            }
        }
        if (attrs == null) {
            return null;
        }
        Attribute objectClass = attrs.get("objectClass");
        NamingEnumeration<?> e = objectClass.getAll();
        String objectClassName = null;
        while (e.hasMore()) {
            objectClassName = (String) e.next();
            if (Objects.equals(objectClassName, SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES) ||
                    objectClassName.equals(SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES) ||
                    objectClassName.equals(SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP)) {
                return objectClassName;
            }
        }
        return objectClassName;
    }

    private static List<String> getMemberDNsFromGroupOfNames(DirContext ctx, String groupDN) throws NamingException {
        List<String> memberDNs = new ArrayList<>();
        Attributes attrs = ctx.getAttributes(groupDN, new String[] {"member"});
        Attribute member = attrs.get("member");
        NamingEnumeration<?> e = member.getAll();
        while (e.hasMore()) {
            String memberDN = (String) e.next();
            // recursively get all the members of subgroup
            // `memberDN` may or may not be a supported group
            String groupType = getGroupType(ctx, memberDN);
            if (Objects.equals(groupType, SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES) ||
                    Objects.equals(groupType, SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES)) {
                memberDNs.addAll(getMemberDNsFromGroupOfNames(ctx, memberDN));
            } else {
                memberDNs.add(memberDN);
            }
        }
        return memberDNs;
    }

    private static List<String> getMemberDNsFromPosixGroup(DirContext ctx, String groupDN) throws NamingException {
        List<String> memberDNs = new ArrayList<>();
        Attributes attrs = ctx.getAttributes(groupDN, new String[] {"memberUid"});
        Attribute memberUid = attrs.get("memberUid");
        NamingEnumeration<?> e = memberUid.getAll();
        while (e.hasMore()) {
            String memberUidValue = (String) e.next();
            String memberDN = "uid=" + memberUidValue;
            memberDNs.add(memberDN);
        }
        return memberDNs;
    }
}
