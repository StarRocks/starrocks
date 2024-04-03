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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.epack.privilege.AuthenticationMgrEPack;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
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
    /**
     * For microsoft active directory service.
     */
    private static final String SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP = "group";
    private static final Set<String> SUPPORTED_LDAP_GROUP_TYPES = new HashSet<>(Arrays.asList(
            SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES,
            SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES,
            SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP,
            SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP));

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
        AuthenticationMgrEPack authenticationMgrEPack = (AuthenticationMgrEPack) authenticationManager;
        LDAPSecurityIntegration ldapSecurityIntegration =
                (LDAPSecurityIntegration) authenticationMgrEPack.getSecurityIntegration(integrationName);
        if (ldapSecurityIntegration == null) {
            return null;
        }

        if (member2Groups.containsKey(integrationName)) {
            return member2Groups.get(integrationName).getOrDefault(username, null);
        }

        return null;
    }

    @Override
    public void runAfterCatalogReady() {
        refreshGroupCache(false);
    }

    public synchronized void refreshGroupCache(boolean force) {
        AuthenticationMgrEPack authenticationMgrEPack = (AuthenticationMgrEPack) authenticationManager;
        Map<String, Set<String>> mappedGroupDNs = authorizationManager.getRoleMappingMetaMgr().getMappedGroupDNs();
        for (Map.Entry<String, Set<String>> entry : mappedGroupDNs.entrySet()) {
            SecurityIntegration securityIntegration = authenticationMgrEPack.getSecurityIntegration(entry.getKey());
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
                        result = getMemberToGroupsMap(entry.getValue(),
                                ldapSecurityIntegration.getLdapServerHost(),
                                ldapSecurityIntegration.getLdapServerPort(),
                                ldapSecurityIntegration.getLdapBindRootDn(),
                                ldapSecurityIntegration.getLdapBindRootPwd(),
                                ldapSecurityIntegration.getLdapUserGroupMatchAttr(),
                                ldapSecurityIntegration.getLdapGroupMatchUseMemberUid());
                        long end = System.currentTimeMillis();
                        member2Groups.put(ldapSecurityIntegration.getName(), result);
                        ldapSecurityIntegration.setLastRefreshTime(end);
                        LOG.info("refreshed {} groups with {} members for security integration '{}' in {}ms",
                                entry.getValue().size(), result.size(),
                                ldapSecurityIntegration.getName(), end - start);
                        LOG.info("refresh result for groups {} with security integration '{}': {}",
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

    private static Map<String, List<String>> getMemberToGroupsMap(Set<String> groupDNs,
                                                                  String ldapServerHost,
                                                                  String ldapServerPort,
                                                                  String ldapRootDn,
                                                                  String ldapRootPassword,
                                                                  String ldapGroupMatchAttr,
                                                                  boolean ldapGroupMatchUseMemberUid)
            throws NamingException {
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
                Set<String> memberNames;
                switch (groupType) {
                    case SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES:
                    case SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES: {
                        memberNames = getMemberNamesFromGroupOfNames(ctx, groupDN, ldapGroupMatchAttr);
                        break;
                    }
                    case SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP: {
                        memberNames = getMemberNamesFromPosixGroup(ctx, groupDN);
                        break;
                    }
                    case SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP:
                        memberNames = getMemberNamesFromADGroup(ctx, groupDN,
                                ldapGroupMatchAttr, ldapGroupMatchUseMemberUid);
                        break;
                    default:
                        LOG.warn("unsupported group objectClass for group '" +
                                groupDN + "' with class '" + groupType +
                                "', currently supported: " + SUPPORTED_LDAP_GROUP_TYPES);
                        continue;
                }

                updateGroupMembership(memberToGroups, memberNames, groupDN);
            }
        } finally {
            if (ctx != null) {
                ctx.close();
            }
        }
        return memberToGroups;
    }

    private static void updateGroupMembership(Map<String, List<String>> memberToGroups,
                                              Set<String> memberNames, String groupDN) {
        memberNames.forEach(memberName ->
                memberToGroups.computeIfAbsent(memberName, k -> new ArrayList<>()).add(groupDN));
    }

    private static String getGroupType(DirContext ctx, String groupDN) throws NamingException {
        Attributes attrs;
        try {
            attrs = ctx.getAttributes(groupDN);
        } catch (NameNotFoundException e) {
            // For non-existed group, ignore it
            return null;
        }
        if (attrs == null) {
            return null;
        }
        Attribute objectClass = attrs.get("objectClass");
        NamingEnumeration<?> e = objectClass.getAll();
        String objectClassName = null;
        while (e.hasMore()) {
            objectClassName = (String) e.next();
            if (SUPPORTED_LDAP_GROUP_TYPES.contains(objectClassName)) {
                return objectClassName;
            }
        }
        return objectClassName;
    }

    private static Set<String> getMemberNamesFromGroupOfNames(DirContext ctx,
                                                               String groupDN,
                                                               String ldapGroupMatchAttr) throws NamingException {
        Set<String> memberNames = new HashSet<>();
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
                memberNames.addAll(getMemberNamesFromGroupOfNames(ctx, memberDN, ldapGroupMatchAttr));
            } else {
                String name = retrieveMemberNameFromDn(memberDN, ldapGroupMatchAttr);
                if (!Strings.isNullOrEmpty(name)) {
                    memberNames.add(name);
                }
            }
        }
        return memberNames;
    }

    private static Set<String> getMemberNamesFromPosixGroup(DirContext ctx, String groupDN) throws NamingException {
        Set<String> memberNames = new HashSet<>();
        Attributes attrs = ctx.getAttributes(groupDN, new String[] {"memberUid"});
        Attribute memberUid = attrs.get("memberUid");
        NamingEnumeration<?> e = memberUid.getAll();
        while (e.hasMore()) {
            String memberUidValue = (String) e.next();
            memberNames.add(memberUidValue);
        }
        return memberNames;
    }

    private static Set<String> getMemberNamesFromADGroup(DirContext ctx,
                                                         String groupDN,
                                                         String ldapGroupMatchAttr,
                                                         boolean ldapGroupMatchUseMemberUid) throws NamingException {
        LOG.info("getting member names from AD group '{}'", groupDN);
        Set<String> memberNames = new HashSet<>();
        // check whether `memberUid` attribute is present or not.
        Attributes attrs = ctx.getAttributes(groupDN, new String[] {"memberUid", "member"});
        Attribute memberUid = attrs.get("memberUid");
        boolean memberRetrievedFromUid = false;
        if (ldapGroupMatchUseMemberUid && memberUid != null && memberUid.size() > 0) {
            // If present, we will use `memberUid` attribute to get the members of the group directly,
            // otherwise we will retrieve the members of the group using `member` attribute.
            memberRetrievedFromUid = true;
            NamingEnumeration<?> e = memberUid.getAll();
            while (e.hasMore()) {
                String memberUidValue = (String) e.next();
                LOG.info("get memberUid: '{}' from AD group '{}'", memberUidValue, groupDN);
                memberNames.add(memberUidValue);
            }
        }

        Attribute member = attrs.get("member");
        NamingEnumeration<?> e = member.getAll();
        while (e.hasMore()) {
            String memberDN = (String) e.next();
            // recursively get all the members of subgroup
            // `memberDN` may or may not be a supported group
            String groupType = getGroupType(ctx, memberDN);
            if (Objects.equals(groupType, SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP)) {
                LOG.info("found sub AD group '{}' from '{}'", memberDN, groupDN);
                memberNames.addAll(getMemberNamesFromADGroup(ctx, memberDN,
                        ldapGroupMatchAttr, ldapGroupMatchUseMemberUid));
            } else if (!memberRetrievedFromUid) {
                String name = retrieveMemberNameFromDn(memberDN, ldapGroupMatchAttr);
                if (!Strings.isNullOrEmpty(name)) {
                    memberNames.add(name);
                }
            }
        }

        return memberNames;
    }

    @VisibleForTesting
    public static String retrieveMemberNameFromDn(String memberDn, String ldapGroupMatchAttr) {
        boolean usingRegex = ldapGroupMatchAttr.startsWith("regex:");
        String[] splits = memberDn.split(",\\s*");
        if (usingRegex) {
            String regex = ldapGroupMatchAttr.substring(ldapGroupMatchAttr.indexOf(":") + 1);
            Pattern p = Pattern.compile(regex);
            for (String split : splits) {
                Matcher m = p.matcher(split);
                if (m.find()) {
                    if (m.groupCount() != 1) {
                        LOG.warn("invalid regex pattern: '{}', no matched group found", regex);
                        continue;
                    }
                    String matchedName = m.group(1);
                    LOG.info("found regex matched member name '{}' from member '{}'", matchedName, memberDn);
                    return matchedName;
                }
            }
        } else {
            for (String split : splits) {
                if (split.startsWith(ldapGroupMatchAttr + "=")) {
                    String matchedName;
                    try {
                        matchedName = split.substring(split.indexOf("=") + 1);
                    } catch (IndexOutOfBoundsException e) {
                        LOG.warn("invalid member name format: '{}', msg: {}", memberDn, e.getMessage());
                        continue;
                    }
                    LOG.info("found matched member name '{}' from member '{}'", matchedName, memberDn);
                    return matchedName;
                }
            }
        }

        return null;
    }
}
