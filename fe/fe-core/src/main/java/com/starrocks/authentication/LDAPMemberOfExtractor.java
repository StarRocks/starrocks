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

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

/**
 * Turns the `memberOf` attribute of a user entry into a set of group names.
 * <p>
 * Everything here is a pure function of the {@link Attributes} handed back by the directory, so it
 * can be unit tested without an LDAP server.
 */
public class LDAPMemberOfExtractor {
    private static final Logger LOG = LogManager.getLogger(LDAPMemberOfExtractor.class);

    private LDAPMemberOfExtractor() {
    }

    /**
     * Extract the group names carried by `memberOfAttr` of a user entry.
     * <p>
     * Failure of a single value never fails the whole extraction: an unparsable DN is skipped and the
     * remaining values still take effect, because a group-resolution problem must not turn into a
     * login failure.
     *
     * @param attributes   attributes read back from the directory, may be null
     * @param memberOfAttr name of the attribute that carries the group membership
     * @param user         login name, only used for logging
     * @return group names, never null. Empty when the user belongs to no group (an attribute with no
     * value simply does not exist in LDAP), or when the attribute could not be read.
     */
    public static Set<String> extractGroupNames(Attributes attributes, String memberOfAttr, String user) {
        Set<String> groups = new LinkedHashSet<>();
        if (attributes == null || Strings.isNullOrEmpty(memberOfAttr)) {
            return groups;
        }

        Set<String> matchedIds = matchingAttributeIds(attributes, memberOfAttr);
        for (String id : matchedIds) {
            Attribute attribute = attributes.get(id);
            if (attribute == null) {
                continue;
            }
            try {
                NamingEnumeration<?> values = attribute.getAll();
                while (values.hasMore()) {
                    Object value = values.next();
                    String groupName = groupNameFromDn(value == null ? null : value.toString());
                    if (groupName != null) {
                        groups.add(groupName);
                    }
                }
            } catch (Exception e) {
                LOG.warn("failed to read values of attribute {} for user {}", id, user, e);
            }
        }

        // Detect - but do not page through - a directory that returned the values in ranges.
        // Whatever we already got is still used: fewer grants is bad, zero grants is worse, and
        // "the user really has no group" would become indistinguishable from "the values were truncated".
        // Same detection as hasRangedValues(), reached through the one helper both share, so a fix
        // here cannot diverge from the one the tests exercise.
        String rangedId = findRangedAttributeId(matchedIds, memberOfAttr);
        if (rangedId != null) {
            LOG.warn("ldap returned attribute '{}' in ranges for user '{}': group information is incomplete, " +
                            "the user may be granted fewer roles than expected. Resolved groups so far: {}",
                    rangedId, user, groups);
        }

        return groups;
    }

    /**
     * @return true if any of the attribute IDs is a ranged form of `memberOfAttr`, e.g.
     * `memberOf;range=0-1499` (Active Directory truncates large attributes this way).
     */
    static boolean hasRangedValues(Attributes attributes, String memberOfAttr) {
        if (attributes == null || Strings.isNullOrEmpty(memberOfAttr)) {
            return false;
        }
        return findRangedAttributeId(matchingAttributeIds(attributes, memberOfAttr), memberOfAttr) != null;
    }

    /**
     * All attribute IDs of the entry that carry `memberOfAttr`, matched ignoring case, including the
     * ones with attribute options such as `memberOf;range=0-1499`.
     */
    private static Set<String> matchingAttributeIds(Attributes attributes, String memberOfAttr) {
        Set<String> ids = new LinkedHashSet<>();
        String wanted = memberOfAttr.trim().toLowerCase(Locale.ROOT);
        NamingEnumeration<String> allIds = attributes.getIDs();
        try {
            while (allIds.hasMore()) {
                String id = allIds.next();
                if (id == null) {
                    continue;
                }
                String lower = id.toLowerCase(Locale.ROOT);
                if (lower.equals(wanted) || lower.startsWith(wanted + ";")) {
                    ids.add(id);
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to enumerate ldap attribute ids", e);
        }
        return ids;
    }

    private static String findRangedAttributeId(Set<String> matchedIds, String memberOfAttr) {
        String wanted = memberOfAttr.trim().toLowerCase(Locale.ROOT);
        for (String id : matchedIds) {
            String lower = id.toLowerCase(Locale.ROOT);
            if (lower.startsWith(wanted + ";") && lower.contains("range=")) {
                return id;
            }
        }
        return null;
    }

    /**
     * Take the value of the first RDN of a group DN as the group name, e.g.
     * `CN=SR Analysts,OU=Groups,DC=company,DC=com` -> `SR Analysts`.
     * <p>
     * Parsed with {@link LdapName} rather than split(",") on purpose: a DN value may contain an
     * escaped comma (`CN=Doe\, John,OU=...`), which string splitting would cut in the wrong place.
     * The case of the name is kept exactly as the directory returned it - the string is handed to
     * Ranger as is, and Ranger matches group names case-sensitively.
     *
     * @return the group name, or null if the value is blank or not a parsable DN
     */
    public static String groupNameFromDn(String dn) {
        if (Strings.isNullOrEmpty(dn) || dn.isBlank()) {
            return null;
        }
        try {
            LdapName name = new LdapName(dn.trim());
            if (name.isEmpty()) {
                LOG.debug("skip empty group dn from memberOf: '{}'", dn);
                return null;
            }
            // Index 0 of an LdapName is the least significant component (`DC=com`), so the entry's own
            // name - the first RDN of the textual DN - is the last element.
            Rdn rdn = name.getRdn(name.size() - 1);
            Object value = rdn.getValue();
            if (value == null) {
                return null;
            }
            String groupName = value.toString();
            return groupName.isBlank() ? null : groupName;
        } catch (Exception e) {
            LOG.debug("skip unparsable group dn from memberOf: '{}', reason: {}", dn, e.getMessage());
            return null;
        }
    }

    /**
     * @return a new set holding the lower-cased form of every element, for building a lookup index.
     */
    public static Set<String> toLowerCaseSet(Iterable<String> values) {
        Set<String> lowered = new HashSet<>();
        if (values == null) {
            return lowered;
        }
        for (String value : values) {
            if (value != null) {
                lowered.add(value.toLowerCase(Locale.ROOT));
            }
        }
        return lowered;
    }
}
