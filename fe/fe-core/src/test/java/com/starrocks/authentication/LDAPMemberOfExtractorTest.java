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

import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

/**
 * Pure-function tests for turning a `memberOf` attribute into group names, plus the group source
 * enum. No LDAP server involved.
 */
class LDAPMemberOfExtractorTest {

    /**
     * JNDI hands back case-insensitive Attributes, so the fixtures have to be built the same way -
     * otherwise the tests would pass for a reason the real code cannot rely on.
     */
    private static Attributes attributesOf(String id, String... values) {
        BasicAttributes attributes = new BasicAttributes(true);
        BasicAttribute attribute = new BasicAttribute(id);
        for (String value : values) {
            attribute.add(value);
        }
        attributes.put(attribute);
        return attributes;
    }

    @Test
    void testGroupNameIsFirstRdnValue() {
        Assertions.assertEquals("SR Analysts",
                LDAPMemberOfExtractor.groupNameFromDn("CN=SR Analysts,OU=Groups,OU=Corp,DC=company,DC=com"));
        // The RDN type does not have to be CN: the first RDN is the entry's name in its parent.
        Assertions.assertEquals("Team A",
                LDAPMemberOfExtractor.groupNameFromDn("OU=Team A,DC=x,DC=com"));
    }

    @Test
    void testEscapedCommaInDnIsNotSplit() {
        // This is what a split(",") implementation gets wrong.
        Assertions.assertEquals("Doe, John",
                LDAPMemberOfExtractor.groupNameFromDn("CN=Doe\\, John,OU=Groups,DC=y,DC=com"));
    }

    @Test
    void testOriginalCaseIsKept() {
        // Group names are handed to Ranger, which matches case-sensitively, so they must never be
        // rewritten - not even to lower case.
        Assertions.assertEquals("StarRocks-Admin",
                LDAPMemberOfExtractor.groupNameFromDn("CN=StarRocks-Admin,OU=Groups,DC=x"));
    }

    @Test
    void testBlankAndUnparsableDnAreSkipped() {
        Assertions.assertNull(LDAPMemberOfExtractor.groupNameFromDn(null));
        Assertions.assertNull(LDAPMemberOfExtractor.groupNameFromDn(""));
        Assertions.assertNull(LDAPMemberOfExtractor.groupNameFromDn("   "));
        Assertions.assertNull(LDAPMemberOfExtractor.groupNameFromDn("not a dn at all"));
    }

    @Test
    void testAllValuesOfAMultiValuedAttributeAreUsed() {
        Attributes attributes = attributesOf("memberOf",
                "CN=SR Analysts,OU=Groups,DC=x",
                "CN=Data Platform,OU=Groups,DC=x",
                "OU=Team A,DC=x");
        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform", "Team A"),
                LDAPMemberOfExtractor.extractGroupNames(attributes, "memberOf", "alice"));
    }

    @Test
    void testAnUnparsableValueDoesNotDropTheOthers() {
        Attributes attributes = attributesOf("memberOf",
                "CN=SR Analysts,OU=Groups,DC=x",
                "",
                "CN=Data Platform,OU=Groups,DC=x");
        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"),
                LDAPMemberOfExtractor.extractGroupNames(attributes, "memberOf", "alice"));
    }

    @Test
    void testSameFirstRdnCollapsesToOneGroupName() {
        Attributes attributes = attributesOf("memberOf",
                "CN=Analysts,OU=Corp,DC=x",
                "CN=Analysts,OU=Partners,DC=x");
        Assertions.assertEquals(Set.of("Analysts"),
                LDAPMemberOfExtractor.extractGroupNames(attributes, "memberOf", "alice"));
    }

    @Test
    void testNoAttributeMeansNoGroupAndNoError() {
        Assertions.assertTrue(LDAPMemberOfExtractor.extractGroupNames(null, "memberOf", "bob").isEmpty());
        Assertions.assertTrue(LDAPMemberOfExtractor
                .extractGroupNames(new BasicAttributes(true), "memberOf", "bob").isEmpty());
        // An entry that carries other attributes but no memberOf.
        Assertions.assertTrue(LDAPMemberOfExtractor
                .extractGroupNames(attributesOf("cn", "bob"), "memberOf", "bob").isEmpty());
    }

    @Test
    void testAttributeIdIsMatchedIgnoringCase() {
        // The user may configure `memberof` while the directory answers with `memberOf`.
        Attributes attributes = attributesOf("memberOf", "CN=SR Analysts,OU=Groups,DC=x");
        Assertions.assertEquals(Set.of("SR Analysts"),
                LDAPMemberOfExtractor.extractGroupNames(attributes, "memberof", "alice"));
    }

    @Test
    void testCustomAttributeName() {
        Attributes attributes = attributesOf("isMemberOf", "CN=SR Analysts,OU=Groups,DC=x");
        Assertions.assertEquals(Set.of("SR Analysts"),
                LDAPMemberOfExtractor.extractGroupNames(attributes, "isMemberOf", "alice"));
        // ... and the default name must not pick it up.
        Assertions.assertTrue(LDAPMemberOfExtractor.extractGroupNames(attributes, "memberOf", "alice").isEmpty());
    }

    @Test
    void testRangedAttributeIsDetected() {
        Attributes ranged = attributesOf("memberOf;range=0-1499", "CN=SR Analysts,OU=Groups,DC=x");
        Assertions.assertTrue(LDAPMemberOfExtractor.hasRangedValues(ranged, "memberOf"));
        // The values already returned are still used - fewer grants beats zero grants.
        Assertions.assertEquals(Set.of("SR Analysts"),
                LDAPMemberOfExtractor.extractGroupNames(ranged, "memberOf", "alice"));

        // Case of the option and a custom attribute name.
        Attributes upperCase = attributesOf("isMemberOf;RANGE=0-1499", "CN=A,DC=x");
        Assertions.assertTrue(LDAPMemberOfExtractor.hasRangedValues(upperCase, "isMemberOf"));

        // Not ranged.
        Assertions.assertFalse(LDAPMemberOfExtractor
                .hasRangedValues(attributesOf("memberOf", "CN=A,DC=x"), "memberOf"));
    }

    @Test
    void testBaseAndRangedAttributeAreUnioned() {
        BasicAttributes attributes = new BasicAttributes(true);
        attributes.put(new BasicAttribute("memberOf", "CN=Base,OU=Groups,DC=x"));
        attributes.put(new BasicAttribute("memberOf;range=1500-2999", "CN=Ranged,OU=Groups,DC=x"));
        Assertions.assertTrue(LDAPMemberOfExtractor.hasRangedValues(attributes, "memberOf"));
        Assertions.assertEquals(Set.of("Base", "Ranged"),
                LDAPMemberOfExtractor.extractGroupNames(attributes, "memberOf", "alice"));
    }

    private static final String LDAP_PROP =
            SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE;
    private static final String TOKEN_PROP = JWTBaseSecurityIntegration.GROUP_SOURCE;

    @Test
    void testGroupSourceLegalValues() {
        Assertions.assertEquals(GroupSource.GROUP_PROVIDER, GroupSource.parseOrThrow("group_provider", LDAP_PROP));
        Assertions.assertEquals(GroupSource.BOTH, GroupSource.parseOrThrow("both", LDAP_PROP));
        // The third value is spelled after the mechanism of the property: `memberof` on the LDAP
        // property, `claim` on the token integrations' `group_source`. The neutral internal name
        // `authenticator` is accepted on both; the other type's spelling is not.
        Assertions.assertEquals(GroupSource.AUTHENTICATOR, GroupSource.parseOrThrow("memberof", LDAP_PROP));
        Assertions.assertEquals(GroupSource.AUTHENTICATOR, GroupSource.parseOrThrow("authenticator", LDAP_PROP));
        Assertions.assertEquals(GroupSource.AUTHENTICATOR, GroupSource.parseOrThrow("claim", TOKEN_PROP));
        Assertions.assertEquals(GroupSource.AUTHENTICATOR, GroupSource.parseOrThrow("authenticator", TOKEN_PROP));
        Assertions.assertThrows(SemanticException.class, () -> GroupSource.parseOrThrow("claim", LDAP_PROP));
        Assertions.assertThrows(SemanticException.class, () -> GroupSource.parseOrThrow("memberof", TOKEN_PROP));
        Assertions.assertEquals("memberof", GroupSource.AUTHENTICATOR.spelledFor(LDAP_PROP));
        Assertions.assertEquals("claim", GroupSource.AUTHENTICATOR.spelledFor(TOKEN_PROP));
        Assertions.assertEquals("both", GroupSource.BOTH.spelledFor(LDAP_PROP));
        // Case and surrounding blanks are tolerated.
        Assertions.assertEquals(GroupSource.AUTHENTICATOR, GroupSource.parseOrThrow(" MemberOf ", LDAP_PROP));
        Assertions.assertEquals(GroupSource.AUTHENTICATOR, GroupSource.parseOrThrow(" Claim ", TOKEN_PROP));

        Assertions.assertTrue(GroupSource.GROUP_PROVIDER.usesGroupProvider());
        Assertions.assertFalse(GroupSource.GROUP_PROVIDER.usesAuthenticatorGroups());
        Assertions.assertFalse(GroupSource.AUTHENTICATOR.usesGroupProvider());
        Assertions.assertTrue(GroupSource.AUTHENTICATOR.usesAuthenticatorGroups());
        Assertions.assertTrue(GroupSource.BOTH.usesGroupProvider());
        Assertions.assertTrue(GroupSource.BOTH.usesAuthenticatorGroups());
    }

    @Test
    void testGroupSourceIllegalValueIsRejectedOnTheDdlPath() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> GroupSource.parseOrThrow("memberof_only", LDAP_PROP));
        Assertions.assertTrue(e.getMessage().contains("memberof_only"));
        // The message names the property the user typed and lists the values legal *on it*.
        Assertions.assertTrue(e.getMessage().contains("authentication_ldap_simple_group_source"));
        Assertions.assertTrue(e.getMessage().contains("group_provider, memberof, both"), e.getMessage());
        String tokenMessage = Assertions.assertThrows(SemanticException.class,
                () -> GroupSource.parseOrThrow("memberof", TOKEN_PROP)).getMessage();
        Assertions.assertTrue(tokenMessage.contains("group_source"), tokenMessage);
        Assertions.assertTrue(tokenMessage.contains("group_provider, claim, both"), tokenMessage);
        Assertions.assertThrows(SemanticException.class, () -> GroupSource.parseOrThrow("", LDAP_PROP));
    }

    @Test
    void testGroupSourceIllegalValueFallsBackAtRuntime() {
        // The same value coming from fe.conf must not fail every LDAP login.
        Assertions.assertEquals(GroupSource.GROUP_PROVIDER, GroupSource.parseOrDefault("memberof_only", LDAP_PROP));
        Assertions.assertEquals(GroupSource.GROUP_PROVIDER, GroupSource.parseOrDefault(null, LDAP_PROP));
        Assertions.assertEquals(GroupSource.GROUP_PROVIDER, GroupSource.parseOrDefault("", LDAP_PROP));
        Assertions.assertEquals(GroupSource.BOTH, GroupSource.parseOrDefault("both", LDAP_PROP));
    }

    @Test
    void testLowerCaseSetHelper() {
        Assertions.assertEquals(Set.of("sr analysts", "data platform"),
                LDAPMemberOfExtractor.toLowerCaseSet(List.of("SR Analysts", "Data Platform")));
        Assertions.assertTrue(LDAPMemberOfExtractor.toLowerCaseSet(null).isEmpty());
    }
}
