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

import com.nimbusds.jwt.JWTClaimsSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * Pure-function tests for reading the groups out of a claim. No token, no signature, no identity
 * provider: the fixtures are JSON claim sets, parsed the same way a verified token's payload is, so
 * a nested claim here has exactly the shape a real one has.
 */
class ClaimGroupExtractorTest {

    private static final String SI = "jwt_si";

    private static Set<String> extract(String claimsJson, String path) throws Exception {
        return ClaimGroupExtractor.extract(JWTClaimsSet.parse(claimsJson), path, SI);
    }

    @Test
    void testTopLevelArray() throws Exception {
        Assertions.assertEquals(Set.of("a", "b"), extract("{\"groups\":[\"a\",\"b\"]}", "groups"));
    }

    /** Keycloak's realm roles - one level down, which is why the path is dot-separated at all. */
    @Test
    void testOneLevelOfNesting() throws Exception {
        Assertions.assertEquals(Set.of("a"),
                extract("{\"realm_access\":{\"roles\":[\"a\"]}}", "realm_access.roles"));
    }

    /** Keycloak's client roles - two levels, with the client id as the middle segment. */
    @Test
    void testTwoLevelsOfNesting() throws Exception {
        Assertions.assertEquals(Set.of("a"),
                extract("{\"resource_access\":{\"cli\":{\"roles\":[\"a\"]}}}", "resource_access.cli.roles"));
    }

    /**
     * Some providers emit a single group as a bare string rather than a one-element array. Accepting
     * it costs one branch and saves an administrator a failure with no visible cause.
     */
    @Test
    void testScalarClaimIsAcceptedAsOneGroup() throws Exception {
        Assertions.assertEquals(Set.of("a"), extract("{\"groups\":\"a\"}", "groups"));
    }

    /**
     * A missing claim is not an error: Entra ID drops the whole claim for a user in more than 200
     * groups, so this is a state a correctly configured provider produces.
     */
    @Test
    void testAbsentClaimResolvesNoGroup() throws Exception {
        Assertions.assertEquals(Set.of(), extract("{}", "groups"));
    }

    /** A path that runs into a scalar half way down - the typical typo in a nested path. */
    @Test
    void testPathBlockedByAScalarResolvesNoGroup() throws Exception {
        Assertions.assertEquals(Set.of(), extract("{\"realm_access\":\"x\"}", "realm_access.roles"));
    }

    /** The same path when the first segment is absent altogether. */
    @Test
    void testMissingIntermediateSegmentResolvesNoGroup() throws Exception {
        Assertions.assertEquals(Set.of(), extract("{}", "realm_access.roles"));
    }

    /**
     * A mixed array yields the strings and skips the rest. It must not throw: one odd element in a
     * token would otherwise take down every login of that user.
     */
    @Test
    void testNonStringElementsAreSkippedWithoutFailing() throws Exception {
        Assertions.assertEquals(Set.of("a"), extract("{\"groups\":[\"a\",1,null]}", "groups"));
    }

    /**
     * Group names go to Ranger, which matches case-sensitively, so nothing may be rewritten - not
     * the leading slash of a Keycloak full group path, not the case.
     */
    @Test
    void testGroupNamesAreKeptVerbatim() throws Exception {
        Set<String> groups = extract("{\"groups\":[\"/tenant_a\",\"Tenant_A\"]}", "groups");
        Assertions.assertEquals(Set.of("/tenant_a", "Tenant_A"), groups);
    }

    /**
     * The one exception to verbatim: surrounding whitespace, which no GRANT or Ranger policy can
     * refer to. A name that is nothing but whitespace is dropped rather than kept as an empty group.
     */
    @Test
    void testSurroundingWhitespaceIsTrimmedAndBlankNamesDropped() throws Exception {
        Assertions.assertEquals(Set.of("a", "B c"),
                extract("{\"groups\":[\" a \",\"B c\",\"   \",\"\"]}", "groups"));
        Assertions.assertEquals(Set.of("a"), extract("{\"groups\":\" a \"}", "groups"));
        Assertions.assertEquals(Set.of(), extract("{\"groups\":\"  \"}", "groups"));
    }

    /** An object where an array was expected: no groups, and no exception. */
    @Test
    void testObjectClaimResolvesNoGroup() throws Exception {
        Assertions.assertEquals(Set.of(), extract("{\"groups\":{\"a\":1}}", "groups"));
    }

    /**
     * The guards on the call itself. `group_claim` is mandatory in DDL when the source reads it, but
     * the value can still arrive null through a replayed edit log of an older version.
     */
    @Test
    void testNullInputsResolveNoGroup() throws Exception {
        JWTClaimsSet claims = JWTClaimsSet.parse("{\"groups\":[\"a\"]}");
        Assertions.assertEquals(Set.of(), ClaimGroupExtractor.extract(claims, null, SI));
        Assertions.assertEquals(Set.of(), ClaimGroupExtractor.extract(claims, "", SI));
        Assertions.assertEquals(Set.of(), ClaimGroupExtractor.extract(null, "groups", SI));
    }
}
