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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Reads the user's groups out of a claim of a <b>verified</b> JWT.
 *
 * <p>A claim set is a parsed JSON object - {@link JWTClaimsSet#getClaims()} hands back a
 * {@code Map<String, Object>} where nested objects are nested maps and arrays are lists - so a
 * configured path like {@code realm_access.roles} is just that map walked segment by segment. The
 * top-level shorthands Nimbus offers ({@code getStringListClaim}) are deliberately not used: they
 * only reach top-level claims, and having two code paths for "top level" and "nested" is how the
 * two drift apart.
 *
 * <p>Every failure to read the claim resolves to <b>no groups</b> rather than an exception. The
 * reason is not leniency for its own sake: Microsoft Entra ID drops the whole groups claim once a
 * user is in more than 200 groups and replaces it with an overage pointer, so "the claim is not
 * there" is a state a correctly configured IdP produces, and turning it into a failed login would
 * report "too many groups" as "authentication is broken".
 */
public class ClaimGroupExtractor {
    private static final Logger LOG = LogManager.getLogger(ClaimGroupExtractor.class);

    // An absent claim is a state a correct configuration produces (Entra ID drops it above 200
    // groups), and HTTP and the load RPCs authenticate on every request, so one such user would
    // otherwise write a WARN line per query. Said at WARN once a minute per integration and claim,
    // at DEBUG in between.
    private static final long ABSENT_CLAIM_WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final ConcurrentHashMap<String, Long> ABSENT_CLAIM_LAST_WARNED = new ConcurrentHashMap<>();

    private ClaimGroupExtractor() {
    }

    /** Whether the absent-claim line for this integration and claim goes out at WARN now. */
    static boolean warnAbsentClaimNow(String key) {
        long now = System.nanoTime();
        Long last = ABSENT_CLAIM_LAST_WARNED.get(key);
        if (last != null && now - last < ABSENT_CLAIM_WARN_INTERVAL_NANOS) {
            return false;
        }
        ABSENT_CLAIM_LAST_WARNED.put(key, now);
        return true;
    }

    /**
     * @param claims the claims of a token whose signature has already been verified
     * @param path   claim path, dot-separated for nested claims: {@code groups},
     *               {@code realm_access.roles}, {@code resource_access.<client>.roles}
     * @return the group names as the token spelled them, with surrounding whitespace removed and
     * blank names dropped. Nothing else is rewritten - not the case, not a leading slash - because
     * the names are handed to Ranger, which matches them exactly. Never null; empty when the claim
     * is absent or does not hold strings.
     */
    public static Set<String> extract(JWTClaimsSet claims, String path, String integrationName) {
        if (claims == null || path == null || path.isEmpty()) {
            return Set.of();
        }

        String[] segments = path.split("\\.", -1);
        Object current = claims.getClaims();
        for (int i = 0; i < segments.length; i++) {
            if (!(current instanceof Map)) {
                // Naming the segment matters: the difference between a typo in the middle of the
                // path and a claim the IdP simply did not send is invisible in the result.
                LOG.warn("group claim '{}' of security integration {} stops being an object at segment {} ('{}'); " +
                                "resolved no group", path, integrationName, i + 1, segments[i]);
                return Set.of();
            }
            current = ((Map<?, ?>) current).get(segments[i]);
        }

        if (current == null) {
            String message = "group claim '{}' of security integration {} is not present in the token; resolved " +
                    "no group. Does the identity provider emit it? Keycloak needs a group-membership mapper, and " +
                    "Entra ID drops the claim for users in more than 200 groups";
            if (warnAbsentClaimNow(integrationName + ":" + path)) {
                LOG.warn(message, path, integrationName);
            } else {
                LOG.debug(message, path, integrationName);
            }
            return Set.of();
        }

        // A list is the normal shape; a single value arrives as a bare string from some providers.
        Set<String> groups = new LinkedHashSet<>();
        if (current instanceof List) {
            for (Object element : (List<?>) current) {
                if (element instanceof String) {
                    addTrimmed(groups, (String) element);
                } else {
                    LOG.warn("group claim '{}' of security integration {} holds a non-string element ({}); " +
                                    "skipping it", path, integrationName,
                            element == null ? "null" : element.getClass().getSimpleName());
                }
            }
        } else if (current instanceof String) {
            addTrimmed(groups, (String) current);
        } else {
            LOG.warn("group claim '{}' of security integration {} is a {}, expected an array of strings or a " +
                            "string; resolved no group", path, integrationName,
                    current.getClass().getSimpleName());
        }
        return groups;
    }

    /**
     * Surrounding whitespace can never be part of a name a GRANT or a Ranger policy refers to, so
     * it is dropped, as it is for the comma-separated properties an administrator types by hand.
     */
    private static void addTrimmed(Set<String> groups, String name) {
        String trimmed = name.trim();
        if (!trimmed.isEmpty()) {
            groups.add(trimmed);
        }
    }
}
