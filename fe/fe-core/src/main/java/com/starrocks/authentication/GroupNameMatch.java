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

import com.starrocks.common.Config;

/**
 * How a group name from a login is compared with a group name an administrator wrote - in
 * {@code GRANT ... TO EXTERNAL GROUP} and in {@code permitted_groups}.
 *
 * <p>One predicate, so the two places cannot drift apart. Whether case counts is a cluster-wide
 * choice ({@link Config#authentication_group_case_insensitive}), because it depends on the identity
 * system behind the groups and not on any one integration: an LDAP {@code cn} is case-insensitive
 * in the directory, while an OIDC provider may well hold two groups that differ only in case.
 *
 * <p>Whatever the setting, the names themselves are never rewritten. Stored keys and the group set
 * of a session keep the spelling they arrived with, because Apache Ranger receives those strings and
 * matches them exactly.
 */
public final class GroupNameMatch {
    private GroupNameMatch() {
    }

    public static boolean equal(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return Config.authentication_group_case_insensitive ? a.equalsIgnoreCase(b) : a.equals(b);
    }
}
