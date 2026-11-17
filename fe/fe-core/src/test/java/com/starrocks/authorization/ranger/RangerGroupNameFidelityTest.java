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
package com.starrocks.authorization.ranger;

import com.google.common.collect.Lists;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The group names handed to Apache Ranger must be exactly the strings the directory returned.
 * <p>
 * This is the assertion that protects the case-insensitive relaxation from turning into a
 * regression. Matching a `GRANT ... TO EXTERNAL GROUP` or `permitted_groups` entry ignores case
 * (AuthorizationMgr#getRoleIdListByGroup and the login gate), but Ranger matches group names
 * case-sensitively, so a policy written against the directory's spelling breaks the moment anything
 * normalizes the set on the way out. Asserting `current_group()` would not catch that: it is a
 * different reader of the same field. This one sits on the actual handoff.
 */
public class RangerGroupNameFidelityTest {
    /** What the directory returned, mixed case on purpose. */
    private static final Set<String> DIRECTORY_GROUPS = Set.of("SR Analysts", "Data Platform");

    private static final List<Set<String>> HANDED_TO_RANGER = new java.util.ArrayList<>();

    @BeforeEach
    public void setUp() {
        HANDED_TO_RANGER.clear();

        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }

            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request,
                                                    RangerAccessResultProcessor resultProcessor) {
                return null;
            }
        };

        // The handoff itself: whatever the controller passes ends up here on its way into Ranger.
        new MockUp<RangerAccessRequestImpl>() {
            @Mock
            public void setUserGroups(Set<String> groups) {
                HANDED_TO_RANGER.add(groups == null ? null : new HashSet<>(groups));
            }
        };
    }

    @Test
    public void testGroupNamesReachRangerWithTheDirectorySpelling() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setGroups(new HashSet<>(DIRECTORY_GROUPS));

        List<Column> columns = Lists.newArrayList(new Column("v1", IntegerType.INT));
        new RangerStarRocksAccessController()
                .getColumnMaskingPolicy(context, new TableName("db", "tbl"), columns);

        // Ranger's own request initialization also passes through the same setter with a null set,
        // so only the calls that actually carry a group set are of interest here.
        List<Set<String>> withGroups = HANDED_TO_RANGER.stream()
                .filter(groups -> groups != null && !groups.isEmpty())
                .collect(Collectors.toList());

        Assertions.assertFalse(withGroups.isEmpty(),
                "the controller never handed a group set to Ranger - has the call path changed?");
        for (Set<String> groups : withGroups) {
            Assertions.assertEquals(DIRECTORY_GROUPS, groups,
                    "the group set must reach Ranger unchanged, in the directory's own case");
            Assertions.assertFalse(groups.contains("sr analysts"),
                    "a lower-cased group name would silently break every existing Ranger policy");
        }
    }
}
