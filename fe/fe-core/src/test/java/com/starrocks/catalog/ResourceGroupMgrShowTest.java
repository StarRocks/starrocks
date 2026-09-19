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

package com.starrocks.catalog;

import com.starrocks.thrift.TWorkGroupType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Zero-dependency unit tests for the lock-free show methods added to
 * {@link ResourceGroupMgr} by the CopyOnWrite volatile-snapshot refactor.
 *
 * <p>No {@code UtFrameUtils}, {@code GlobalStateMgr}, or {@code EditLog} is
 * required — {@link ResourceGroup#show} has no such dependencies.
 * Groups are injected directly into the volatile {@code snapshot}
 * field via reflection, avoiding {@code createResourceGroup}'s EditLog call.
 *
 * <p>This class intentionally has no {@code @BeforeEach} / {@code @AfterEach}
 * so it runs cleanly in every test environment.
 */
class ResourceGroupMgrShowTest {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Creates a minimal {@link ResourceGroup} that can call {@code show()} without NPE. */
    private ResourceGroup buildGroup(String name, long id) {
        ResourceGroup rg = new ResourceGroup();
        rg.setName(name);
        rg.setId(id);
        // memLimit is dereferenced as (Double * 100) in showClassifier — must be non-null.
        rg.setMemLimit(0.1);
        // resourceGroupType.name() is called in showClassifier — must be non-null.
        rg.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        rg.setClassifiers(Collections.emptyList());
        return rg;
    }

    /**
     * Injects (name → group) pairs directly into the volatile
     * {@code snapshot} field of the given {@link ResourceGroupMgr},
     * bypassing {@code createResourceGroup} and its EditLog dependency.
     *
     * <p>Both {@code byName} and {@code byId} are populated so that the full
     * snapshot is consistent; {@code byClassifier} is left empty.
     */
    private void injectMap(ResourceGroupMgr mgr, Object... namesAndGroups) {
        Map<String, ResourceGroup> byName = new LinkedHashMap<>();
        Map<Long, ResourceGroup>   byId   = new HashMap<>();
        for (int i = 0; i < namesAndGroups.length; i += 2) {
            ResourceGroup rg = (ResourceGroup) namesAndGroups[i + 1];
            byName.put((String) namesAndGroups[i], rg);
            byId.put(rg.getId(), rg);
        }
        ResourceGroupMgr.ResourceGroupSnapshot snap = ResourceGroupMgr.newSnapshotForTest(
                byName, byId, Collections.emptyMap(), null);
        mgr.setSnapshotForTest(snap);
    }

    // -------------------------------------------------------------------------
    // showAllResourceGroups — isListAll=true branch (lines 257-263)
    // -------------------------------------------------------------------------

    /**
     * Verifies the {@code isListAll=true} branch of
     * {@link ResourceGroupMgr#showAllResourceGroups}: the volatile snapshot is
     * read lock-free and every group's rows appear in the result.
     */
    @Test
    void testShowAllResourceGroupsListAll() {
        ResourceGroupMgr mgr = new ResourceGroupMgr();
        ResourceGroup rgA = buildGroup("rg_show_a", 1001L);
        ResourceGroup rgB = buildGroup("rg_show_b", 1002L);
        injectMap(mgr, "rg_show_a", rgA, "rg_show_b", rgB);

        // isListAll=true → the if-branch at line 258 executes (lines 257-263).
        List<List<String>> rows = mgr.showAllResourceGroups(null, false, true);

        assertThat(rows).isNotEmpty();
        assertThat(rows.stream().anyMatch(r -> r.contains("rg_show_a"))).isTrue();
        assertThat(rows.stream().anyMatch(r -> r.contains("rg_show_b"))).isTrue();
    }

    // -------------------------------------------------------------------------
    // showAllResourceGroups — isListAll=false branch (lines 264-271)
    // -------------------------------------------------------------------------

    /**
     * Verifies the {@code isListAll=false} branch of
     * {@link ResourceGroupMgr#showAllResourceGroups}: when
     * {@code ConnectContext.get()} is non-null and {@code isListAll=false},
     * the per-user visibility path executes (lines 265-271).
     */
    @Test
    void testShowAllResourceGroupsPerUserBranch() {
        ResourceGroupMgr mgr = new ResourceGroupMgr();
        ResourceGroup rg = buildGroup("rg_show_user", 2001L);
        injectMap(mgr, "rg_show_user", rg);

        // Set a ConnectContext so ConnectContext.get() != null, forcing the else-branch.
        com.starrocks.qe.ConnectContext ctx = new com.starrocks.qe.ConnectContext();
        ctx.setRemoteIP("127.0.0.1");
        // Prevents NPE in getUnqualifiedUser() which calls qualifiedUser.split(":")
        ctx.setQualifiedUser("test_user");
        com.starrocks.qe.ConnectContext.set(ctx);
        try {
            // isListAll=false + get()!=null → else-branch (lines 264-271).
            List<List<String>> rows = mgr.showAllResourceGroups(ctx, false, false);
            // Result may be empty (group not visible to test_user) but must not throw.
            assertThat(rows).isNotNull();
        } finally {
            com.starrocks.qe.ConnectContext.set(null);
        }
    }

    // -------------------------------------------------------------------------
    // showOneResourceGroup — found and not-found branches (lines 276-282)
    // -------------------------------------------------------------------------

    /**
     * Verifies both branches of {@link ResourceGroupMgr#showOneResourceGroup}:
     * the found path (lines 277-278, 281) returns non-empty rows, and
     * the not-found path (lines 278-279) returns an empty list.
     */
    @Test
    void testShowOneResourceGroupFoundAndNotFound() {
        ResourceGroupMgr mgr = new ResourceGroupMgr();
        ResourceGroup rg = buildGroup("rg_show_one", 3001L);
        injectMap(mgr, "rg_show_one", rg);

        // Found branch (lines 277-278, 281): containsKey → true.
        List<List<String>> found = mgr.showOneResourceGroup("rg_show_one", false);
        assertThat(found).isNotEmpty();
        assertThat(found.stream().anyMatch(r -> r.contains("rg_show_one"))).isTrue();

        // Not-found branch (line 279): containsKey → false → emptyList.
        List<List<String>> notFound = mgr.showOneResourceGroup("rg_does_not_exist", false);
        assertThat(notFound).isEmpty();
    }
}
