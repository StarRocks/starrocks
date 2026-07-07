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

package com.starrocks.context;

import com.google.common.collect.ImmutableMap;
import com.starrocks.persist.ContextOpLog;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins two related ContextMgr behaviors:
 *
 * <ul>
 *   <li><b>Cascade on DROP COLLECTION.</b> {@code dropCollection} must remove every workspace
 *       whose qualified name lives under the collection prefix. The pre-fix implementation
 *       only deleted the collection meta, leaving workspaces as orphans that would survive
 *       the next image dump+load cycle.</li>
 *   <li><b>O(1) by-id lookup.</b> {@code getContextBaseById} / {@code getCollectionById} were
 *       linear scans over the by-name map. With a parallel by-id index they're constant-time,
 *       which removes the O(M·N) hot path in the REST list endpoints.</li>
 * </ul>
 */
public class ContextMgrCascadeAndIndexTest {

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void dropCollectionRemovesChildWorkspaces() {
        ContextMgr mgr = new ContextMgr();
        // Use the replay path so the test doesn't depend on the global id generator.
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(100L, "cb", ImmutableMap.of()));
        mgr.replayCreateCollection(ContextOpLog.forCollection(200L, 100L, "cb.col", "knowledge",
                ImmutableMap.of()));
        // Two workspaces under cb.col, plus a control workspace under cb.other_col that must
        // survive the drop.
        mgr.replayCreateWorkspace(ContextOpLog.forWorkspace(301L, 200L, "cb.col.alpha",
                ImmutableMap.of()));
        mgr.replayCreateWorkspace(ContextOpLog.forWorkspace(302L, 200L, "cb.col.beta",
                ImmutableMap.of()));
        mgr.replayCreateCollection(ContextOpLog.forCollection(210L, 100L, "cb.other_col",
                "knowledge", ImmutableMap.of()));
        mgr.replayCreateWorkspace(ContextOpLog.forWorkspace(303L, 210L, "cb.other_col.gamma",
                ImmutableMap.of()));

        Assertions.assertNotNull(mgr.getWorkspace("cb.col.alpha"));
        Assertions.assertNotNull(mgr.getWorkspace("cb.col.beta"));
        Assertions.assertNotNull(mgr.getWorkspace("cb.other_col.gamma"));

        mgr.dropCollection("cb", "col", false);

        Assertions.assertNull(mgr.getCollection("cb", "col"));
        Assertions.assertNull(mgr.getWorkspace("cb.col.alpha"),
                "dropCollection must cascade-remove workspaces under the collection");
        Assertions.assertNull(mgr.getWorkspace("cb.col.beta"),
                "dropCollection must cascade-remove workspaces under the collection");
        Assertions.assertNotNull(mgr.getWorkspace("cb.other_col.gamma"),
                "dropCollection must NOT touch workspaces under sibling collections");
    }

    @Test
    public void replayDropCollectionCascadesToo() {
        // Followers see the drop via replayDropCollection — the same cascade must run there or
        // a promoted-then-demoted-then-promoted FE would resurrect the orphan workspaces.
        ContextMgr follower = new ContextMgr();
        follower.replayCreateContextBase(ContextOpLog.forContextBase(100L, "cb", ImmutableMap.of()));
        follower.replayCreateCollection(ContextOpLog.forCollection(200L, 100L, "cb.col",
                "knowledge", ImmutableMap.of()));
        follower.replayCreateWorkspace(ContextOpLog.forWorkspace(301L, 200L, "cb.col.alpha",
                ImmutableMap.of()));

        follower.replayDropCollection(ContextOpLog.forQualifiedName("cb.col"));
        Assertions.assertNull(follower.getWorkspace("cb.col.alpha"));
    }

    @Test
    public void getContextBaseByIdMatchesPutRemoveLifecycle() {
        ContextMgr mgr = new ContextMgr();
        // Add three.
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(11L, "a", ImmutableMap.of()));
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(22L, "b", ImmutableMap.of()));
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(33L, "c", ImmutableMap.of()));

        Assertions.assertEquals("a", mgr.getContextBaseById(11L).getName());
        Assertions.assertEquals("b", mgr.getContextBaseById(22L).getName());
        Assertions.assertEquals("c", mgr.getContextBaseById(33L).getName());
        Assertions.assertNull(mgr.getContextBaseById(99L));

        // Drop one and verify the by-id index follows.
        mgr.replayDropContextBase(ContextOpLog.forName("b"));
        Assertions.assertNull(mgr.getContextBaseById(22L),
                "by-id index must drop the entry when the by-name map drops it");
        Assertions.assertEquals("a", mgr.getContextBaseById(11L).getName());
        Assertions.assertEquals("c", mgr.getContextBaseById(33L).getName());
    }

    @Test
    public void getCollectionByIdMatchesPutRemoveLifecycle() {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(100L, "cb", ImmutableMap.of()));
        mgr.replayCreateCollection(ContextOpLog.forCollection(201L, 100L, "cb.foo", "knowledge",
                ImmutableMap.of()));
        mgr.replayCreateCollection(ContextOpLog.forCollection(202L, 100L, "cb.bar", "knowledge",
                ImmutableMap.of()));

        Assertions.assertEquals("foo", mgr.getCollectionById(201L).getName());
        Assertions.assertEquals("bar", mgr.getCollectionById(202L).getName());
        Assertions.assertNull(mgr.getCollectionById(999L));

        mgr.dropCollection("cb", "foo", false);
        Assertions.assertNull(mgr.getCollectionById(201L));
        Assertions.assertEquals("bar", mgr.getCollectionById(202L).getName());

        // Cascade from contextbase: dropping the whole base purges every collection from the
        // by-id index.
        mgr.replayDropContextBase(ContextOpLog.forName("cb"));
        Assertions.assertNull(mgr.getCollectionById(202L),
                "cascade-remove via contextbase drop must clear the collection by-id index too");
    }
}
