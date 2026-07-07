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
import com.starrocks.persist.OperationType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Coverage for the in-place {@code RENAME CONTEXTBASE} rekey in {@link ContextMgr}.
 *
 * <p>A rename must be a metadata-only rekey: the base keeps its numeric id (so physical data and
 * privileges, both keyed by id, survive), while the in-memory name maps and their name-derived
 * collection / workspace keys move to the new prefix. The leader path emits an
 * {@code OP_RENAME_CONTEXTBASE} journal entry; the follower replay applies the exact same rekey, so
 * these tests double as a leader-failover / follower-divergence guard.
 *
 * <p>Uses the pseudo-journal harness because {@code renameContextBase} writes an edit-log entry
 * before mutating (same ordering as create/alter/drop).
 */
public class ContextMgrRenameTest {

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    /** Build a base with two collections and a workspace under one of them (pure replay, no journal). */
    private ContextMgr seededMgr() {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(
                101L, "ai_team", ImmutableMap.of("_owner_user", "kaisen", "k", "v")));
        mgr.replayCreateCollection(ContextOpLog.forCollection(
                201L, 101L, "ai_team.docs", "knowledge", null));
        mgr.replayCreateCollection(ContextOpLog.forCollection(
                202L, 101L, "ai_team.notes", "memory", null));
        mgr.replayCreateWorkspace(ContextOpLog.forWorkspace(
                301L, 201L, "ai_team.docs.session_1", ImmutableMap.of("ttl", "3600")));
        return mgr;
    }

    @Test
    public void testRenameRekeysBaseKeepingIdAndProperties() {
        ContextMgr mgr = seededMgr();
        long idBefore = mgr.getContextBase("ai_team").getId();

        mgr.renameContextBase("ai_team", "team_ai");

        // Old name is gone; new name resolves to the SAME id + properties (owner preserved).
        Assertions.assertNull(mgr.getContextBase("ai_team"));
        ContextMgr.ContextBaseMeta renamed = mgr.getContextBase("team_ai");
        Assertions.assertNotNull(renamed);
        Assertions.assertEquals(idBefore, renamed.getId());
        Assertions.assertEquals("kaisen", renamed.getOwner());
        Assertions.assertEquals("v", renamed.getProperties().get("k"));
        // The by-id reverse index (privilege validate() / getContextBaseById depend on it) still hits.
        Assertions.assertNotNull(mgr.getContextBaseById(idBefore));
        Assertions.assertEquals("team_ai", mgr.getContextBaseById(idBefore).getName());
    }

    @Test
    public void testRenameRekeysCollectionsAndWorkspaces() {
        ContextMgr mgr = seededMgr();

        mgr.renameContextBase("ai_team", "team_ai");

        // Collections follow the new prefix; the old prefix resolves to nothing.
        Assertions.assertEquals(0, mgr.listCollections("ai_team").size());
        Assertions.assertEquals(2, mgr.listCollections("team_ai").size());
        Assertions.assertNotNull(mgr.getCollection("team_ai", "docs"));
        Assertions.assertNull(mgr.getCollection("ai_team", "docs"));
        // Collection id / plain name are stable across the rename.
        Assertions.assertEquals(201L, mgr.getCollection("team_ai", "docs").getId());
        Assertions.assertEquals("docs", mgr.getCollection("team_ai", "docs").getName());

        // Workspaces follow the new prefix, and their stored FULL qualified name is rewritten.
        Assertions.assertEquals(0, mgr.listWorkspaces("ai_team").size());
        Assertions.assertEquals(1, mgr.listWorkspaces("team_ai").size());
        ContextMgr.WorkspaceMeta ws = mgr.getWorkspace("team_ai.docs.session_1");
        Assertions.assertNotNull(ws);
        Assertions.assertEquals(301L, ws.getId());
        Assertions.assertEquals("team_ai.docs.session_1", ws.getName());
        Assertions.assertEquals("3600", ws.getProperties().get("ttl"));
        Assertions.assertNull(mgr.getWorkspace("ai_team.docs.session_1"));
        // The workspace still resolves back to its parent collection after the rekey.
        Assertions.assertEquals(201L, mgr.resolveWorkspaceCollection(ws).getId());
    }

    /**
     * Leader emits OP_RENAME_CONTEXTBASE carrying the stable id + old name ({@code name}) + new name
     * ({@code qualifiedName}); replaying it on a fresh follower reproduces the leader's rekeyed state
     * exactly, including the collection / workspace rekey.
     */
    @Test
    public void testRenameRoundTripsThroughEditLog() throws Exception {
        ContextMgr leader = seededMgr();
        long id = leader.getContextBase("ai_team").getId();

        leader.renameContextBase("ai_team", "team_ai");

        ContextOpLog log = (ContextOpLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RENAME_CONTEXTBASE);
        Assertions.assertNotNull(log);
        Assertions.assertEquals(id, log.getId());
        Assertions.assertEquals("ai_team", log.getName());
        Assertions.assertEquals("team_ai", log.getQualifiedName());

        // Follower seeded with the pre-rename topology, then replays the captured op.
        ContextMgr follower = seededMgr();
        follower.replayRenameContextBase(log);
        Assertions.assertNull(follower.getContextBase("ai_team"));
        Assertions.assertEquals(id, follower.getContextBase("team_ai").getId());
        Assertions.assertEquals(2, follower.listCollections("team_ai").size());
        Assertions.assertEquals(1, follower.listWorkspaces("team_ai").size());
        Assertions.assertNotNull(follower.getWorkspace("team_ai.docs.session_1"));
    }

    @Test
    public void testRenameMissingSourceThrows() {
        ContextMgr mgr = seededMgr();
        Assertions.assertThrows(IllegalStateException.class,
                () -> mgr.renameContextBase("nope", "whatever"));
    }

    @Test
    public void testRenameToExistingNameThrows() {
        ContextMgr mgr = seededMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(102L, "team_ai", null));
        Assertions.assertThrows(IllegalStateException.class,
                () -> mgr.renameContextBase("ai_team", "team_ai"));
        // Failed rename leaves both bases untouched.
        Assertions.assertNotNull(mgr.getContextBase("ai_team"));
        Assertions.assertEquals(102L, mgr.getContextBase("team_ai").getId());
    }

    @Test
    public void testRenameToSameNameThrows() {
        ContextMgr mgr = seededMgr();
        Assertions.assertThrows(IllegalStateException.class,
                () -> mgr.renameContextBase("ai_team", "ai_team"));
    }

    @Test
    public void testReplayRenameMissingSourceIsNoOp() {
        ContextMgr mgr = seededMgr();
        // A rename whose source is absent (e.g. already dropped) must not throw during replay.
        mgr.replayRenameContextBase(ContextOpLog.forRename(999L, "ghost", "ghost_v2"));
        Assertions.assertNull(mgr.getContextBase("ghost_v2"));
        // Existing state is untouched.
        Assertions.assertNotNull(mgr.getContextBase("ai_team"));
    }
}
