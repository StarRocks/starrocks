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
import com.starrocks.epack.persist.OperationTypeEPack;
import com.starrocks.persist.ContextOpLog;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Edit-log serialization roundtrip for {@link ContextMgr}. Drives a leader-side write through the
 * pseudo-journal recorder, then replays the captured {@link ContextOpLog} entry on a fresh
 * {@code ContextMgr} and asserts state matches. This catches the class of regression where a
 * field rename or {@code @SerializedName} change silently breaks follower replay.
 *
 * <p>The five op codes covered here form the full control-plane lifecycle: contextbase create,
 * collection create + drop, workspace create, retrieval profile create. The remaining four
 * ({@code OP_DROP_CONTEXTBASE}, {@code OP_DROP_CONTEXT_WORKSPACE},
 * {@code OP_DROP_CONTEXT_RETRIEVAL_PROFILE}, {@code OP_ALTER_CONTEXTBASE}) follow the same payload
 * shape and the existing {@code ContextMgrReplayTest} already pins the in-memory replay logic.
 */
public class ContextMgrEditLogTest {

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testCreateContextBaseRoundTripsThroughEditLog() throws Exception {
        ContextMgr leader = new ContextMgr();
        leader.createContextBase("editlog_cb", ImmutableMap.of("default_consistency", "STRICT"), false);

        ContextOpLog log = (ContextOpLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_CONTEXTBASE);
        Assertions.assertNotNull(log);
        Assertions.assertEquals("editlog_cb", log.getName());
        Assertions.assertEquals("STRICT", log.getProperties().get("default_consistency"));

        ContextMgr follower = new ContextMgr();
        Assertions.assertNull(follower.getContextBase("editlog_cb"));
        follower.replayCreateContextBase(log);
        ContextMgr.ContextBaseMeta replayed = follower.getContextBase("editlog_cb");
        Assertions.assertNotNull(replayed);
        Assertions.assertEquals(leader.getContextBase("editlog_cb").getId(), replayed.getId());
        Assertions.assertEquals("STRICT", replayed.getProperties().get("default_consistency"));
    }

    @Test
    public void testCreateAndDropCollectionRoundTrip() throws Exception {
        ContextMgr leader = new ContextMgr();
        leader.createContextBase("editlog_cb2", null, false);

        // Drain the create-contextbase entry so the next replayNextJournal returns the collection op.
        UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_CONTEXTBASE);

        leader.createCollection("editlog_cb2", "pipeline", "knowledge",
                ImmutableMap.of("retrieval_profile", "balanced"), false);
        ContextOpLog createLog = (ContextOpLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_CONTEXT_COLLECTION);
        Assertions.assertEquals("editlog_cb2.pipeline", createLog.getName());
        Assertions.assertEquals("knowledge", createLog.getTypeTag());
        Assertions.assertEquals("balanced", createLog.getProperties().get("retrieval_profile"));

        leader.dropCollection("editlog_cb2", "pipeline", false);
        ContextOpLog dropLog = (ContextOpLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_DROP_CONTEXT_COLLECTION);
        Assertions.assertEquals("editlog_cb2.pipeline", dropLog.getQualifiedName());

        // Replay both entries on a fresh follower.
        ContextMgr follower = new ContextMgr();
        follower.replayCreateContextBase(ContextOpLog.forContextBase(
                leader.getContextBase("editlog_cb2").getId(), "editlog_cb2", null));
        follower.replayCreateCollection(createLog);
        Assertions.assertEquals(1, follower.listCollections("editlog_cb2").size());
        follower.replayDropCollection(dropLog);
        Assertions.assertEquals(0, follower.listCollections("editlog_cb2").size());
    }

    @Test
    public void testCreateWorkspaceRoundTrip() throws Exception {
        ContextMgr leader = new ContextMgr();
        leader.createContextBase("cb", null, false);
        UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_CONTEXTBASE);
        long colId = leader.createCollection("cb", "col", "knowledge", null, false);
        UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_CONTEXT_COLLECTION);
        leader.createWorkspace("cb.col.session_1", colId,
                ImmutableMap.of("ttl_hours", "24"), false);
        ContextOpLog log = (ContextOpLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_CONTEXT_WORKSPACE);
        Assertions.assertEquals("cb.col.session_1", log.getQualifiedName());
        Assertions.assertEquals(colId, log.getParentId());
        Assertions.assertEquals("24", log.getProperties().get("ttl_hours"));

        ContextMgr follower = new ContextMgr();
        follower.replayCreateWorkspace(log);
        Assertions.assertEquals(1, follower.listWorkspaces(null).size());
        Assertions.assertEquals("cb.col.session_1", follower.listWorkspaces(null).get(0).getName());
    }

    @Test
    public void testCreateRetrievalProfileRoundTrip() throws Exception {
        ContextMgr leader = new ContextMgr();
        leader.createRetrievalProfile("balanced_v1",
                ImmutableMap.of("fusion_mode", "RRF", "text_weight", "0.5"), false);
        ContextOpLog log = (ContextOpLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_CONTEXT_RETRIEVAL_PROFILE);
        Assertions.assertEquals("balanced_v1", log.getName());
        Assertions.assertEquals("RRF", log.getProperties().get("fusion_mode"));

        ContextMgr follower = new ContextMgr();
        follower.replayCreateRetrievalProfile(log);
        ContextMgr.RetrievalProfileMeta replayed = follower.getRetrievalProfile("balanced_v1");
        Assertions.assertNotNull(replayed);
        Assertions.assertEquals(leader.getRetrievalProfile("balanced_v1").getId(), replayed.getId());
        Assertions.assertEquals("0.5", replayed.getProperties().get("text_weight"));
    }

    @Test
    public void testAlterContextBaseRoundTripsThroughEditLog() throws Exception {
        // Regression: ALTER CONTEXTBASE used to be a leader-side no-op even though replay logic
        // existed; the leader path now emits OP_ALTER_CONTEXTBASE so the merge survives failover.
        ContextMgr leader = new ContextMgr();
        leader.createContextBase("alter_cb",
                ImmutableMap.of("default_consistency", "PRIMARY_CONSISTENT"), false);
        UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_CONTEXTBASE);

        // Merge: change default_consistency, add a new key — old keys not present in the call must
        // be preserved.
        leader.alterContextBase("alter_cb",
                ImmutableMap.of("default_consistency", "STRICT", "owner", "alice"), false);

        ContextOpLog log = (ContextOpLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_ALTER_CONTEXTBASE);
        Assertions.assertNotNull(log);
        Assertions.assertEquals("alter_cb", log.getName());
        Assertions.assertEquals("STRICT", log.getProperties().get("default_consistency"));
        Assertions.assertEquals("alice", log.getProperties().get("owner"));

        // Replay the merge on a follower seeded with the original meta.
        ContextMgr follower = new ContextMgr();
        follower.replayCreateContextBase(ContextOpLog.forContextBase(
                leader.getContextBase("alter_cb").getId(), "alter_cb",
                ImmutableMap.of("default_consistency", "PRIMARY_CONSISTENT")));
        follower.replayAlterContextBase(log);
        ContextMgr.ContextBaseMeta replayed = follower.getContextBase("alter_cb");
        Assertions.assertEquals("STRICT", replayed.getProperties().get("default_consistency"));
        Assertions.assertEquals("alice", replayed.getProperties().get("owner"));
    }

    @Test
    public void testDropContextBaseCascadesCollectionsAndWorkspaces() throws Exception {
        // Regression: drop used to leave collections and workspaces dangling in memory and in the
        // image; the leader path now removes them inline and the replay path mirrors the cascade.
        ContextMgr leader = new ContextMgr();
        leader.createContextBase("cascade_cb", null, false);
        long cascadeColId = leader.createCollection("cascade_cb", "pipeline_rules", "knowledge", null, false);
        leader.createWorkspace("cascade_cb.pipeline_rules.session_1", cascadeColId, null, false);

        Assertions.assertEquals(1, leader.listCollections("cascade_cb").size());
        Assertions.assertEquals(1, leader.listWorkspaces("cascade_cb").size());

        leader.dropContextBase("cascade_cb", false);

        Assertions.assertNull(leader.getContextBase("cascade_cb"));
        Assertions.assertEquals(0, leader.listCollections("cascade_cb").size());
        Assertions.assertEquals(0, leader.listWorkspaces("cascade_cb").size());
    }
}
