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
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Image save/load roundtrip for {@link ContextMgr}. Validates that all four control-plane object
 * kinds — contextbase, collection, workspace, retrieval profile — survive serialization to a
 * metablock and deserialization on a fresh manager. This is the test that catches regressions in
 * the {@code @SerializedName} fields of the inner {@code Meta} records.
 *
 * <p>Uses {@link UtFrameUtils.PseudoImage} so the test does not need a real filesystem; the writer
 * and reader hold the buffer in memory. The leader-side writes go through the replay path (not
 * the {@code create*} path) so we don't accidentally trigger an edit-log write that interferes
 * with a parallel {@link ContextMgrEditLogTest}.
 */
public class ContextMgrImageTest {

    @Test
    public void testEmptyMgrRoundTrips() throws Exception {
        ContextMgr leader = new ContextMgr();

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        leader.save(image.getImageWriter());

        ContextMgr follower = new ContextMgr();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        follower.load(reader);
        reader.close();

        Assertions.assertEquals(0, follower.listContextBases().size());
        Assertions.assertEquals(0, follower.listCollections(null).size());
        Assertions.assertEquals(0, follower.listWorkspaces(null).size());
        Assertions.assertEquals(0, follower.listRetrievalProfiles().size());
    }

    @Test
    public void testFourObjectKindsRoundTrip() throws Exception {
        ContextMgr leader = new ContextMgr();
        // Use replay paths to populate state without touching the edit log — this test's scope is
        // pure image roundtrip, not edit-log behaviour.
        leader.replayCreateContextBase(ContextOpLog.forContextBase(
                100L, "img_cb", ImmutableMap.of("default_consistency", "STRICT")));
        leader.replayCreateContextBase(ContextOpLog.forContextBase(
                101L, "img_cb_two", ImmutableMap.of("default_consistency", "EVENTUAL")));
        leader.replayCreateCollection(ContextOpLog.forCollection(
                200L, 100L, "img_cb.knowledge", "knowledge",
                ImmutableMap.of("retrieval_profile", "balanced")));
        leader.replayCreateCollection(ContextOpLog.forCollection(
                201L, 100L, "img_cb.skills", "skill", null));
        leader.replayCreateWorkspace(ContextOpLog.forWorkspace(
                300L, 200L, "img_cb.knowledge.session_a", ImmutableMap.of("ttl_hours", "12")));
        leader.replayCreateRetrievalProfile(ContextOpLog.forRetrievalProfile(
                400L, "balanced", ImmutableMap.of("fusion_mode", "RRF", "text_weight", "0.5")));

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        leader.save(image.getImageWriter());

        ContextMgr follower = new ContextMgr();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        follower.load(reader);
        reader.close();

        // Contextbases
        Assertions.assertEquals(2, follower.listContextBases().size());
        ContextMgr.ContextBaseMeta cb1 = follower.getContextBase("img_cb");
        Assertions.assertNotNull(cb1);
        Assertions.assertEquals(100L, cb1.getId());
        Assertions.assertEquals("STRICT", cb1.getProperties().get("default_consistency"));
        ContextMgr.ContextBaseMeta cb2 = follower.getContextBase("img_cb_two");
        Assertions.assertNotNull(cb2);
        Assertions.assertEquals("EVENTUAL", cb2.getProperties().get("default_consistency"));

        // Collections (scoped to the parent contextbase)
        Assertions.assertEquals(2, follower.listCollections("img_cb").size());
        Assertions.assertEquals(0, follower.listCollections("img_cb_two").size());
        ContextMgr.CollectionMeta collKnow = follower.listCollections("img_cb").stream()
                .filter(c -> c.getName().equals("knowledge")).findFirst().orElseThrow();
        Assertions.assertEquals(200L, collKnow.getId());
        Assertions.assertEquals(100L, collKnow.getContextBaseId());
        Assertions.assertEquals("knowledge", collKnow.getCollectionType());
        Assertions.assertEquals("balanced", collKnow.getProperties().get("retrieval_profile"));

        // Workspaces
        Assertions.assertEquals(1, follower.listWorkspaces(null).size());
        ContextMgr.WorkspaceMeta ws = follower.listWorkspaces(null).get(0);
        Assertions.assertEquals(300L, ws.getId());
        Assertions.assertEquals(200L, ws.getCollectionId());
        Assertions.assertEquals("img_cb.knowledge.session_a", ws.getName());
        Assertions.assertEquals("12", ws.getProperties().get("ttl_hours"));

        // Retrieval profiles
        Assertions.assertEquals(1, follower.listRetrievalProfiles().size());
        ContextMgr.RetrievalProfileMeta prof = follower.getRetrievalProfile("balanced");
        Assertions.assertNotNull(prof);
        Assertions.assertEquals("RRF", prof.getProperties().get("fusion_mode"));
        Assertions.assertEquals("0.5", prof.getProperties().get("text_weight"));
    }

    @Test
    public void testLoadClearsExistingState() throws Exception {
        ContextMgr leader = new ContextMgr();
        leader.replayCreateContextBase(ContextOpLog.forContextBase(100L, "cb_a", null));

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        leader.save(image.getImageWriter());

        ContextMgr follower = new ContextMgr();
        // Pre-populate the follower with state that should be CLEARED on load — `cb_a` should
        // survive because the loaded image contains it; `cb_stale` should be dropped because it
        // isn't in the image.
        follower.replayCreateContextBase(ContextOpLog.forContextBase(999L, "cb_stale", null));
        Assertions.assertNotNull(follower.getContextBase("cb_stale"));

        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        follower.load(reader);
        reader.close();

        Assertions.assertNotNull(follower.getContextBase("cb_a"));
        Assertions.assertNull(follower.getContextBase("cb_stale"));
        Assertions.assertEquals(1, follower.listContextBases().size());
    }
}
