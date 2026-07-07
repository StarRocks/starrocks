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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Exercises the in-memory replay path of {@link ContextMgr}. These tests do not depend on the full FE
 * image/edit-log stack — they call the replay APIs that the dispatch in {@code EditLog.loadJournal}
 * invokes, so a correctness regression here tells us leader-failover replay would drop state.
 */
public class ContextMgrReplayTest {

    @Test
    public void testReplayCreateContextBase() {
        ContextMgr mgr = new ContextMgr();
        ContextOpLog log = ContextOpLog.forContextBase(
                101L, "sales_ai", ImmutableMap.of("default_consistency", "STRICT"));
        mgr.replayCreateContextBase(log);

        ContextMgr.ContextBaseMeta meta = mgr.getContextBase("sales_ai");
        Assertions.assertNotNull(meta);
        Assertions.assertEquals(101L, meta.getId());
        Assertions.assertEquals("STRICT", meta.getProperties().get("default_consistency"));
    }

    @Test
    public void testReplayAlterContextBaseMergesProperties() {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(
                101L, "sales_ai",
                ImmutableMap.of("default_consistency", "STRICT", "owner", "team_sales")));

        // Alter overrides consistency, keeps owner.
        Map<String, String> altered = ImmutableMap.of("default_consistency", "PRIMARY_CONSISTENT");
        mgr.replayAlterContextBase(ContextOpLog.forContextBase(101L, "sales_ai", altered));

        ContextMgr.ContextBaseMeta meta = mgr.getContextBase("sales_ai");
        Assertions.assertEquals("PRIMARY_CONSISTENT", meta.getProperties().get("default_consistency"));
        Assertions.assertEquals("team_sales", meta.getProperties().get("owner"));
    }

    @Test
    public void testReplayDropContextBase() {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(101L, "sales_ai", null));
        mgr.replayDropContextBase(ContextOpLog.forName("sales_ai"));
        Assertions.assertNull(mgr.getContextBase("sales_ai"));
    }

    @Test
    public void testReplayCollectionAndDropUseQualifiedName() {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateCollection(ContextOpLog.forCollection(
                201L, 101L, "sales_ai.pipeline_rules", "knowledge", null));

        Assertions.assertEquals(1, mgr.listCollections("sales_ai").size());
        Assertions.assertEquals("pipeline_rules", mgr.listCollections("sales_ai").get(0).getName());
        Assertions.assertEquals("knowledge", mgr.listCollections("sales_ai").get(0).getCollectionType());

        mgr.replayDropCollection(ContextOpLog.forQualifiedName("sales_ai.pipeline_rules"));
        Assertions.assertEquals(0, mgr.listCollections("sales_ai").size());
    }

    @Test
    public void testReplayWorkspaceAndRetrievalProfile() {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateWorkspace(ContextOpLog.forWorkspace(
                301L, 201L, "sales_ai.pipeline_rules.session_123",
                ImmutableMap.of("ttl_hours", "24")));
        Assertions.assertEquals(1, mgr.listWorkspaces("sales_ai").size());
        Assertions.assertEquals("24",
                mgr.listWorkspaces("sales_ai").get(0).getProperties().get("ttl_hours"));

        mgr.replayCreateRetrievalProfile(ContextOpLog.forRetrievalProfile(
                401L, "balanced_v1", ImmutableMap.of("fusion_mode", "RRF")));
        Assertions.assertEquals("balanced_v1", mgr.getRetrievalProfile("balanced_v1").getName());
        Assertions.assertEquals("RRF",
                mgr.getRetrievalProfile("balanced_v1").getProperties().get("fusion_mode"));

        mgr.replayDropWorkspace(ContextOpLog.forQualifiedName("sales_ai.pipeline_rules.session_123"));
        Assertions.assertEquals(0, mgr.listWorkspaces("sales_ai").size());

        mgr.replayDropRetrievalProfile(ContextOpLog.forName("balanced_v1"));
        Assertions.assertNull(mgr.getRetrievalProfile("balanced_v1"));
    }
}
