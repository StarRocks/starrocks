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

package com.starrocks.server;

import com.starrocks.catalog.AIModel;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.persist.DropAIModelLog;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class AIModelMgrTest {
    private AIModelMgr mgr;

    static Map<String, String> properties() {
        return Map.of("capability", "CHAT", "provider", "openai_compatible",
                "endpoint", "https://models.example.test/v1/chat/completions",
                "model", "chat-model", "credential_ref", "TEST_MODEL");
    }

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
        mgr = new AIModelMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testCrudAndCaseSensitiveSnapshots() throws Exception {
        AIModel upper = mgr.createModel("Chat", properties(), "original", false);
        AIModel lower = mgr.createModel("chat", properties(), "", false);
        Assertions.assertNotEquals(upper.getId(), lower.getId());
        Assertions.assertSame(upper, mgr.getById(upper.getId()));
        Assertions.assertNull(mgr.getByName("CHAT"));
        Assertions.assertSame(upper, mgr.createModel("Chat", properties(), "ignored", true));
        Assertions.assertThrows(AlreadyExistsException.class, () -> mgr.createModel("Chat", properties(), "", false));
        List<AIModel> snapshot = mgr.listModels();
        Assertions.assertEquals(List.of(upper, lower), snapshot);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> snapshot.clear());
        Map<String, AIModel> selected = mgr.getModelsByNames(List.of("Chat", "missing", "Chat"));
        Assertions.assertEquals(Map.of("Chat", upper), selected);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> selected.clear());

        AIModel changed = mgr.alterModel(upper, Map.of("model", "new-model"), "");
        Assertions.assertEquals(2, changed.getRevision());
        Assertions.assertEquals("", changed.getComment());
        Assertions.assertSame(upper, snapshot.get(0));
        Assertions.assertSame(upper, selected.get("Chat"));
        Assertions.assertSame(changed, mgr.alterModel(changed, Map.of(), null));
        Assertions.assertThrows(DdlException.class, () -> mgr.alterModel(upper, Map.of(), null));
        Assertions.assertThrows(DdlException.class, () -> mgr.dropModel(upper));
        mgr.dropModel(changed);
        Assertions.assertNull(mgr.getById(upper.getId()));
        Assertions.assertSame(lower, mgr.getByName("chat"));
    }

    @Test
    public void testDropRecreateRejectsPreviouslyAuthorizedTarget() throws Exception {
        AIModel old = mgr.createModel("Chat", properties(), "", false);
        mgr.dropModel(old);
        AIModel replacement = mgr.createModel("Chat", properties(), "", false);
        Assertions.assertTrue(replacement.getId() > old.getId());
        Assertions.assertThrows(DdlException.class, () -> mgr.alterModel(old, Map.of("model", "wrong"), null));
        Assertions.assertThrows(DdlException.class, () -> mgr.dropModel(old));
        mgr.replayDropModel(new DropAIModelLog(old.getId()));
        Assertions.assertSame(replacement, mgr.getByName("Chat"));
    }

    @Test
    public void testReplayIsIdempotentAndRejectsConflicts() throws Exception {
        AIModel model = AIModel.create(123, "Chat", properties(), "");
        AIModelMgr follower = new AIModelMgr();
        follower.replayCreateModel(model);
        follower.replayCreateModel(model);
        Assertions.assertNull(mgr.getByName("Chat"));
        AIModel changed = model.withAlteredProperties(Map.of("model", "new"), null);
        follower.replayAlterModel(changed);
        follower.replayAlterModel(changed);
        Assertions.assertThrows(IllegalStateException.class, () -> follower.replayAlterModel(model));
        Assertions.assertThrows(IllegalStateException.class,
                () -> follower.replayCreateModel(AIModel.create(124, "Chat", properties(), "")));
        Assertions.assertThrows(IllegalStateException.class,
                () -> follower.replayCreateModel(AIModel.create(123, "Other", properties(), "")));
        Assertions.assertThrows(IllegalStateException.class,
                () -> follower.replayAlterModel(changed.withAlteredProperties(Map.of("model", "next"), null)
                        .withAlteredProperties(Map.of("model", "skipped"), null)));
        follower.replayDropModel(new DropAIModelLog(123));
        follower.replayDropModel(new DropAIModelLog(123));
        Assertions.assertTrue(follower.listModels().isEmpty());
    }
}
