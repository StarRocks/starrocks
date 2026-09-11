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
import com.starrocks.persist.DropAIModelLog;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AIModelMgrImageTest {
    @Test
    public void testImageRebuildsIndependentIndexes() throws Exception {
        AIModelMgr source = new AIModelMgr();
        AIModel model = AIModel.create(123, "Chat", AIModelMgrTest.properties(), "")
                .withAlteredProperties(Map.of("model", "next"), "comment");
        source.replayCreateModel(model);
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        AIModelMgr restored = new AIModelMgr();
        SRMetaBlockReader reader = image.getMetaBlockReader();
        restored.load(reader);
        reader.close();
        Assertions.assertEquals(model, restored.getByName("Chat"));
        Assertions.assertSame(restored.getByName("Chat"), restored.getById(123));
        Assertions.assertNull(restored.getByName("chat"));
        Assertions.assertEquals(List.of(model), restored.listModels());
        source.replayDropModel(new DropAIModelLog(123));
        Assertions.assertNotNull(restored.getById(123));
    }

    @Test
    public void testEmptyImageClearsPreviousIndexes() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        new AIModelMgr().save(image.getImageWriter());
        AIModelMgr restored = new AIModelMgr();
        restored.replayCreateModel(AIModel.create(123, "Chat", AIModelMgrTest.properties(), ""));
        SRMetaBlockReader reader = image.getMetaBlockReader();
        restored.load(reader);
        reader.close();
        Assertions.assertTrue(restored.listModels().isEmpty());
        Assertions.assertNull(restored.getByName("Chat"));
        Assertions.assertNull(restored.getById(123));
    }

    @Test
    public void testCorruptImageDoesNotPublishPartialState() throws Exception {
        AIModel original = AIModel.create(123, "Original", AIModelMgrTest.properties(), "");
        for (boolean duplicateId : List.of(true, false)) {
            AIModelMgr restored = new AIModelMgr();
            restored.replayCreateModel(original);
            UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
            SRMetaBlockWriter writer = image.getImageWriter().getBlockWriter(SRMetaBlockID.AI_MODEL_MGR, 3);
            writer.writeInt(2);
            writer.writeJson(AIModel.create(456, "Duplicate", AIModelMgrTest.properties(), ""));
            writer.writeJson(AIModel.create(duplicateId ? 456 : 457, duplicateId ? "Other" : "Duplicate",
                    AIModelMgrTest.properties(), ""));
            writer.close();
            SRMetaBlockReader reader = image.getMetaBlockReader();
            Assertions.assertThrows(IOException.class, () -> restored.load(reader));
            Assertions.assertEquals(List.of(original), restored.listModels());
            Assertions.assertSame(original, restored.getByName("Original"));
        }
    }

    @Test
    public void testInvalidCountDoesNotClearExistingState() throws Exception {
        AIModel original = AIModel.create(123, "Original", AIModelMgrTest.properties(), "");
        AIModelMgr restored = new AIModelMgr();
        restored.replayCreateModel(original);
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        SRMetaBlockWriter writer = image.getImageWriter().getBlockWriter(SRMetaBlockID.AI_MODEL_MGR, 1);
        writer.writeInt(-1);
        writer.close();
        SRMetaBlockReader reader = image.getMetaBlockReader();
        Assertions.assertThrows(IOException.class, () -> restored.load(reader));
        Assertions.assertEquals(List.of(original), restored.listModels());
    }
}
