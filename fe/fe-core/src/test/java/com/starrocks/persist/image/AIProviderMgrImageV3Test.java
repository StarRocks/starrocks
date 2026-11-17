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

package com.starrocks.persist.image;

import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class AIProviderMgrImageV3Test {
    @TempDir
    Path temporary;

    @Test
    void sourceValuesSurviveBothImageFormats() throws Exception {
        AIProviderMgr source = new AIProviderMgr();
        AIProvider provider = new AIProvider("provider-123", "chat-service", AIProviderType.CHAT,
                Map.of("endpoint", "https://example.invalid/v1", "model", "chat-model",
                        "api_key", "test-only-key", "timeout_ms", "17000"), "persisted provider");
        source.replayCreateProvider(provider);
        setField(source, "defaultByType", Map.of(AIProviderType.CHAT, "provider-123"));
        Path file = LoadImageTestSupport.save(temporary, source::save);
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        AIProviderMgr v2 = new AIProviderMgr();
        v2.load(image.getMetaBlockReader());
        AIProviderMgr v3 = new AIProviderMgr();
        LoadImageTestSupport.load(file, v3.getImageV3Loaders());
        for (AIProviderMgr restored : List.of(v2, v3)) {
            AIProvider actual = restored.getDefaultProvider(AIProviderType.CHAT);
            assertNotNull(actual);
            assertEquals("provider-123", actual.getId());
            assertEquals("chat-service", actual.getName());
            assertEquals(AIProviderType.CHAT, actual.getType());
            assertEquals("chat-model", actual.getModel());
            assertEquals("test-only-key", actual.getApiKey());
            assertEquals(17000, actual.getTimeoutMs());
            assertEquals("persisted provider", actual.getComment());
            assertEquals(1, restored.listProviders().size());
        }
    }
}
