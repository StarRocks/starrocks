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

package com.starrocks.utframe;

import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;

import java.lang.reflect.Field;
import java.util.Map;

/** Provider metadata fixtures for analyzer and planner tests; no remote requests are made. */
public final class AIProviderTestUtil {
    private AIProviderTestUtil() {
    }

    public static String chatProperty(String key) {
        return property(AIProviderType.CHAT, key);
    }

    public static String embeddingProperty(String key) {
        return property(AIProviderType.EMBEDDING, key);
    }

    public static void setChatProperty(String key, String value) {
        setProperty(AIProviderType.CHAT, key, value);
    }

    public static void setEmbeddingProperty(String key, String value) {
        setProperty(AIProviderType.EMBEDDING, key, value);
    }

    public static String defaultName(AIProviderType type) {
        return "test_default_" + type.lower();
    }

    public static void clearDefaults() {
        try {
            AIProviderMgr manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
            clearDefault(manager, AIProviderType.CHAT);
            clearDefault(manager, AIProviderType.EMBEDDING);
            manager.dropProvider(defaultName(AIProviderType.CHAT), true);
            manager.dropProvider(defaultName(AIProviderType.EMBEDDING), true);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static String property(AIProviderType type, String key) {
        AIProvider provider = GlobalStateMgr.getCurrentState().getAIProviderMgr().getDefaultProvider(type);
        return provider == null ? "" : provider.getParams().getOrDefault(key, "");
    }

    // SQL intentionally cannot unset/drop a default. Reset only test fixture state, as in AIProviderMgrTest.
    private static void clearDefault(AIProviderMgr manager, AIProviderType type) throws ReflectiveOperationException {
        Field defaults = AIProviderMgr.class.getDeclaredField("defaultByType");
        defaults.setAccessible(true);
        ((Map<?, ?>) defaults.get(manager)).remove(type);
        if (type == AIProviderType.EMBEDDING) {
            Field legacyDefault = AIProviderMgr.class.getDeclaredField("defaultProviderId");
            legacyDefault.setAccessible(true);
            legacyDefault.set(manager, "");
        }
    }

    private static void setProperty(AIProviderType type, String key, String value) {
        try {
            AIProviderMgr manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
            String name = defaultName(type);
            if (key.equals("endpoint") && value.isEmpty()) {
                clearDefault(manager, type);
                manager.dropProvider(name, true);
                return;
            }
            if (manager.getProvider(name) == null) {
                if (value.isEmpty()) {
                    return;
                }
                manager.createProvider(name, type, Map.of("endpoint", "https://models.example.test/v1/inference",
                        "model", "default-model", "protocol", "openai"), "");
            }
            manager.alterProvider(name, Map.of(key, value), false);
            manager.setDefaultProvider(name);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
