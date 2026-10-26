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

import com.starrocks.context.ai.AIProviderType;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;

import java.lang.reflect.Field;
import java.util.EnumMap;
import java.util.Map;

/** Scoped registry defaults for tests, restored before removing their provider. */
public final class AIProviderTestUtils implements AutoCloseable {
    private final AIProviderMgr manager;
    private final String name;
    private final Field defaults;
    private final Object previousDefaults;

    private AIProviderTestUtils(String name, AIProviderType type, Map<String, String> parameters) throws Exception {
        this.manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        this.name = name;
        defaults = AIProviderMgr.class.getDeclaredField("defaultByType");
        defaults.setAccessible(true);
        previousDefaults = defaults.get(manager);
        // Replace the map so setDefaultProvider does not mutate the saved registry state.
        EnumMap<AIProviderType, String> isolated = new EnumMap<>(AIProviderType.class);
        for (AIProviderType candidate : AIProviderType.values()) {
            var provider = manager.getDefaultProvider(candidate);
            if (provider != null) {
                isolated.put(candidate, provider.getId());
            }
        }
        manager.createProvider(name, type, parameters, "");
        defaults.set(manager, isolated);
        manager.setDefaultProvider(name);
    }

    public static AIProviderTestUtils withDefaultProvider(String name, AIProviderType type,
                                                          Map<String, String> parameters) throws Exception {
        return new AIProviderTestUtils(name, type, parameters);
    }

    @Override
    public void close() throws Exception {
        defaults.set(manager, previousDefaults);
        manager.dropProvider(name, true);
    }
}
