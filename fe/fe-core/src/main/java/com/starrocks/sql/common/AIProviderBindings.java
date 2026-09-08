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

package com.starrocks.sql.common;

import com.starrocks.sql.analyzer.SemanticException;

import java.util.Map;

/** Immutable provider execution snapshots shared by all physical plans of one statement. */
public final class AIProviderBindings {
    public static final AIProviderBindings EMPTY = new AIProviderBindings(Map.of());

    private final Map<String, AIModelConfigs.ModelConfig> providers;

    public AIProviderBindings(Map<String, AIModelConfigs.ModelConfig> providers) {
        this.providers = Map.copyOf(providers);
    }

    public AIModelConfigs.ModelConfig getRequiredConfig(String name) {
        AIModelConfigs.ModelConfig config = providers.get(name);
        if (config == null) {
            throw new SemanticException("AI provider '" + name + "' is not bound to this statement");
        }
        return config;
    }

    public String configurationId(String name) {
        return "provider:" + getRequiredConfig(name).providerId();
    }
}
