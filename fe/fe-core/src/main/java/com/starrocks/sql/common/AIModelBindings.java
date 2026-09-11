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

import com.starrocks.catalog.AIModel;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Collection;
import java.util.Map;

/** Immutable named-model dependencies shared by authorization and physical planning. */
public final class AIModelBindings {
    public static final AIModelBindings EMPTY = new AIModelBindings(Map.of());

    private final Map<String, AIModel> models;

    public AIModelBindings(Map<String, AIModel> models) {
        this.models = Map.copyOf(models);
    }

    public AIModel getRequiredModel(String name) {
        AIModel model = models.get(name);
        if (model == null) {
            throw new SemanticException("AI model '" + name + "' is not bound to this statement");
        }
        return model;
    }

    public Collection<AIModel> modelsRequiringUsage() {
        return models.values();
    }

    public String configurationId(String name) {
        AIModel model = getRequiredModel(name);
        return "model:" + model.getId() + ":" + model.getRevision();
    }
}
