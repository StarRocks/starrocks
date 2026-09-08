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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.AIModel;
import com.starrocks.server.AIModelMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.common.AIModelBindings;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.thrift.TAIModelSource;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Binds all resolved named-model calls once, before authorization or optimization. */
public final class AIModelBinder {
    private AIModelBinder() {
    }

    public static AIModelBindings bind(StatementBase statement, AIModelMgr manager) {
        List<FunctionCallExpr> calls = ResolvedAIFunctionDetector.findAll(statement);
        Set<String> names = new LinkedHashSet<>();
        for (FunctionCallExpr call : calls) {
            if (call.getFn().getAiModelSource() == TAIModelSource.AI_MODEL) {
                names.add(modelName(call));
            }
        }
        if (names.isEmpty()) {
            return AIModelBindings.EMPTY;
        }
        Map<String, AIModel> models = manager.getModelsByNames(names);
        for (String name : names) {
            if (!models.containsKey(name)) {
                throw new SemanticException("AI model '" + name + "' does not exist");
            }
        }
        AIModelBindings bindings = new AIModelBindings(models);
        validateCalls(calls, bindings);
        return bindings;
    }

    /** Reanalysis must not introduce a new dependency after authorization. */
    public static void validateBindings(StatementBase statement, AIModelBindings bindings) {
        validateCalls(ResolvedAIFunctionDetector.findAll(statement), bindings);
    }

    private static void validateCalls(List<FunctionCallExpr> calls, AIModelBindings bindings) {
        for (FunctionCallExpr call : calls) {
            if (call.getFn().getAiModelSource() != TAIModelSource.AI_MODEL) {
                continue;
            }
            AIModel model = bindings.getRequiredModel(modelName(call));
            AIModel.Capability capability = AIModelConfigs.isTextEmbedding(call.getFn())
                    ? AIModel.Capability.TEXT_EMBEDDING : AIModel.Capability.CHAT;
            if (model.getCapability() != capability) {
                throw new SemanticException("AI model '" + model.getName() + "' requires capability " + capability,
                        call.getPos());
            }
        }
    }

    private static String modelName(FunctionCallExpr call) {
        return AIModelConfigs.aiModelName(call.getChild(AIModelConfigs.getAIModelArgument(call.getFn())));
    }
}
