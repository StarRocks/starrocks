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

import com.starrocks.context.ai.AIProvider;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.common.AIProviderBindings;
import com.starrocks.thrift.TAIModelSource;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Binds resolved provider calls once, without storing metadata in SQL or optimizer objects. */
public final class AIProviderBinder {
    private AIProviderBinder() {
    }

    public static AIProviderBindings bind(StatementBase statement, AIProviderMgr manager) {
        List<FunctionCallExpr> calls = ResolvedAIFunctionDetector.findAll(statement);
        Set<String> names = new LinkedHashSet<>();
        for (FunctionCallExpr call : calls) {
            if (call.getFn().getAiModelSource() == TAIModelSource.PROVIDER) {
                names.add(providerName(call));
            }
        }
        if (names.isEmpty()) {
            return AIProviderBindings.EMPTY;
        }
        Map<String, AIProvider> providers = manager.getProvidersByNames(names);
        Map<String, AIModelConfigs.ModelConfig> configs = new HashMap<>();
        for (String name : names) {
            AIProvider provider = providers.get(name);
            if (provider == null) {
                throw new SemanticException("AI provider '" + name + "' does not exist");
            }
            configs.put(name, AIModelConfigs.fromProvider(provider));
        }
        AIProviderBindings bindings = new AIProviderBindings(configs);
        validateCalls(calls, bindings);
        return bindings;
    }

    /** Reanalysis during a schema retry must not capture a different metadata snapshot. */
    public static void validateBindings(StatementBase statement, AIProviderBindings bindings) {
        validateCalls(ResolvedAIFunctionDetector.findAll(statement), bindings);
    }

    private static void validateCalls(List<FunctionCallExpr> calls, AIProviderBindings bindings) {
        for (FunctionCallExpr call : calls) {
            if (call.getFn().getAiModelSource() != TAIModelSource.PROVIDER) {
                continue;
            }
            String name = providerName(call);
            AIModelConfigs.ModelConfig config = bindings.getRequiredConfig(name);
            boolean embedding = AIModelConfigs.isTextEmbedding(call.getFn());
            if (!(embedding ? "TEXT_EMBEDDING" : "CHAT").equals(config.capability())) {
                throw new SemanticException("AI provider '" + name + "' requires type " + (embedding ? "EMBEDDING" : "CHAT"),
                        call.getPos());
            }
        }
    }

    private static String providerName(FunctionCallExpr call) {
        return AIModelConfigs.providerName(call.getChild(AIModelConfigs.getProviderArgument(call.getFn())));
    }
}
