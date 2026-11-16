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

import com.google.common.base.Preconditions;
import com.starrocks.builtins.VectorizedBuiltinFunctions;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AIFunctionDescriptor;
import com.starrocks.catalog.Function;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;

/** Resolves provider metadata and checks provider types for SQL AI functions. */
public final class AIFunctionProviderResolver {

    private AIFunctionProviderResolver() {
    }

    private static AIFunctionDescriptor descriptor(Function function) {
        AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(function.getFunctionId());
        Preconditions.checkState(descriptor != null, "AI function is missing generated descriptor metadata");
        return descriptor;
    }

    private static String providerName(Expr expression) {
        Preconditions.checkState(expression != null, "AI function is missing its provider argument");
        if (!expression.isConstant()) {
            throw new SemanticException("AI function requires a constant AI provider name", expression.getPos());
        }
        Expr folded = ExprUtils.analyzeAndCastFold(expression.clone());
        if (folded instanceof NullLiteral
                || folded instanceof StringLiteral literal && literal.getValue().isEmpty()) {
            return "";
        }
        if (!(folded instanceof StringLiteral literal) || literal.getValue().isBlank()) {
            throw new SemanticException("AI function requires a nonblank constant AI provider name", expression.getPos());
        }
        return literal.getValue();
    }

    public static AIProviderType requiredProviderType(Function function) {
        switch (descriptor(function).capability()) {
            case CHAT -> {
                return AIProviderType.CHAT;
            }
            case EMBEDDING -> {
                return AIProviderType.EMBEDDING;
            }
            default -> throw new SemanticException("Unsupported AI provider type: " + function.getFunctionId());
        }
    }

    /** Resolve without capturing a plan snapshot; also used before optimizer pruning for authorization. */
    public static AIProvider resolveProvider(AIProviderMgr manager, FunctionCallExpr call) {
        int argument = descriptor(call.getFn()).providerArgument();
        AIProviderType requiredAIProviderType = requiredProviderType(call.getFn());
        AIProvider provider;
        String name = argument < 0 ? "" : providerName(call.getChild(argument));
        if (!name.isEmpty()) {
            provider = manager.getProvider(name);
            if (provider == null) {
                throw new SemanticException("AI provider '" + name + "' does not exist", call.getPos());
            }
        } else {
            provider = manager.getDefaultProvider(requiredAIProviderType);
            if (provider == null) {
                throw new SemanticException("No default " + requiredAIProviderType
                        + " AI provider; use SET <provider_name> AS DEFAULT AI PROVIDER", call.getPos());
            }
        }
        if (provider.getType() != requiredAIProviderType) {
            throw new SemanticException("AI provider '" + provider.getName() + "' requires type " + requiredAIProviderType,
                    call.getPos());
        }
        return provider;
    }
}
