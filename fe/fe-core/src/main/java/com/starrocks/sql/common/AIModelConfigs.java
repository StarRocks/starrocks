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

import com.google.common.base.Preconditions;
import com.starrocks.builtins.VectorizedBuiltinFunctions;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AICapability;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AIFunctionDescriptor;
import com.starrocks.catalog.Function;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderProtocol;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.apache.commons.lang.StringUtils;

/** Resolves and validates providers used by AI functions. */
public final class AIModelConfigs {

    private AIModelConfigs() {
    }

    public static AICapability getAICapability(Function function) {
        return descriptor(function).capability();
    }

    public static int getModelArgument(Function function) {
        return descriptor(function).modelArgument();
    }

    public static int getProviderArgument(Function function) {
        return descriptor(function).providerArgument();
    }

    private static AIFunctionDescriptor descriptor(Function function) {
        AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(function.getFunctionId());
        Preconditions.checkState(descriptor != null, "AI function is missing generated descriptor metadata");
        return descriptor;
    }

    public static String providerName(Expr expression) {
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

    public static AIProviderType parseProviderType(Function function) {
        switch (getAICapability(function)) {
            case CHAT -> {
                return AIProviderType.CHAT;
            }
            case EMBEDDING -> {
                return AIProviderType.EMBEDDING;
            }
            default -> throw new SemanticException("Unsupported AI provider type: " + function.getFunctionId());
        }
    }

    public static boolean shouldUseDefaultProvider(FunctionCallExpr call) {
        int argument = getProviderArgument(call.getFn());
        return argument < 0 || providerName(call.getChild(argument)).isEmpty();
    }

    /** Resolve without capturing a plan snapshot; also used before optimizer pruning for authorization. */
    public static AIProvider resolveProvider(AIProviderMgr manager, FunctionCallExpr call) {
        int argument = getProviderArgument(call.getFn());
        AIProviderType requiredAIProviderType = parseProviderType(call.getFn());
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

    public static void validateProvider(AIProvider provider) {
        AIProviderType type = provider.getType();
        if (type != AIProviderType.CHAT && type != AIProviderType.EMBEDDING) {
            throw new SemanticException("AI functions require a CHAT or EMBEDDING provider, not " + type);
        }
        AIProviderProtocol protocol = provider.getProtocol();
        if (protocol != AIProviderProtocol.OPENAI) {
            throw new SemanticException("AI functions only support an OPENAI provider protocol now, not " + protocol);
        }
        String endpoint = provider.getEndpoint();
        String model = provider.getModel();
        if (StringUtils.isEmpty(endpoint) || StringUtils.isEmpty(model)) {
            throw new SemanticException("AI provider requires a valid endpoint and model");
        }
        final Integer timeout;
        final Integer dimensions;
        try {
            timeout = provider.getTimeoutMs();
            dimensions = type == AIProviderType.EMBEDDING ? provider.getDimensions() : null;
        } catch (NumberFormatException e) {
            throw new SemanticException("AI provider timeout_ms and dimensions must be positive integers");
        }
        if ((timeout != null && timeout <= 0) || (dimensions != null && dimensions <= 0)) {
            throw new SemanticException("AI provider timeout_ms and dimensions must be positive integers");
        }
    }

}
