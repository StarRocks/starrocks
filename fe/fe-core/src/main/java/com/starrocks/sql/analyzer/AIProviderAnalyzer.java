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

import com.google.common.base.Strings;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.aiprovider.AlterAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.CreateAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.DescAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.DropAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.SetDefaultAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.ShowAIProvidersStmt;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Semantic validation for the unified {@code AI PROVIDER} DDL. Validation is keyed off the declared
 * {@code TYPE} (embedding / rerank / text): the common params (endpoint / model / api_key /
 * timeout_ms) apply to every type, and each type adds its own allowed keys. ALTER/DROP/SET DEFAULT
 * are type-agnostic (the provider is resolved by name), so they only need a name check — the manager
 * infers the type from the stored provider.
 */
public class AIProviderAnalyzer {

    private static final Set<String> COMMON_KEYS = Set.of(
            AIProvider.PROPERTY_ENDPOINT,
            AIProvider.PROPERTY_MODEL,
            AIProvider.PROPERTY_API_KEY,
            AIProvider.PROPERTY_TIMEOUT_MS);

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new Visitor().visit(stmt, session);
    }

    private static Set<String> allowedKeys(AIProviderType type) {
        TreeSet<String> keys = new TreeSet<>(COMMON_KEYS);
        switch (type) {
            case EMBEDDING:
                keys.add(AIProvider.PROPERTY_DIMENSIONS);
                break;
            case RERANK:
                keys.add(AIProvider.PROPERTY_MAX_DOCUMENTS);
                keys.add(AIProvider.PROPERTY_DEADLINE_MS);
                break;
            case TEXT:
            default:
                break;
        }
        return keys;
    }

    static class Visitor implements AstVisitor<Void, ConnectContext> {

        @Override
        public Void visitCreateAIProviderStatement(CreateAIProviderStmt statement, ConnectContext context) {
            requireName(statement.getName());
            AIProviderType type = parseType(statement.getType());
            Map<String, String> properties = statement.getProperties();
            validateKnownKeys(properties, type);
            requireProperty(properties, AIProvider.PROPERTY_ENDPOINT);
            requireProperty(properties, AIProvider.PROPERTY_MODEL);
            validateEndpoint(properties.get(AIProvider.PROPERTY_ENDPOINT));
            validatePositiveInt(properties, AIProvider.PROPERTY_TIMEOUT_MS);
            validatePositiveInt(properties, AIProvider.PROPERTY_DIMENSIONS);
            validatePositiveInt(properties, AIProvider.PROPERTY_MAX_DOCUMENTS);
            validatePositiveInt(properties, AIProvider.PROPERTY_DEADLINE_MS);
            return null;
        }

        @Override
        public Void visitAlterAIProviderStatement(AlterAIProviderStmt statement, ConnectContext context) {
            requireName(statement.getName());
            Map<String, String> properties = statement.getProperties();
            if (properties.isEmpty()) {
                throw new SemanticException("ALTER AI PROVIDER requires at least one property in SET (...)");
            }
            // Type-agnostic: we don't know the provider type at analyze time, so accept any known key
            // from any type's allowlist; the manager merges and the value semantics are enforced below.
            rejectEmptyIfPresent(properties, AIProvider.PROPERTY_ENDPOINT);
            rejectEmptyIfPresent(properties, AIProvider.PROPERTY_MODEL);
            if (properties.containsKey(AIProvider.PROPERTY_ENDPOINT)) {
                validateEndpoint(properties.get(AIProvider.PROPERTY_ENDPOINT));
            }
            validatePositiveInt(properties, AIProvider.PROPERTY_TIMEOUT_MS);
            validatePositiveInt(properties, AIProvider.PROPERTY_DIMENSIONS);
            validatePositiveInt(properties, AIProvider.PROPERTY_MAX_DOCUMENTS);
            validatePositiveInt(properties, AIProvider.PROPERTY_DEADLINE_MS);
            return null;
        }

        @Override
        public Void visitDropAIProviderStatement(DropAIProviderStmt statement, ConnectContext context) {
            requireName(statement.getName());
            return null;
        }

        @Override
        public Void visitSetDefaultAIProviderStatement(SetDefaultAIProviderStmt statement, ConnectContext context) {
            requireName(statement.getName());
            return null;
        }

        @Override
        public Void visitShowAIProvidersStatement(ShowAIProvidersStmt statement, ConnectContext context) {
            if (!Strings.isNullOrEmpty(statement.getTypeFilter())) {
                parseType(statement.getTypeFilter());
            }
            return null;
        }

        @Override
        public Void visitDescAIProviderStatement(DescAIProviderStmt statement, ConnectContext context) {
            requireName(statement.getName());
            return null;
        }

        private static AIProviderType parseType(String type) {
            try {
                return AIProviderType.fromString(type);
            } catch (IllegalArgumentException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        private static void requireName(String name) {
            if (Strings.isNullOrEmpty(name)) {
                throw new SemanticException("'ai provider name' can not be null or empty");
            }
        }

        private static void requireProperty(Map<String, String> properties, String key) {
            if (Strings.isNullOrEmpty(properties.get(key))) {
                throw new SemanticException("'" + key + "' is required in PROPERTIES");
            }
        }

        private static void validateKnownKeys(Map<String, String> properties, AIProviderType type) {
            Set<String> allowed = allowedKeys(type);
            for (String key : properties.keySet()) {
                if (!allowed.contains(key)) {
                    throw new SemanticException("Unknown property '" + key + "' for AI provider type "
                            + type.lower() + ". Allowed: " + allowed);
                }
            }
        }

        private static void rejectEmptyIfPresent(Map<String, String> properties, String key) {
            if (properties.containsKey(key) && Strings.isNullOrEmpty(properties.get(key))) {
                throw new SemanticException("'" + key + "' cannot be set to empty via ALTER; "
                        + "DROP the provider if you want to unset it");
            }
        }

        private static void validateEndpoint(String endpoint) {
            if (Strings.isNullOrEmpty(endpoint)) {
                return;
            }
            String lower = endpoint.toLowerCase();
            if (!lower.startsWith("http://") && !lower.startsWith("https://")) {
                throw new SemanticException("'endpoint' must start with http:// or https://, got: " + endpoint);
            }
        }

        private static void validatePositiveInt(Map<String, String> properties, String key) {
            String value = properties.get(key);
            if (Strings.isNullOrEmpty(value)) {
                return;
            }
            int parsed;
            try {
                parsed = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new SemanticException("'" + key + "' must be a positive integer, got: " + value);
            }
            if (parsed <= 0) {
                throw new SemanticException("'" + key + "' must be a positive integer, got: " + value);
            }
        }
    }
}
