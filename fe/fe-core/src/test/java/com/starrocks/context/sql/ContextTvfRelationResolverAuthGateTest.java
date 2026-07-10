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

package com.starrocks.context.sql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Source-level check that every TVF resolver entry in {@link ContextTvfRelationResolver}
 * routes through a {@code requireUsageOn*} gate before producing data.
 *
 * <p>The resolver runs at analyze time and emits a {@code SubqueryRelation} flagged
 * {@code createByPolicyRewritten=true}. {@code ColumnPrivilege.AccessControlChecker.visitSubqueryRelation}
 * short-circuits on that flag, and {@code AuthorizerStmtVisitor.visitTableFunction} only
 * collects {@code queryTable} catalog names — so the standard privilege walker never enforces
 * USAGE on the contextbase the TVF actually reads. The fix put a gate inside each
 * {@code resolveXxx}; a regression that removes one of those gates is a silent privilege
 * escalation. This test fails when any resolver is missing its gate so we catch the regression
 * before it ships.
 */
public class ContextTvfRelationResolverAuthGateTest {

    private static final String SOURCE_PATH =
            "src/main/java/com/starrocks/context/sql/ContextTvfRelationResolver.java";

    private static final String[] EXPECTED_RESOLVERS = new String[] {
            "resolveContextGet",
            "resolveEntityHistory",
            "resolveReadCollection",
            "resolveReadContextBase",
            "resolveTextSearch",
            "resolveVectorSearch",
            "resolveContextSearch",
            "resolveGraphExpand",
            "resolveContextPack",
    };

    private static final String[] GATE_TOKENS = new String[] {
            "requireUsageOnContextBase",
            "requireUsageOnContextBaseId",
            "requireUsageOnEntityId",
            "requireUsageOnCollectionId",
            // Indirect gate: this helper resolves the owning contextbase from the entity ids and
            // internally calls requireUsageOnContextBaseId (throwing ACCESS_DENIED), so a resolver
            // that gates through it (e.g. resolveGraphExpand, the id-based context_get path) is
            // still protected even though it carries no direct requireUsage* token.
            "resolveAuthorizedContextBaseIdForEntityIds",
    };

    @Test
    public void everyResolveXxxCallsAnAuthGate() throws IOException {
        String src = new String(Files.readAllBytes(Paths.get(SOURCE_PATH)));
        for (String method : EXPECTED_RESOLVERS) {
            int start = src.indexOf("private Relation " + method + "(");
            Assertions.assertTrue(start > 0, "could not find " + method);
            // Find the end of the method body — naive but adequate: search for the next
            // method declaration with `private` or end of file.
            int end = src.indexOf("\n    private ", start + 1);
            if (end < 0) {
                end = src.length();
            }
            String body = src.substring(start, end);
            boolean hasGate = false;
            for (String token : GATE_TOKENS) {
                if (body.contains(token)) {
                    hasGate = true;
                    break;
                }
            }
            Assertions.assertTrue(hasGate,
                    method + " must call one of requireUsage* before materializing data — "
                            + "without the gate the TVF bypasses CONTEXTBASE privilege checks");
        }
    }
}
