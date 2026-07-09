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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

/**
 * Pins that {@link AuthorizerStmtVisitor} overrides every {@code visitShowContext*} method.
 *
 * <p>The default visitor for these statements (in {@code AstVisitorExtendInterface}) forwards
 * to {@code visitShowStatement}, which performs no privilege check. Before this fix, that
 * meant any authenticated user could enumerate the entire semantic-context topology — every
 * contextbase, every collection, every workspace, the task queue, the embedding lag, and the
 * consistency state. A regression that drops one of the overrides would silently reopen the
 * topology-disclosure surface, so we lock in the override existence here.
 *
 * <p>End-to-end privilege validation (i.e. that the override actually denies non-admin users)
 * lives in the {@code test_semantic_context_scope_auth} SQL integration test; this class
 * exists to make a regression land at unit-test time instead of waiting for the SQL suite.
 */
public class AuthorizerStmtVisitorShowContextTest {

    private static final String[] EXPECTED_OVERRIDES = new String[] {
            "visitShowContextBasesStatement",
            "visitShowContextCollectionsStatement",
            "visitShowContextWorkspacesStatement",
            "visitShowContextStatusStatement",
            "visitShowContextTasksStatement",
            "visitShowContextProfileStatement",
    };

    @Test
    public void everyShowContextMethodIsOverridden() {
        Method[] declared = AuthorizerStmtVisitor.class.getDeclaredMethods();
        for (String name : EXPECTED_OVERRIDES) {
            boolean found = false;
            for (Method m : declared) {
                if (m.getName().equals(name)) {
                    found = true;
                    break;
                }
            }
            Assertions.assertTrue(found,
                    "AuthorizerStmtVisitor must declare " + name + " — the default in "
                            + "AstVisitorExtendInterface performs no privilege check and "
                            + "would silently allow topology enumeration");
        }
    }
}
