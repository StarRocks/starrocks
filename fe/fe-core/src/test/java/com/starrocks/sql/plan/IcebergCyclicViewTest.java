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

package com.starrocks.sql.plan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

/**
 * Verifies that a cycle through Iceberg connector views (cyc_a -> cyc_b -> cyc_a) is detected and
 * reported as a graceful SemanticException instead of recursing forever into a StackOverflowError.
 *
 * <p>Connector views are rebuilt with a freshly generated id on every metadata lookup (see
 * IcebergApiConverter.toView), so keying cycle detection on the view id never observes the same id
 * twice and the guard would never fire. The fix keys connector views by their stable, fully
 * qualified name; this test would StackOverflow without it.
 */
public class IcebergCyclicViewTest extends ConnectorPlanTestBase {
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(newFolder(temp, "junit").toURI().toString());
    }

    @Test
    public void testCyclicIcebergViewRaisesSemanticErrorNotStackOverflow() {
        Throwable t = Assertions.assertThrows(Throwable.class,
                () -> getFragmentPlan("select * from iceberg0.view_db.cyc_a"));
        Assertions.assertFalse(t instanceof StackOverflowError,
                "cyclic connector view must not overflow the stack");
        Assertions.assertTrue(t.getMessage() != null && t.getMessage().contains("cycle"),
                "expected a graceful cycle semantic error, but got: " + t);
    }
}
