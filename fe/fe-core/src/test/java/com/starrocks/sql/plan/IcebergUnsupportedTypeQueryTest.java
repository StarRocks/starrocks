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

import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

/**
 * Verifies that querying Iceberg columns with unsupported V3 types
 * (e.g., geometry, timestamp_nano) fails at analysis time with a clear error,
 * while querying supported columns on the same table works fine.
 */
public class IcebergUnsupportedTypeQueryTest extends ConnectorPlanTestBase {
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(newFolder(temp, "junit").toURI().toString());
    }

    @Test
    public void testSelectUnsupportedColumnFails() {
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT geo_col FROM iceberg0.unpartitioned_db.t_unknown_types"));
        Assertions.assertTrue(ex.getMessage().contains("not supported"),
                "Expected 'not supported' error, got: " + ex.getMessage());
    }

    @Test
    public void testSelectAnotherUnsupportedColumnFails() {
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT ts_nano_col FROM iceberg0.unpartitioned_db.t_unknown_types"));
        Assertions.assertTrue(ex.getMessage().contains("not supported"),
                "Expected 'not supported' error, got: " + ex.getMessage());
    }

    @Test
    public void testSelectStarWithUnsupportedColumnFails() {
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT * FROM iceberg0.unpartitioned_db.t_unknown_types"));
        Assertions.assertTrue(ex.getMessage().contains("not supported"),
                "Expected 'not supported' error, got: " + ex.getMessage());
    }

}
