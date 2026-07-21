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

package com.starrocks.epack.connector.iceberg;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IcebergDeletionVectorSupportTest extends TableTestBase {

    @Test
    public void testAssertV3DeleteSupportedPassesOnCleanTable() {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name",
                "iceberg_db", "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        // Single spec, no delete files -> the V3 DELETE gate must pass.
        Assertions.assertDoesNotThrow(() -> IcebergDeletionVectorSupport.assertV3DeleteSupported(icebergTable));
    }

    @Test
    public void testAssertV3DeleteSupportedRejectsExistingDeletes() {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        // A pre-existing position-delete file must trip the fail-fast gate (previous-delete
        // merge is not implemented yet, so proceeding would silently drop those deletes).
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_DELETES).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name",
                "iceberg_db", "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        StarRocksPlannerException ex = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> IcebergDeletionVectorSupport.assertV3DeleteSupported(icebergTable));
        Assertions.assertTrue(ex.getMessage().contains("pre-existing delete files"),
                "expected pre-existing-delete rejection, got: " + ex.getMessage());
    }
}
