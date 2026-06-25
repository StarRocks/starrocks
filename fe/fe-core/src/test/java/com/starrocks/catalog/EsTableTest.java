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

package com.starrocks.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class EsTableTest {

    @Test
    public void testGetSupportedOperations() {
        EsTable table = new EsTable();
        Assertions.assertEquals(Set.of(TableOperation.READ, TableOperation.ALTER), table.getSupportedOperations());
    }

    @Test
    public void testSupportsOperation() {
        EsTable table = new EsTable();
        Assertions.assertTrue(table.supportsOperation(TableOperation.READ));
        Assertions.assertTrue(table.supportsOperation(TableOperation.ALTER));

        Assertions.assertFalse(table.supportsOperation(TableOperation.CREATE));
        Assertions.assertFalse(table.supportsOperation(TableOperation.INSERT));
        Assertions.assertFalse(table.supportsOperation(TableOperation.UPDATE));
        Assertions.assertFalse(table.supportsOperation(TableOperation.DELETE));
        Assertions.assertFalse(table.supportsOperation(TableOperation.DROP));
        Assertions.assertFalse(table.supportsOperation(TableOperation.CREATE_TABLE_LIKE));
    }
}
