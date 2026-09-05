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

import java.util.List;

public class VirtualColumnRegistryTest {

    @Test
    public void testGetDefinition() {
        // Test case-insensitive lookup
        VirtualColumnDefinition def1 = VirtualColumnRegistry.getDefinition("_tablet_id_");
        Assertions.assertNotNull(def1);
        Assertions.assertEquals("_tablet_id_", def1.getName());

        // Case-insensitive
        VirtualColumnDefinition def2 = VirtualColumnRegistry.getDefinition("_TABLET_ID_");
        Assertions.assertNotNull(def2);
        Assertions.assertEquals(def1, def2);

        // Non-existent column
        VirtualColumnDefinition def3 = VirtualColumnRegistry.getDefinition("_nonexistent_");
        Assertions.assertNull(def3);
    }

    @Test
    public void testGetColumn() {
        // Test getting column instance
        Column col = VirtualColumnRegistry.getColumn("_tablet_id_");
        Assertions.assertNotNull(col);
        Assertions.assertEquals("_tablet_id_", col.getName());
        Assertions.assertTrue(col.isVirtual());

        // Test singleton - should return same instance
        Column col2 = VirtualColumnRegistry.getColumn("_tablet_id_");
        Assertions.assertSame(col, col2);

        // Non-existent column
        Column col3 = VirtualColumnRegistry.getColumn("_nonexistent_");
        Assertions.assertNull(col3);
    }

    @Test
    public void testGetAllDefinitions() {
        List<VirtualColumnDefinition> defs = VirtualColumnRegistry.getAllDefinitions();
        Assertions.assertNotNull(defs);
        Assertions.assertFalse(defs.isEmpty());

        // Should contain tablet_id
        Assertions.assertTrue(defs.stream().anyMatch(d -> d.getName().equals("_tablet_id_")));
    }

    @Test
    public void testGetAllColumns() {
        List<Column> columns = VirtualColumnRegistry.getAllColumns();
        Assertions.assertNotNull(columns);
        Assertions.assertFalse(columns.isEmpty());

        // Should contain tablet_id
        Assertions.assertTrue(columns.stream().anyMatch(c -> c.getName().equals("_tablet_id_")));

        // All should be virtual
        Assertions.assertTrue(columns.stream().allMatch(Column::isVirtual));
    }

    @Test
    public void testIsVirtualColumn() {
        Assertions.assertTrue(VirtualColumnRegistry.isVirtualColumn("_tablet_id_"));
        Assertions.assertTrue(VirtualColumnRegistry.isVirtualColumn("_TABLET_ID_")); // case insensitive
        Assertions.assertFalse(VirtualColumnRegistry.isVirtualColumn("_nonexistent_"));
        Assertions.assertFalse(VirtualColumnRegistry.isVirtualColumn("v1"));
    }

    @Test
    public void testGetCount() {
        int count = VirtualColumnRegistry.getCount();
        Assertions.assertTrue(count > 0);

        // Should match getAllColumns size
        Assertions.assertEquals(VirtualColumnRegistry.getAllColumns().size(), count);
    }

    @Test
    public void testVirtualColumnProperties() {
        Column col = VirtualColumnRegistry.getColumn("_tablet_id_");
        Assertions.assertNotNull(col);

        // Virtual columns should be marked as virtual
        Assertions.assertTrue(col.isVirtual());

        // Should have correct type
        Assertions.assertNotNull(col.getType());
    }
}
