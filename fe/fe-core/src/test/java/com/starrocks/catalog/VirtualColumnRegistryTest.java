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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class VirtualColumnRegistryTest {
    
    @Test
    public void testGetDefinition() {
        // Test case-insensitive lookup
        VirtualColumnDefinition def1 = VirtualColumnRegistry.getDefinition("_tablet_id_");
        assertNotNull(def1);
        assertEquals("_tablet_id_", def1.getName());
        
        // Case insensitive
        VirtualColumnDefinition def2 = VirtualColumnRegistry.getDefinition("_TABLET_ID_");
        assertNotNull(def2);
        assertEquals(def1, def2);
        
        // Non-existent column
        VirtualColumnDefinition def3 = VirtualColumnRegistry.getDefinition("_nonexistent_");
        assertNull(def3);
    }
    
    @Test
    public void testGetColumn() {
        // Test getting column instance
        Column col = VirtualColumnRegistry.getColumn("_tablet_id_");
        assertNotNull(col);
        assertEquals("_tablet_id_", col.getName());
        assertTrue(col.isVirtual());
        
        // Test singleton - should return same instance
        Column col2 = VirtualColumnRegistry.getColumn("_tablet_id_");
        assertSame(col, col2);
        
        // Non-existent column
        Column col3 = VirtualColumnRegistry.getColumn("_nonexistent_");
        assertNull(col3);
    }
    
    @Test
    public void testGetAllDefinitions() {
        List<VirtualColumnDefinition> defs = VirtualColumnRegistry.getAllDefinitions();
        assertNotNull(defs);
        assertFalse(defs.isEmpty());
        
        // Should contain tablet_id
        assertTrue(defs.stream().anyMatch(d -> d.getName().equals("_tablet_id_")));
    }
    
    @Test
    public void testGetAllColumns() {
        List<Column> columns = VirtualColumnRegistry.getAllColumns();
        assertNotNull(columns);
        assertFalse(columns.isEmpty());
        
        // Should contain tablet_id
        assertTrue(columns.stream().anyMatch(c -> c.getName().equals("_tablet_id_")));
        
        // All should be virtual
        assertTrue(columns.stream().allMatch(Column::isVirtual));
    }
    
    @Test
    public void testIsVirtualColumn() {
        assertTrue(VirtualColumnRegistry.isVirtualColumn("_tablet_id_"));
        assertTrue(VirtualColumnRegistry.isVirtualColumn("_TABLET_ID_")); // case insensitive
        assertFalse(VirtualColumnRegistry.isVirtualColumn("_nonexistent_"));
        assertFalse(VirtualColumnRegistry.isVirtualColumn("v1"));
    }
    
    @Test
    public void testGetCount() {
        int count = VirtualColumnRegistry.getCount();
        assertTrue(count > 0);
        
        // Should match getAllColumns size
        assertEquals(VirtualColumnRegistry.getAllColumns().size(), count);
    }
    
    @Test
    public void testVirtualColumnProperties() {
        Column col = VirtualColumnRegistry.getColumn("_tablet_id_");
        assertNotNull(col);
        
        // Virtual columns should be marked as virtual
        assertTrue(col.isVirtual());
        
        // Should have correct type
        assertNotNull(col.getType());
    }
}
