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


package com.starrocks.authorization;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class UserPrivilegeCollectionTest {
    @Test
    public void testClone() throws Exception {
        // Create original collection with all properties set
        UserPrivilegeCollectionV2 original = new UserPrivilegeCollectionV2();
        
        // Set up roles
        original.grantRole(1L);
        original.grantRole(2L);
        original.grantRole(3L);
        
        // Set up default roles
        Set<Long> defaultRoles = new HashSet<>();
        defaultRoles.add(1L);
        defaultRoles.add(2L);
        original.setDefaultRoleIds(defaultRoles);
        
        // Grant some privileges
        TablePEntryObject table1 = new TablePEntryObject("db1", "table1");
        original.grant(
                ObjectType.TABLE,
                Arrays.asList(PrivilegeType.SELECT, PrivilegeType.INSERT),
                Arrays.asList(table1),
                false);
        
        // Clone the collection
        UserPrivilegeCollectionV2 cloned = original.clone();
        
        // Test 1: Verify all properties are consistent after clone
        Set<Long> originalRoles = original.getAllRoles();
        Set<Long> clonedRoles = cloned.getAllRoles();
        Assertions.assertEquals(originalRoles.size(), clonedRoles.size());
        Assertions.assertTrue(clonedRoles.containsAll(originalRoles));
        Assertions.assertTrue(originalRoles.containsAll(clonedRoles));
        
        Set<Long> originalDefaultRoles = original.getDefaultRoleIds();
        Set<Long> clonedDefaultRoles = cloned.getDefaultRoleIds();
        Assertions.assertEquals(originalDefaultRoles.size(), clonedDefaultRoles.size());
        Assertions.assertTrue(clonedDefaultRoles.containsAll(originalDefaultRoles));
        Assertions.assertTrue(originalDefaultRoles.containsAll(clonedDefaultRoles));
        
        // Verify privileges
        Assertions.assertTrue(cloned.check(ObjectType.TABLE, PrivilegeType.SELECT, table1));
        Assertions.assertTrue(cloned.check(ObjectType.TABLE, PrivilegeType.INSERT, table1));
        Assertions.assertEquals(
                original.getTypeToPrivilegeEntryList().size(),
                cloned.getTypeToPrivilegeEntryList().size());
        
        // Test 2: Verify modifying clone does not affect original
        // Modify clone's roles
        cloned.grantRole(4L);
        Assertions.assertFalse(original.getAllRoles().contains(4L));
        Assertions.assertTrue(cloned.getAllRoles().contains(4L));
        Assertions.assertEquals(3, original.getAllRoles().size());
        Assertions.assertEquals(4, cloned.getAllRoles().size());
        
        // Modify clone's default roles
        Set<Long> newDefaultRoles = new HashSet<>();
        newDefaultRoles.add(3L);
        newDefaultRoles.add(4L);
        cloned.setDefaultRoleIds(newDefaultRoles);
        Assertions.assertEquals(2, original.getDefaultRoleIds().size());
        Assertions.assertEquals(2, cloned.getDefaultRoleIds().size());
        Assertions.assertTrue(original.getDefaultRoleIds().contains(1L));
        Assertions.assertTrue(original.getDefaultRoleIds().contains(2L));
        Assertions.assertTrue(cloned.getDefaultRoleIds().contains(3L));
        Assertions.assertTrue(cloned.getDefaultRoleIds().contains(4L));
        Assertions.assertFalse(cloned.getDefaultRoleIds().contains(1L));
        
        // Modify clone's privileges
        TablePEntryObject table2 = new TablePEntryObject("db2", "table2");
        cloned.grant(
                ObjectType.TABLE,
                Arrays.asList(PrivilegeType.DELETE),
                Arrays.asList(table2),
                false);
        Assertions.assertFalse(original.check(ObjectType.TABLE, PrivilegeType.DELETE, table2));
        Assertions.assertTrue(cloned.check(ObjectType.TABLE, PrivilegeType.DELETE, table2));
        
        // Verify original's privileges are still intact
        Assertions.assertTrue(original.check(ObjectType.TABLE, PrivilegeType.SELECT, table1));
        Assertions.assertTrue(original.check(ObjectType.TABLE, PrivilegeType.INSERT, table1));
        Assertions.assertFalse(original.check(ObjectType.TABLE, PrivilegeType.DELETE, table2));
        
        // Verify clone's original privileges are still intact
        Assertions.assertTrue(cloned.check(ObjectType.TABLE, PrivilegeType.SELECT, table1));
        Assertions.assertTrue(cloned.check(ObjectType.TABLE, PrivilegeType.INSERT, table1));
        
        // Test revoke on clone should not affect original
        cloned.revokeRole(1L);
        Assertions.assertTrue(original.getAllRoles().contains(1L));
        Assertions.assertFalse(cloned.getAllRoles().contains(1L));
        Assertions.assertTrue(original.getDefaultRoleIds().contains(1L));
        Assertions.assertFalse(cloned.getDefaultRoleIds().contains(1L));
    }
}

