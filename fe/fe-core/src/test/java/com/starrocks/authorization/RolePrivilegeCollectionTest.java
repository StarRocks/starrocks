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
import java.util.Set;

public class RolePrivilegeCollectionTest {
    @Test
    public void testFlags() throws Exception {
        RolePrivilegeCollectionV2 collection = new RolePrivilegeCollectionV2("nolabel");
        Assertions.assertFalse(collection.isRemovable());
        try {
            collection.addParentRole(-1);
            Assertions.fail();
        } catch (PrivilegeException e) {
            Assertions.assertTrue(e.getMessage().contains("is not mutable"));
        }

        collection = new RolePrivilegeCollectionV2(
                "public",
                RolePrivilegeCollectionV2.RoleFlags.MUTABLE,
                RolePrivilegeCollectionV2.RoleFlags.REMOVABLE);
        Assertions.assertTrue(collection.isRemovable());
        collection.addSubRole(-1);
        collection.disableMutable();
        try {
            collection.addParentRole(-1);
            Assertions.fail();
        } catch (PrivilegeException e) {
            Assertions.assertTrue(e.getMessage().contains("is not mutable"));
        }


        collection = new RolePrivilegeCollectionV2(
                "admin",
                RolePrivilegeCollectionV2.RoleFlags.REMOVABLE);
        Assertions.assertTrue(collection.isRemovable());
        try {
            collection.addParentRole(-1);
            Assertions.fail();
        } catch (PrivilegeException e) {
            Assertions.assertTrue(e.getMessage().contains("is not mutable"));
        }
    }

    @Test
    public void testClone() throws Exception {
        // Create original collection with all properties set
        RolePrivilegeCollectionV2 original = new RolePrivilegeCollectionV2(
                "test_role",
                "test comment",
                RolePrivilegeCollectionV2.RoleFlags.MUTABLE,
                RolePrivilegeCollectionV2.RoleFlags.REMOVABLE);
        
        // Set up parent and sub roles
        original.addParentRole(1L);
        original.addParentRole(2L);
        original.addSubRole(3L);
        original.addSubRole(4L);
        
        // Grant some privileges
        TablePEntryObject table1 = new TablePEntryObject("db1", "table1");
        original.grantWithoutAssertMutable(
                ObjectType.TABLE,
                Arrays.asList(PrivilegeType.SELECT, PrivilegeType.INSERT),
                Arrays.asList(table1),
                false);
        
        // Clone the collection
        RolePrivilegeCollectionV2 cloned = original.clone();
        
        // Test 1: Verify all properties are consistent after clone
        Assertions.assertEquals(original.getName(), cloned.getName());
        Assertions.assertEquals(original.getComment(), cloned.getComment());
        Assertions.assertEquals(original.isMutable(), cloned.isMutable());
        Assertions.assertEquals(original.isRemovable(), cloned.isRemovable());
        
        Set<Long> originalParentRoles = original.getParentRoleIds();
        Set<Long> clonedParentRoles = cloned.getParentRoleIds();
        Assertions.assertEquals(originalParentRoles.size(), clonedParentRoles.size());
        Assertions.assertTrue(clonedParentRoles.containsAll(originalParentRoles));
        
        Set<Long> originalSubRoles = original.getSubRoleIds();
        Set<Long> clonedSubRoles = cloned.getSubRoleIds();
        Assertions.assertEquals(originalSubRoles.size(), clonedSubRoles.size());
        Assertions.assertTrue(clonedSubRoles.containsAll(originalSubRoles));
        
        // Verify privileges
        Assertions.assertTrue(cloned.check(ObjectType.TABLE, PrivilegeType.SELECT, table1));
        Assertions.assertTrue(cloned.check(ObjectType.TABLE, PrivilegeType.INSERT, table1));
        Assertions.assertEquals(
                original.getTypeToPrivilegeEntryList().size(),
                cloned.getTypeToPrivilegeEntryList().size());
        
        // Test 2: Verify modifying clone does not affect original
        // Modify clone's name (should not affect original since String is immutable)
        cloned.setComment("modified comment");
        Assertions.assertNotEquals(original.getComment(), cloned.getComment());
        Assertions.assertEquals("test comment", original.getComment());
        Assertions.assertEquals("modified comment", cloned.getComment());
        
        // Modify clone's parent roles
        cloned.addParentRole(5L);
        Assertions.assertFalse(original.getParentRoleIds().contains(5L));
        Assertions.assertTrue(cloned.getParentRoleIds().contains(5L));
        Assertions.assertEquals(2, original.getParentRoleIds().size());
        Assertions.assertEquals(3, cloned.getParentRoleIds().size());
        
        // Modify clone's sub roles
        cloned.addSubRole(6L);
        Assertions.assertFalse(original.getSubRoleIds().contains(6L));
        Assertions.assertTrue(cloned.getSubRoleIds().contains(6L));
        Assertions.assertEquals(2, original.getSubRoleIds().size());
        Assertions.assertEquals(3, cloned.getSubRoleIds().size());
        
        // Modify clone's privileges
        TablePEntryObject table2 = new TablePEntryObject("db2", "table2");
        cloned.grantWithoutAssertMutable(
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
        
        // Test removeParentRole on clone should not affect original
        cloned.removeParentRole(1L);
        Assertions.assertTrue(original.getParentRoleIds().contains(1L));
        Assertions.assertFalse(cloned.getParentRoleIds().contains(1L));
        
        // Test removeSubRole on clone should not affect original
        cloned.removeSubRole(3L);
        Assertions.assertTrue(original.getSubRoleIds().contains(3L));
        Assertions.assertFalse(cloned.getSubRoleIds().contains(3L));
        
        // Test disableMutable on clone should not affect original
        cloned.disableMutable();
        Assertions.assertTrue(original.isMutable());
        Assertions.assertFalse(cloned.isMutable());
    }
}
