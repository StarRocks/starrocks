// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import org.junit.Assert;
import org.junit.Test;

public class RolePrivilegeCollectionTest {
    @Test
    public void testFlags() throws Exception {
        RolePrivilegeCollection collection = new RolePrivilegeCollection("nolabel");
        Assert.assertFalse(collection.isRemovable());
        try {
            collection.addSubRole(-1);
            Assert.fail();
        } catch (PrivilegeException e) {
            Assert.assertTrue(e.getMessage().contains("is not mutable"));
        }

        collection = new RolePrivilegeCollection(
                "public",
                RolePrivilegeCollection.RoleFlags.MUTABLE,
                RolePrivilegeCollection.RoleFlags.REMOVABLE);
        Assert.assertTrue(collection.isRemovable());
        collection.addSubRole(-1);
        collection.disableMutable();
        try {
            collection.addSubRole(-1);
            Assert.fail();
        } catch (PrivilegeException e) {
            Assert.assertTrue(e.getMessage().contains("is not mutable"));
        }


        collection = new RolePrivilegeCollection(
                "admin",
                RolePrivilegeCollection.RoleFlags.REMOVABLE);
        Assert.assertTrue(collection.isRemovable());
        try {
            collection.addSubRole(-1);
            Assert.fail();
        } catch (PrivilegeException e) {
            Assert.assertTrue(e.getMessage().contains("is not mutable"));
        }
    }
}
