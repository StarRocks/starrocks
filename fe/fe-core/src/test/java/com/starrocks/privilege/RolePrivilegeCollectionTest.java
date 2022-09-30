// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import org.junit.Assert;
import org.junit.Test;

public class RolePrivilegeCollectionTest {
    @Test
    public void testFlags() throws Exception {
        RolePrivilegeCollection collection = new RolePrivilegeCollection("nolabel");
        Assert.assertFalse(collection.isDefault());
        Assert.assertFalse(collection.isMutable());
        Assert.assertFalse(collection.isRemovable());

        collection = new RolePrivilegeCollection(
                "public",
                RolePrivilegeCollection.RoleFlags.DEFAULT,
                RolePrivilegeCollection.RoleFlags.MUTABLE,
                RolePrivilegeCollection.RoleFlags.REMOVABLE);
        Assert.assertTrue(collection.isDefault());
        Assert.assertTrue(collection.isMutable());
        Assert.assertTrue(collection.isRemovable());

        collection = new RolePrivilegeCollection(
                "admin",
                RolePrivilegeCollection.RoleFlags.REMOVABLE);
        Assert.assertFalse(collection.isDefault());
        Assert.assertFalse(collection.isMutable());
        Assert.assertTrue(collection.isRemovable());
    }
}
