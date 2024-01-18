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


package com.starrocks.authz.authorization;

import org.junit.Assert;
import org.junit.Test;

public class RolePrivilegeCollectionTest {
    @Test
    public void testFlags() throws Exception {
        RolePrivilegeCollectionV2 collection = new RolePrivilegeCollectionV2("nolabel");
        Assert.assertFalse(collection.isRemovable());
        try {
            collection.addParentRole(-1);
            Assert.fail();
        } catch (PrivilegeException e) {
            Assert.assertTrue(e.getMessage().contains("is not mutable"));
        }

        collection = new RolePrivilegeCollectionV2(
                "public",
                RolePrivilegeCollectionV2.RoleFlags.MUTABLE,
                RolePrivilegeCollectionV2.RoleFlags.REMOVABLE);
        Assert.assertTrue(collection.isRemovable());
        collection.addSubRole(-1);
        collection.disableMutable();
        try {
            collection.addParentRole(-1);
            Assert.fail();
        } catch (PrivilegeException e) {
            Assert.assertTrue(e.getMessage().contains("is not mutable"));
        }


        collection = new RolePrivilegeCollectionV2(
                "admin",
                RolePrivilegeCollectionV2.RoleFlags.REMOVABLE);
        Assert.assertTrue(collection.isRemovable());
        try {
            collection.addParentRole(-1);
            Assert.fail();
        } catch (PrivilegeException e) {
            Assert.assertTrue(e.getMessage().contains("is not mutable"));
        }
    }
}
