// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import org.junit.Test;
import org.wildfly.common.Assert;

import java.util.Arrays;

public class PrivilegeCollectionTest {

    @Test
    public void testBasic() throws Exception {
        PrivilegeCollection collection = new PrivilegeCollection();
        short table = 1;
        Action select = new Action((short) 1, "SELECT");
        Action insert = new Action((short) 2, "INSERT");
        Action delete = new Action((short) 3, "DELETE");
        PEntryObject table1 = new PEntryObject(1);

        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.check(table, delete, table1));

        // grant select on object1
        Assert.assertFalse(collection.checkAnyObject(table, select));
        Assert.assertFalse(collection.hasType(table));
        Assert.assertFalse(collection.check(table, select, table1));
        collection.grant(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.checkAnyObject(table, select));
        Assert.assertTrue(collection.hasType(table));


        // grant select, insert on object1
        Assert.assertFalse(collection.check(table, insert, table1));
        collection.grant(table, new ActionSet(Arrays.asList(select, insert)), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.check(table, insert, table1));

        // grant select, delete with grant option
        Assert.assertFalse(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, select, table1));
        collection.grant(table, new ActionSet(Arrays.asList(select, delete)), Arrays.asList(table1), true);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertTrue(collection.allowGrant(table, delete, table1));
        Assert.assertTrue(collection.allowGrant(table, select, table1));

        // revoke select with grant option
        collection.revoke(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1), true);
        Assert.assertFalse(collection.check(table, select, table1));
        Assert.assertFalse(collection.allowGrant(table, select, table1));

        // revoke select, insert with grant option
        collection.revoke(table, new ActionSet(Arrays.asList(select, insert)), Arrays.asList(table1), true);
        Assert.assertFalse(collection.check(table, select, table1));
        Assert.assertFalse(collection.allowGrant(table, select, table1));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, insert, table1));

        // revoke insert,delete
        Assert.assertTrue(collection.checkAnyObject(table, delete));
        Assert.assertTrue(collection.hasType(table));
        collection.revoke(table, new ActionSet(Arrays.asList(insert, delete)), Arrays.asList(table1), false);
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, insert, table1));
        Assert.assertFalse(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, delete, table1));
        Assert.assertFalse(collection.checkAnyObject(table, delete));

        // nothing left
        Assert.assertFalse(collection.hasType(table));
        collection.revoke(table, new ActionSet(Arrays.asList(insert, delete)), Arrays.asList(table1), false);
    }

    @Test
    public void testGrantOptionComplicated() throws Exception {
        PrivilegeCollection collection = new PrivilegeCollection();
        short table = 1;
        Action select = new Action((short) 1, "SELECT");
        Action insert = new Action((short) 2, "INSERT");
        Action delete = new Action((short) 3, "DELETE");
        PEntryObject table1 = new PEntryObject(1);

        // grant select on table1 with grant option
        collection.grant(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1), true);
        // grant insert on table1 without grant option
        collection.grant(table, new ActionSet(Arrays.asList(insert)), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, select, table1));
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, insert, table1));
        Assert.assertFalse(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, delete, table1));

        // grant delete on table1, without grant option
        collection.grant(table, new ActionSet(Arrays.asList(delete)), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, select, table1));
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, insert, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, delete, table1));


        // grant insert on table1 with grant option
        collection.grant(table, new ActionSet(Arrays.asList(insert)), Arrays.asList(table1), true);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, select, table1));
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertTrue(collection.allowGrant(table, insert, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, delete, table1));

        // revoke insert on table1 without grant option
        collection.revoke(table, new ActionSet(Arrays.asList(insert)), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, select, table1));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, insert, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, delete, table1));

        // revoke select,delete with grant option
        collection.revoke(table, new ActionSet(Arrays.asList(select, delete)), Arrays.asList(table1), true);
        Assert.assertFalse(collection.check(table, select, table1));
        Assert.assertFalse(collection.allowGrant(table, select, table1));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, insert, table1));
        Assert.assertFalse(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, delete, table1));

        // nothing left
        Assert.assertFalse(collection.hasType(table));
    }
}
