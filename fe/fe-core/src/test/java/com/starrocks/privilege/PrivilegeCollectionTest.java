// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PrivilegeCollectionTest {

    @Test
    public void testBasic() throws Exception {
        PrivilegeCollection collection = new PrivilegeCollection();
        short table = 1;
        Action select = new Action((short) 1, "SELECT");
        Action insert = new Action((short) 2, "INSERT");
        Action delete = new Action((short) 3, "DELETE");
        TablePEntryObject table1 = new TablePEntryObject(1, 2);
        short system = 2;
        Action admin = new Action((short) 4, "ADMIN");

        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.check(table, delete, table1));

        // grant admin on system
        Assert.assertFalse(collection.check(system, admin, null));
        collection.grant(system, new ActionSet(Arrays.asList(admin)), null, false);
        Assert.assertTrue(collection.check(system, admin, null));

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
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(select, delete)), Arrays.asList(table1)));
        collection.grant(table, new ActionSet(Arrays.asList(select, delete)), Arrays.asList(table1), true);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertTrue(collection.allowGrant(table, new ActionSet(Arrays.asList(select, delete)), Arrays.asList(table1)));
        Assert.assertTrue(collection.allowGrant(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1)));
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(select, insert)), Arrays.asList(table1)));

        // revoke select with grant option
        collection.revoke(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1), true);
        Assert.assertFalse(collection.check(table, select, table1));
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1)));

        // revoke select, insert with grant option
        collection.revoke(table, new ActionSet(Arrays.asList(select, insert)), Arrays.asList(table1), true);
        Assert.assertFalse(collection.check(table, select, table1));
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1)));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(insert)), Arrays.asList(table1)));

        // revoke insert,delete
        Assert.assertTrue(collection.checkAnyObject(table, delete));
        Assert.assertTrue(collection.hasType(table));
        collection.revoke(table, new ActionSet(Arrays.asList(insert, delete)), Arrays.asList(table1), false);
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(delete, insert)), Arrays.asList(table1)));
        Assert.assertFalse(collection.check(table, delete, table1));
        Assert.assertFalse(collection.checkAnyObject(table, delete));

        // revoke system
        collection.revoke(system, new ActionSet(Arrays.asList(admin)), null, false);
        Assert.assertFalse(collection.check(system, admin, null));

        // nothing left
        Assert.assertFalse(collection.hasType(table));
        Assert.assertFalse(collection.hasType(system));
        collection.revoke(table, new ActionSet(Arrays.asList(insert, delete)), Arrays.asList(table1), false);
    }

    @Test
    public void testGrantOptionComplicated() throws Exception {
        PrivilegeCollection collection = new PrivilegeCollection();
        short table = 1;
        Action select = new Action((short) 1, "SELECT");
        Action insert = new Action((short) 2, "INSERT");
        Action delete = new Action((short) 3, "DELETE");
        TablePEntryObject table1 = new TablePEntryObject(1, 2);

        // grant select on table1 with grant option
        collection.grant(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1), true);
        // grant insert on table1 without grant option
        collection.grant(table, new ActionSet(Arrays.asList(insert)), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1)));
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertFalse(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(insert, delete)), Arrays.asList(table1)));

        // grant delete on table1, without grant option
        collection.grant(table, new ActionSet(Arrays.asList(delete)), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1)));
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(insert, delete)), Arrays.asList(table1)));


        // grant insert on table1 with grant option
        collection.grant(table, new ActionSet(Arrays.asList(insert)), Arrays.asList(table1), true);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertTrue(collection.allowGrant(table, new ActionSet(Arrays.asList(select, insert)), Arrays.asList(table1)));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(delete)), Arrays.asList(table1)));

        // revoke insert on table1 without grant option
        collection.revoke(table, new ActionSet(Arrays.asList(insert)), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, new ActionSet(Arrays.asList(select)), Arrays.asList(table1)));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, new ActionSet(Arrays.asList(insert, delete)), Arrays.asList(table1)));

        // revoke select,delete with grant option
        collection.revoke(table, new ActionSet(Arrays.asList(select, delete)), Arrays.asList(table1), true);
        Assert.assertFalse(collection.check(table, select, table1));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(
                table, new ActionSet(Arrays.asList(select, insert, delete)), Arrays.asList(table1)));
        Assert.assertFalse(collection.check(table, delete, table1));

        // nothing left
        Assert.assertFalse(collection.hasType(table));
    }

    @Test
    public void testAll() throws Exception {
        PrivilegeCollection collection = new PrivilegeCollection();
        short table = 1;
        Action select = new Action((short) 1, "SELECT");
        Action insert = new Action((short) 2, "INSERT");
        TablePEntryObject table1 = new TablePEntryObject(1, 2);
        TablePEntryObject allTablesInDb = new TablePEntryObject(1, TablePEntryObject.ALL_TABLES_ID);
        TablePEntryObject allTablesInALLDb = new TablePEntryObject(
                TablePEntryObject.ALL_DATABASE_ID, TablePEntryObject.ALL_TABLES_ID);

        ActionSet selectSet = new ActionSet(Arrays.asList(select));
        ActionSet insertSet = new ActionSet(Arrays.asList(insert));

        // grant select,insert on db1.table1
        collection.grant(table, new ActionSet(Arrays.asList(select, insert)), Arrays.asList(table1), false);
        Assert.assertEquals(1, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // grant select,insert on all tables in database db1
        collection.grant(table, new ActionSet(Arrays.asList(select, insert)), Arrays.asList(allTablesInDb), false);
        Assert.assertEquals(2, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // grant select,insert on all tables in all databases
        collection.grant(table, new ActionSet(Arrays.asList(select, insert)), Arrays.asList(allTablesInALLDb), false);
        Assert.assertEquals(3, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // revoke select on all tables in all databases
        collection.revoke(table, new ActionSet(Arrays.asList(select)), Arrays.asList(allTablesInALLDb), false);
        Assert.assertEquals(3, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // revoke insert on all tables in all databases
        collection.revoke(table, insertSet, Arrays.asList(allTablesInALLDb), false);
        // revoke select on all tables in database db1
        collection.revoke(table, selectSet, Arrays.asList(allTablesInDb), false);
        Assert.assertEquals(2, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // revoke insert on all tables in database db1
        collection.revoke(table, insertSet, Arrays.asList(allTablesInDb), false);
        // revoke select on all tables in database db1
        collection.revoke(table, selectSet, Arrays.asList(table1), false);
        Assert.assertEquals(1, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertFalse(collection.check(table, select, table1));

    }
}
