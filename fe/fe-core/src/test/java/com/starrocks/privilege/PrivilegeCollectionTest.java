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


package com.starrocks.privilege;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PrivilegeCollectionTest {

    @Test
    public void testBasic() throws Exception {
        PrivilegeCollection collection = new PrivilegeCollection();
        ObjectType table = ObjectType.TABLE;
        PrivilegeType select = PrivilegeType.SELECT;
        PrivilegeType insert = PrivilegeType.INSERT;
        PrivilegeType delete = PrivilegeType.DELETE;
        TablePEntryObject table1 = new TablePEntryObject("1", "2");
        ObjectType system = ObjectType.SYSTEM;

        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.check(table, delete, table1));

        // grant select on object1
        Assert.assertFalse(collection.searchAnyActionOnObject(table, table1));
        Assert.assertFalse(collection.check(table, select, table1));
        collection.grant(table, Arrays.asList(select), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.searchAnyActionOnObject(table, table1));

        // grant select, insert on object1
        Assert.assertFalse(collection.check(table, insert, table1));
        collection.grant(table, Arrays.asList(select, insert), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.check(table, insert, table1));

        // grant select, delete with grant option
        Assert.assertFalse(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(select, delete), Arrays.asList(table1)));
        collection.grant(table, Arrays.asList(select, delete), Arrays.asList(table1), true);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertTrue(collection.allowGrant(table, Arrays.asList(select, delete), Arrays.asList(table1)));
        Assert.assertTrue(collection.allowGrant(table, Arrays.asList(select), Arrays.asList(table1)));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(select, insert), Arrays.asList(table1)));

        // revoke select with grant option
        collection.revoke(table, Arrays.asList(select), Arrays.asList(table1));
        Assert.assertFalse(collection.check(table, select, table1));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(select), Arrays.asList(table1)));

        // revoke select, insert with grant option
        collection.revoke(table, Arrays.asList(select, insert), Arrays.asList(table1));
        Assert.assertFalse(collection.check(table, select, table1));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(select), Arrays.asList(table1)));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(insert), Arrays.asList(table1)));

        // revoke insert,delete
        Assert.assertTrue(collection.searchAnyActionOnObject(table, table1));
        collection.revoke(table, Arrays.asList(insert, delete), Arrays.asList(table1));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(delete, insert), Arrays.asList(table1)));
        Assert.assertFalse(collection.check(table, delete, table1));
        Assert.assertFalse(collection.searchAnyActionOnObject(table, table1));

        // nothing left
        Assert.assertEquals(0, collection.typeToPrivilegeEntryList.size());
        collection.revoke(table, Arrays.asList(insert, delete), Arrays.asList(table1));
    }

    @Test
    public void testGrantOptionComplicated() throws Exception {
        PrivilegeCollection collection = new PrivilegeCollection();
        ObjectType table = ObjectType.TABLE;
        PrivilegeType select = PrivilegeType.SELECT;
        PrivilegeType insert = PrivilegeType.INSERT;
        PrivilegeType delete = PrivilegeType.DELETE;
        TablePEntryObject table1 = new TablePEntryObject("1", "2");

        // grant select on table1 with grant option
        collection.grant(table, Arrays.asList(select), Arrays.asList(table1), true);
        // grant insert on table1 without grant option
        collection.grant(table, Arrays.asList(insert), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, Arrays.asList(select), Arrays.asList(table1)));
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertFalse(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(insert, delete), Arrays.asList(table1)));

        // grant delete on table1, without grant option
        collection.grant(table, Arrays.asList(delete), Arrays.asList(table1), false);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, Arrays.asList(select), Arrays.asList(table1)));
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(insert, delete), Arrays.asList(table1)));


        // grant insert on table1 with grant option
        collection.grant(table, Arrays.asList(insert), Arrays.asList(table1), true);
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertTrue(collection.allowGrant(table, Arrays.asList(select, insert), Arrays.asList(table1)));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(delete), Arrays.asList(table1)));

        // revoke insert on table1 without grant option
        collection.revoke(table, Arrays.asList(insert), Arrays.asList(table1));
        Assert.assertTrue(collection.check(table, select, table1));
        Assert.assertTrue(collection.allowGrant(table, Arrays.asList(select), Arrays.asList(table1)));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertTrue(collection.check(table, delete, table1));
        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(insert, delete), Arrays.asList(table1)));

        // revoke select,delete with grant option
        collection.revoke(table, Arrays.asList(select, delete), Arrays.asList(table1));
        Assert.assertFalse(collection.check(table, select, table1));
        Assert.assertFalse(collection.check(table, insert, table1));
        Assert.assertFalse(collection.allowGrant(
                table, Arrays.asList(select, insert, delete), Arrays.asList(table1)));
        Assert.assertFalse(collection.check(table, delete, table1));

        // nothing left
        Assert.assertEquals(0, collection.typeToPrivilegeEntryList.size());
    }

    @Test
    public void testAll() throws Exception {
        PrivilegeCollection collection = new PrivilegeCollection();
        ObjectType table = ObjectType.TABLE;
        PrivilegeType select = PrivilegeType.SELECT;
        PrivilegeType insert = PrivilegeType.INSERT;
        TablePEntryObject table1 = new TablePEntryObject("1", "2");
        TablePEntryObject allTablesInDb = new TablePEntryObject("1", TablePEntryObject.ALL_TABLES_UUID);
        TablePEntryObject allTablesInALLDb = new TablePEntryObject(
                TablePEntryObject.ALL_DATABASES_UUID, TablePEntryObject.ALL_TABLES_UUID);

        // grant select,insert on db1.table1
        collection.grant(table, Arrays.asList(select, insert), Arrays.asList(table1), false);
        Assert.assertEquals(1, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // grant select,insert on all tables in database db1
        collection.grant(table, Arrays.asList(select, insert), Arrays.asList(allTablesInDb), false);
        Assert.assertEquals(2, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // grant select,insert on all tables in all databases
        collection.grant(table, Arrays.asList(select, insert), Arrays.asList(allTablesInALLDb), false);
        Assert.assertEquals(3, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // revoke select on all tables in all databases
        collection.revoke(table, Arrays.asList(select), Arrays.asList(allTablesInALLDb));
        Assert.assertEquals(3, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // revoke insert on all tables in all databases
        collection.revoke(table, Arrays.asList(PrivilegeType.INSERT), Arrays.asList(allTablesInALLDb));
        // revoke select on all tables in database db1
        collection.revoke(table, Arrays.asList(PrivilegeType.SELECT), Arrays.asList(allTablesInDb));
        Assert.assertEquals(2, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertTrue(collection.check(table, select, table1));

        // revoke insert on all tables in database db1
        collection.revoke(table, Arrays.asList(PrivilegeType.INSERT), Arrays.asList(allTablesInDb));
        // revoke select on all tables in database db1
        collection.revoke(table, Arrays.asList(PrivilegeType.SELECT), Arrays.asList(table1));
        Assert.assertEquals(1, collection.typeToPrivilegeEntryList.get(table).size());
        Assert.assertFalse(collection.check(table, select, table1));

    }

    @Test
    public void testMergeCollection() throws Exception {
        ObjectType table = ObjectType.TABLE;
        PrivilegeType select = PrivilegeType.SELECT;
        PrivilegeType insert = PrivilegeType.INSERT;

        TablePEntryObject table1 = new TablePEntryObject("111", "222");
        ObjectType db = ObjectType.DATABASE;
        PrivilegeType drop = PrivilegeType.DROP;
        DbPEntryObject db1 = new DbPEntryObject("333");

        PrivilegeCollection collection = new PrivilegeCollection();
        PrivilegeCollection selectTable = new PrivilegeCollection();
        selectTable.grant(table, Arrays.asList(select), Arrays.asList(table1), false);

        Assert.assertFalse(collection.check(table, select, table1));
        collection.merge(selectTable);
        Assert.assertTrue(collection.check(table, select, table1));

        PrivilegeCollection insertTable = new PrivilegeCollection();
        insertTable.grant(table, Arrays.asList(insert), Arrays.asList(table1), false);

        Assert.assertFalse(collection.check(table, insert, table1));
        collection.merge(insertTable);
        Assert.assertTrue(collection.check(table, insert, table1));
        // make sure won't overlap previous collections..
        Assert.assertFalse(selectTable.check(table, insert, table1));

        // twice
        collection.merge(insertTable);
        Assert.assertTrue(collection.check(table, insert, table1));

        // with grant option
        PrivilegeCollection insertTableWithGrant = new PrivilegeCollection();
        insertTableWithGrant.grant(table, Arrays.asList(insert), Arrays.asList(table1), true);

        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(insert), Arrays.asList(table1)));
        collection.merge(insertTableWithGrant);
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertTrue(collection.allowGrant(table, Arrays.asList(insert), Arrays.asList(table1)));
        // make sure won't overlap previous collections..
        Assert.assertFalse(selectTable.check(table, insert, table1));
        Assert.assertFalse(insertTable.allowGrant(table, Arrays.asList(insert), Arrays.asList(table1)));

        PrivilegeCollection createTable = new PrivilegeCollection();
        createTable.grant(db, Arrays.asList(drop), Arrays.asList(db1), true);

        Assert.assertFalse(collection.allowGrant(db, Arrays.asList(drop), Arrays.asList(db1)));
        Assert.assertFalse(collection.check(db, drop, db1));
        collection.merge(createTable);
        Assert.assertTrue(collection.allowGrant(db, Arrays.asList(drop), Arrays.asList(db1)));
        Assert.assertTrue(collection.check(db, drop, db1));
    }

    @Test
    public void testPEntryCopyConstructor() throws Exception {
        PrivilegeType select = PrivilegeType.SELECT;
        PrivilegeType insert = PrivilegeType.INSERT;
        PrivilegeType delete = PrivilegeType.DELETE;
        TablePEntryObject table1 = new TablePEntryObject("111", "222");
        PrivilegeCollection.PrivilegeEntry entry = new PrivilegeCollection.PrivilegeEntry(
                new ActionSet(Arrays.asList(select, insert)),
                table1,
                false);
        PrivilegeCollection.PrivilegeEntry clonedEntry = new PrivilegeCollection.PrivilegeEntry(entry);

        entry.actionSet.add(new ActionSet(Arrays.asList(delete)));
        Assert.assertFalse(clonedEntry.actionSet.contains(delete));

        clonedEntry.actionSet.remove(new ActionSet(Arrays.asList(insert)));
        Assert.assertTrue(entry.actionSet.contains(insert));
    }
}
