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

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PrivilegeCollectionTest {

    @Test
    public void testBasic() throws Exception {
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
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
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
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
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
        ObjectType table = ObjectType.TABLE;
        PrivilegeType select = PrivilegeType.SELECT;
        PrivilegeType insert = PrivilegeType.INSERT;
        TablePEntryObject table1 = new TablePEntryObject("1", "2");
        TablePEntryObject allTablesInDb = new TablePEntryObject("1", PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        TablePEntryObject allTablesInALLDb = new TablePEntryObject(
                PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);

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

        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
        PrivilegeCollectionV2 selectTable = new PrivilegeCollectionV2();
        selectTable.grant(table, Arrays.asList(select), Arrays.asList(table1), false);

        Assert.assertFalse(collection.check(table, select, table1));
        collection.merge(selectTable);
        Assert.assertTrue(collection.check(table, select, table1));

        PrivilegeCollectionV2 insertTable = new PrivilegeCollectionV2();
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
        PrivilegeCollectionV2 insertTableWithGrant = new PrivilegeCollectionV2();
        insertTableWithGrant.grant(table, Arrays.asList(insert), Arrays.asList(table1), true);

        Assert.assertFalse(collection.allowGrant(table, Arrays.asList(insert), Arrays.asList(table1)));
        collection.merge(insertTableWithGrant);
        Assert.assertTrue(collection.check(table, insert, table1));
        Assert.assertTrue(collection.allowGrant(table, Arrays.asList(insert), Arrays.asList(table1)));
        // make sure won't overlap previous collections..
        Assert.assertFalse(selectTable.check(table, insert, table1));
        Assert.assertFalse(insertTable.allowGrant(table, Arrays.asList(insert), Arrays.asList(table1)));

        PrivilegeCollectionV2 createTable = new PrivilegeCollectionV2();
        createTable.grant(db, Arrays.asList(drop), Arrays.asList(db1), true);

        Assert.assertFalse(collection.allowGrant(db, Arrays.asList(drop), Arrays.asList(db1)));
        Assert.assertFalse(collection.check(db, drop, db1));
        collection.merge(createTable);
        Assert.assertTrue(collection.allowGrant(db, Arrays.asList(drop), Arrays.asList(db1)));
        Assert.assertTrue(collection.check(db, drop, db1));

        PrivilegeCollectionV2 systemCollection1 = new PrivilegeCollectionV2();
        systemCollection1.grant(ObjectType.SYSTEM, Arrays.asList(PrivilegeType.NODE),
                Collections.singletonList(null), true);

        PrivilegeCollectionV2 systemCollection2 = new PrivilegeCollectionV2();
        systemCollection2.grant(ObjectType.SYSTEM, Arrays.asList(PrivilegeType.OPERATE),
                Collections.singletonList(null), false);
        systemCollection1.merge(systemCollection2);
    }

    @Test
    public void testPEntryCopyConstructor() throws Exception {
        PrivilegeType select = PrivilegeType.SELECT;
        PrivilegeType insert = PrivilegeType.INSERT;
        PrivilegeType delete = PrivilegeType.DELETE;
        TablePEntryObject table1 = new TablePEntryObject("111", "222");
        PrivilegeEntry entry = new PrivilegeEntry(
                new ActionSet(Arrays.asList(select, insert)),
                table1,
                false);
        PrivilegeEntry clonedEntry = new PrivilegeEntry(entry);

        entry.actionSet.add(new ActionSet(Arrays.asList(delete)));
        Assert.assertFalse(clonedEntry.actionSet.contains(delete));

        clonedEntry.actionSet.remove(new ActionSet(Arrays.asList(insert)));
        Assert.assertTrue(entry.actionSet.contains(insert));
    }

    @Test
    public void testPEntryCompatibilityForExternalTable() {
        PEntryObject table1 = new TablePEntryObject(101, "hive.db", "hive.db.tbl.123");
        PEntryObject table2 = new TablePEntryObject(101, "db", "tbl");
        Assert.assertTrue(table1.match(table2));
        Assert.assertEquals(table1, table2);
        Assert.assertEquals(table1.hashCode(), table2.hashCode());

        PEntryObject table3 = new TablePEntryObject(101, "hive.db", "hive.db.tbl.123");
        PEntryObject table4 = new TablePEntryObject(101, "db", "db");
        Assert.assertFalse(table3.match(table4));
        Assert.assertNotEquals(table3, table4);

        PEntryObject table5 = new TablePEntryObject(101, "hive.db", "hive.db.tbl.123");
        PEntryObject table6 = new TablePEntryObject(101, "hive", "tbl");
        Assert.assertFalse(table5.match(table6));
        Assert.assertNotEquals(table5, table6);

        PEntryObject table7 = new TablePEntryObject(101, "hive.db", "hive.db.tbl.123");
        PEntryObject table8 = new TablePEntryObject(101, "db", "123");
        Assert.assertFalse(table7.match(table8));
        Assert.assertNotEquals(table7, table8);

        PEntryObject table9 = new TablePEntryObject(101, "hive.db", "hive.db.tbl.123");
        PEntryObject table10 = new TablePEntryObject(101, "db", "hive");
        Assert.assertFalse(table9.match(table10));
        Assert.assertNotEquals(table9, table10);
    }

    @Test
    public void testPEntryCompatibilityForExternalDb() {
        PEntryObject db1 = new DbPEntryObject(101, "hive.db");
        PEntryObject db2 = new DbPEntryObject(101, "db");
        Assert.assertTrue(db1.match(db2));
        Assert.assertEquals(db1, db2);
        Assert.assertEquals(db1.hashCode(), db2.hashCode());

        PEntryObject db3 = new DbPEntryObject(101, "hive.db");
        PEntryObject db4 = new DbPEntryObject(101, "hive");
        Assert.assertFalse(db3.match(db4));
        Assert.assertNotEquals(db3, db4);
    }

    @Test
    public void testBasicForExternalTable() throws PrivilegeException {
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
        // pentry of external table in old format
        TablePEntryObject table1 = new TablePEntryObject(101, "hive.db", "hive.db.tbl.123");
        // pentry of external table in new format
        TablePEntryObject table2 = new TablePEntryObject(101, "db", "tbl");

        // test interoperability of old & new format
        List<PEntryObject> toGrantWithPEntryList = ImmutableList.of(table1, table2, table1, table2);
        List<PEntryObject> toRevokeWithPEntryList = ImmutableList.of(table1, table2, table2, table1);

        for (int i = 0; i < toGrantWithPEntryList.size(); i++) {
            PEntryObject toGrant = toGrantWithPEntryList.get(i);
            PEntryObject toRevoke = toRevokeWithPEntryList.get(i);
            Assert.assertFalse(collection.check(ObjectType.TABLE, PrivilegeType.SELECT, table1));
            Assert.assertFalse(collection.check(ObjectType.TABLE, PrivilegeType.SELECT, table2));
            collection.grant(ObjectType.TABLE, ImmutableList.of(PrivilegeType.SELECT), ImmutableList.of(toGrant), false);
            Assert.assertTrue(collection.check(ObjectType.TABLE, PrivilegeType.SELECT, table1));
            Assert.assertTrue(collection.check(ObjectType.TABLE, PrivilegeType.SELECT, table2));
            collection.revoke(ObjectType.TABLE, ImmutableList.of(PrivilegeType.SELECT), ImmutableList.of(toRevoke));
            Assert.assertEquals(0, collection.typeToPrivilegeEntryList.size());
        }
    }

    @Test
    public void testBasicForExternalDatabase() throws PrivilegeException {
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();

        // pentry of external database in old format
        DbPEntryObject db1 = new DbPEntryObject(101, "hive.db");
        // pentry of external database in new format
        DbPEntryObject db2 = new DbPEntryObject(101, "db");

        // test interoperability of old & new format
        List<PEntryObject> toGrantWithPEntryList = ImmutableList.of(db1, db2, db1, db2);
        List<PEntryObject> toRevokeWithPEntryList = ImmutableList.of(db1, db2, db2, db1);

        for (int i = 0; i < toGrantWithPEntryList.size(); i++) {
            PEntryObject toGrant = toGrantWithPEntryList.get(i);
            PEntryObject toRevoke = toRevokeWithPEntryList.get(i);
            Assert.assertFalse(collection.check(ObjectType.DATABASE, PrivilegeType.DROP, db1));
            Assert.assertFalse(collection.check(ObjectType.DATABASE, PrivilegeType.DROP, db2));
            collection.grant(ObjectType.DATABASE, ImmutableList.of(PrivilegeType.DROP), ImmutableList.of(toGrant), false);
            Assert.assertTrue(collection.check(ObjectType.DATABASE, PrivilegeType.DROP, db1));
            Assert.assertTrue(collection.check(ObjectType.DATABASE, PrivilegeType.DROP, db2));
            collection.revoke(ObjectType.DATABASE, ImmutableList.of(PrivilegeType.DROP), ImmutableList.of(toRevoke));
            Assert.assertEquals(0, collection.typeToPrivilegeEntryList.size());
        }
    }
}
