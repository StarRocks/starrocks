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

import com.google.api.client.util.Lists;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ForeignKeyConstraintTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;
    Database db = new Database(100, "testDb");

    @BeforeEach
    public void beforeAll() {
        Table table1 = new Table(1000, "tbl1", Table.TableType.OLAP, Lists.newArrayList());
        Table table2 = new Table(1001, "tbl2", Table.TableType.OLAP, Lists.newArrayList());
        Table table3 = new Table(1002, "tbl3", Table.TableType.OLAP, Lists.newArrayList());
        db.registerTableUnlocked(table1);
        db.registerTableUnlocked(table2);
        db.registerTableUnlocked(table3);
    }

    @Test
    public void testParseInternal() {

        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getTable(anyLong, 1000L);
                minTimes = 0;
                result = db.getTable(1000L);

                globalStateMgr.getLocalMetastore().getTable(anyLong, 1001L);
                minTimes = 0;
                result = db.getTable(1001L);

                globalStateMgr.getLocalMetastore().getTable(anyLong, 1002L);
                minTimes = 0;
                result = db.getTable(1002L);
            }
        };

        // internal catalog
        String constraintDescs = "(column1)  REFERENCES  default_catalog.100.1000(newColumn1)";
        List<ForeignKeyConstraint> foreignKeyConstraints1 = ForeignKeyConstraint.parse(constraintDescs);
        Assertions.assertEquals(1, foreignKeyConstraints1.size());
        Assertions.assertEquals("default_catalog", foreignKeyConstraints1.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals(100, foreignKeyConstraints1.get(0).getParentTableInfo().getDbId());
        Assertions.assertEquals(1000, foreignKeyConstraints1.get(0).getParentTableInfo().getTableId());
        Assertions.assertEquals(1, foreignKeyConstraints1.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints1.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints1.get(0).getColumnRefPairs().get(0).second);

        String constraintDescs2 = "(column1, column2 )  REFERENCES default_catalog.100.1000(newColumn1, newColumn2)";
        List<ForeignKeyConstraint> foreignKeyConstraints2 = ForeignKeyConstraint.parse(constraintDescs2);
        Assertions.assertEquals(1, foreignKeyConstraints2.size());
        Assertions.assertEquals("default_catalog", foreignKeyConstraints2.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals(100, foreignKeyConstraints2.get(0).getParentTableInfo().getDbId());
        Assertions.assertEquals(1000, foreignKeyConstraints2.get(0).getParentTableInfo().getTableId());
        Assertions.assertEquals(2, foreignKeyConstraints2.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(0).second);
        Assertions.assertEquals(ColumnId.create("column2"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(1).first);
        Assertions.assertEquals(ColumnId.create("newColumn2"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(1).second);

        String constraintDescs3 = "(column1)  REFERENCES  default_catalog.100.1000    (newColumn1);" +
                " (column1, column2 )  REFERENCES default_catalog.101.1001(newColumn1, newColumn2);" +
                "  (column1, column2,column3)  REFERENCES default_catalog.102.1002(newColumn1, newColumn2, newColumn3)";
        List<ForeignKeyConstraint> foreignKeyConstraints3 = ForeignKeyConstraint.parse(constraintDescs3);
        Assertions.assertEquals(3, foreignKeyConstraints3.size());
        Assertions.assertEquals("default_catalog", foreignKeyConstraints3.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals(100, foreignKeyConstraints3.get(0).getParentTableInfo().getDbId());
        Assertions.assertEquals(1000, foreignKeyConstraints3.get(0).getParentTableInfo().getTableId());
        Assertions.assertEquals(1, foreignKeyConstraints3.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints3.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints3.get(0).getColumnRefPairs().get(0).second);

        Assertions.assertEquals("default_catalog", foreignKeyConstraints3.get(1).getParentTableInfo().getCatalogName());
        Assertions.assertEquals(101, foreignKeyConstraints3.get(1).getParentTableInfo().getDbId());
        Assertions.assertEquals(1001, foreignKeyConstraints3.get(1).getParentTableInfo().getTableId());
        Assertions.assertEquals(2, foreignKeyConstraints3.get(1).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints3.get(1).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints3.get(1).getColumnRefPairs().get(0).second);
        Assertions.assertEquals(ColumnId.create("column2"), foreignKeyConstraints3.get(1).getColumnRefPairs().get(1).first);
        Assertions.assertEquals(ColumnId.create("newColumn2"), foreignKeyConstraints3.get(1).getColumnRefPairs().get(1).second);

        Assertions.assertEquals("default_catalog", foreignKeyConstraints3.get(2).getParentTableInfo().getCatalogName());
        Assertions.assertEquals(102, foreignKeyConstraints3.get(2).getParentTableInfo().getDbId());
        Assertions.assertEquals(1002, foreignKeyConstraints3.get(2).getParentTableInfo().getTableId());
        Assertions.assertEquals(3, foreignKeyConstraints3.get(2).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(0).second);
        Assertions.assertEquals(ColumnId.create("column2"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(1).first);
        Assertions.assertEquals(ColumnId.create("newColumn2"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(1).second);
        Assertions.assertEquals(ColumnId.create("column3"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(2).first);
        Assertions.assertEquals(ColumnId.create("newColumn3"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(2).second);

        String constraintDescs4 = "(_column1)  REFERENCES  default_catalog.100.1000    (_newColumn1)";
        List<ForeignKeyConstraint> foreignKeyConstraints4 = ForeignKeyConstraint.parse(constraintDescs4);
        Assertions.assertEquals(1, foreignKeyConstraints4.size());
        Assertions.assertEquals("default_catalog", foreignKeyConstraints4.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals(100, foreignKeyConstraints4.get(0).getParentTableInfo().getDbId());
        Assertions.assertEquals(1000, foreignKeyConstraints4.get(0).getParentTableInfo().getTableId());
        Assertions.assertEquals(1, foreignKeyConstraints4.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("_column1"), foreignKeyConstraints4.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("_newColumn1"), foreignKeyConstraints4.get(0).getColumnRefPairs().get(0).second);

        String constraintDescs5 = "(_column1)  REFERENCES";
        List<ForeignKeyConstraint> foreignKeyConstraints5 = ForeignKeyConstraint.parse(constraintDescs5);
        Assertions.assertEquals(0, foreignKeyConstraints5.size());
    }

    @Test
    public void testParseExternal() {
        String constraintDescs = "(column1)  REFERENCES  catalog.db.tableName    (newColumn1)";
        List<ForeignKeyConstraint> foreignKeyConstraints1 = ForeignKeyConstraint.parse(constraintDescs);
        Assertions.assertEquals(1, foreignKeyConstraints1.size());
        Assertions.assertEquals("catalog", foreignKeyConstraints1.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints1.get(0).getParentTableInfo().getDbName());
        Assertions.assertEquals("tableName", foreignKeyConstraints1.get(0).getParentTableInfo().getTableName());
        Assertions.assertEquals(1, foreignKeyConstraints1.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints1.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints1.get(0).getColumnRefPairs().get(0).second);

        String constraintDescs2 = "(column1, column2 )  REFERENCES catalog.db.tableName(newColumn1, newColumn2)";
        List<ForeignKeyConstraint> foreignKeyConstraints2 = ForeignKeyConstraint.parse(constraintDescs2);
        Assertions.assertEquals(1, foreignKeyConstraints2.size());
        Assertions.assertEquals("catalog", foreignKeyConstraints2.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints2.get(0).getParentTableInfo().getDbName());
        Assertions.assertEquals("tableName", foreignKeyConstraints2.get(0).getParentTableInfo().getTableName());
        Assertions.assertEquals(2, foreignKeyConstraints2.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(0).second);
        Assertions.assertEquals(ColumnId.create("column2"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(1).first);
        Assertions.assertEquals(ColumnId.create("newColumn2"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(1).second);

        String constraintDescs3 = "(column1)  REFERENCES  catalog.db.tableName    (newColumn1);" +
                " (column1, column2 )  REFERENCES catalog.db2.tableName2(newColumn1, newColumn2);" +
                "  (column1, column2,column3)  REFERENCES catalog.db3.tableName3(newColumn1, newColumn2, newColumn3)";
        List<ForeignKeyConstraint> foreignKeyConstraints3 = ForeignKeyConstraint.parse(constraintDescs3);
        Assertions.assertEquals(3, foreignKeyConstraints3.size());
        Assertions.assertEquals("catalog", foreignKeyConstraints3.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints3.get(0).getParentTableInfo().getDbName());
        Assertions.assertEquals("tableName", foreignKeyConstraints3.get(0).getParentTableInfo().getTableName());
        Assertions.assertEquals(1, foreignKeyConstraints3.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints3.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints3.get(0).getColumnRefPairs().get(0).second);

        Assertions.assertEquals("catalog", foreignKeyConstraints3.get(1).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db2", foreignKeyConstraints3.get(1).getParentTableInfo().getDbName());
        Assertions.assertEquals("tableName2", foreignKeyConstraints3.get(1).getParentTableInfo().getTableName());
        Assertions.assertEquals(2, foreignKeyConstraints3.get(1).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints3.get(1).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints3.get(1).getColumnRefPairs().get(0).second);
        Assertions.assertEquals(ColumnId.create("column2"), foreignKeyConstraints3.get(1).getColumnRefPairs().get(1).first);
        Assertions.assertEquals(ColumnId.create("newColumn2"), foreignKeyConstraints3.get(1).getColumnRefPairs().get(1).second);

        Assertions.assertEquals("catalog", foreignKeyConstraints3.get(2).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db3", foreignKeyConstraints3.get(2).getParentTableInfo().getDbName());
        Assertions.assertEquals("tableName3", foreignKeyConstraints3.get(2).getParentTableInfo().getTableName());
        Assertions.assertEquals(3, foreignKeyConstraints3.get(2).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(0).second);
        Assertions.assertEquals(ColumnId.create("column2"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(1).first);
        Assertions.assertEquals(ColumnId.create("newColumn2"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(1).second);
        Assertions.assertEquals(ColumnId.create("column3"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(2).first);
        Assertions.assertEquals(ColumnId.create("newColumn3"), foreignKeyConstraints3.get(2).getColumnRefPairs().get(2).second);

        String constraintDescs4 = "(_column1)  REFERENCES  catalog.db.tableName    (_newColumn1)";
        List<ForeignKeyConstraint> foreignKeyConstraints4 = ForeignKeyConstraint.parse(constraintDescs4);
        Assertions.assertEquals(1, foreignKeyConstraints4.size());
        Assertions.assertEquals("catalog", foreignKeyConstraints4.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints4.get(0).getParentTableInfo().getDbName());
        Assertions.assertEquals("tableName", foreignKeyConstraints4.get(0).getParentTableInfo().getTableName());
        Assertions.assertEquals(1, foreignKeyConstraints4.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("_column1"), foreignKeyConstraints4.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("_newColumn1"), foreignKeyConstraints4.get(0).getColumnRefPairs().get(0).second);

        String constraintDescs5 = "(_column1)  REFERENCES catalog.db.tableName()";
        List<ForeignKeyConstraint> foreignKeyConstraints5 = ForeignKeyConstraint.parse(constraintDescs5);
        Assertions.assertEquals(0, foreignKeyConstraints5.size());
    }

    @Test
    public void testParseMV() {
        String constraintDescs = "catalog.db.table1(column1)  REFERENCES  catalog.db.table2(newColumn1)";
        List<ForeignKeyConstraint> foreignKeyConstraints1 = ForeignKeyConstraint.parse(constraintDescs);
        Assertions.assertEquals(1, foreignKeyConstraints1.size());
        Assertions.assertEquals("catalog", foreignKeyConstraints1.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints1.get(0).getParentTableInfo().getDbName());
        Assertions.assertEquals("table2", foreignKeyConstraints1.get(0).getParentTableInfo().getTableName());
        Assertions.assertEquals("catalog", foreignKeyConstraints1.get(0).getChildTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints1.get(0).getChildTableInfo().getDbName());
        Assertions.assertEquals("table1", foreignKeyConstraints1.get(0).getChildTableInfo().getTableName());
        Assertions.assertEquals(1, foreignKeyConstraints1.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints1.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints1.get(0).getColumnRefPairs().get(0).second);

        String constraintDescs2 = "catalog.db.table1(column1, column2)  REFERENCES  catalog.db.table2(newColumn1, newColumn2)";
        List<ForeignKeyConstraint> foreignKeyConstraints2 = ForeignKeyConstraint.parse(constraintDescs2);
        Assertions.assertEquals(1, foreignKeyConstraints2.size());
        Assertions.assertEquals("catalog", foreignKeyConstraints2.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints2.get(0).getParentTableInfo().getDbName());
        Assertions.assertEquals("table2", foreignKeyConstraints2.get(0).getParentTableInfo().getTableName());
        Assertions.assertEquals("catalog", foreignKeyConstraints2.get(0).getChildTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints2.get(0).getChildTableInfo().getDbName());
        Assertions.assertEquals("table1", foreignKeyConstraints2.get(0).getChildTableInfo().getTableName());
        Assertions.assertEquals(2, foreignKeyConstraints2.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("column1"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("newColumn1"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(0).second);
        Assertions.assertEquals(ColumnId.create("column2"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(1).first);
        Assertions.assertEquals(ColumnId.create("newColumn2"), foreignKeyConstraints2.get(0).getColumnRefPairs().get(1).second);

        String constraintDescs3 = "catalog.db.table1(column1)  REFERENCES catalog.db.table2(newColumn1);" +
                " catalog.db.table3(column1, column2 )  REFERENCES catalog.db.table4(newColumn1, newColumn2);" +
                " catalog.db.table5(column1, column2,column3)  REFERENCES catalog.db.table6(newColumn1, newColumn2, newColumn3)";
        List<ForeignKeyConstraint> foreignKeyConstraints3 = ForeignKeyConstraint.parse(constraintDescs3);
        Assertions.assertEquals(3, foreignKeyConstraints3.size());
        Assertions.assertEquals("catalog", foreignKeyConstraints3.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints3.get(0).getParentTableInfo().getDbName());
        Assertions.assertEquals("table2", foreignKeyConstraints3.get(0).getParentTableInfo().getTableName());
        Assertions.assertEquals("catalog", foreignKeyConstraints3.get(0).getChildTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints3.get(0).getChildTableInfo().getDbName());
        Assertions.assertEquals("table1", foreignKeyConstraints3.get(0).getChildTableInfo().getTableName());

        Assertions.assertEquals("catalog", foreignKeyConstraints3.get(1).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints3.get(1).getParentTableInfo().getDbName());
        Assertions.assertEquals("table4", foreignKeyConstraints3.get(1).getParentTableInfo().getTableName());
        Assertions.assertEquals("catalog", foreignKeyConstraints3.get(1).getChildTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints3.get(1).getChildTableInfo().getDbName());
        Assertions.assertEquals("table3", foreignKeyConstraints3.get(1).getChildTableInfo().getTableName());

        Assertions.assertEquals("catalog", foreignKeyConstraints3.get(2).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints3.get(2).getParentTableInfo().getDbName());
        Assertions.assertEquals("table6", foreignKeyConstraints3.get(2).getParentTableInfo().getTableName());
        Assertions.assertEquals("catalog", foreignKeyConstraints3.get(2).getChildTableInfo().getCatalogName());
        Assertions.assertEquals("db", foreignKeyConstraints3.get(2).getChildTableInfo().getDbName());
        Assertions.assertEquals("table5", foreignKeyConstraints3.get(2).getChildTableInfo().getTableName());

        Assertions.assertEquals(1, foreignKeyConstraints3.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(2, foreignKeyConstraints3.get(1).getColumnRefPairs().size());
        Assertions.assertEquals(3, foreignKeyConstraints3.get(2).getColumnRefPairs().size());

        String constraintDescs4 = "hive_catalog.hive_ssb_1g_csv.lineorder:1643182323(lo_custkey) " +
                "REFERENCES hive_catalog.hive_ssb_1g_csv.customer:1643182415(c_custkey)";
        List<ForeignKeyConstraint> foreignKeyConstraints4 = ForeignKeyConstraint.parse(constraintDescs4);
        Assertions.assertEquals(1, foreignKeyConstraints4.size());
        Assertions.assertEquals("hive_catalog", foreignKeyConstraints4.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("hive_ssb_1g_csv", foreignKeyConstraints4.get(0).getParentTableInfo().getDbName());
        Assertions.assertEquals("customer", foreignKeyConstraints4.get(0).getParentTableInfo().getTableName());
        Assertions.assertEquals("hive_catalog", foreignKeyConstraints4.get(0).getChildTableInfo().getCatalogName());
        Assertions.assertEquals("hive_ssb_1g_csv", foreignKeyConstraints4.get(0).getChildTableInfo().getDbName());
        Assertions.assertEquals("lineorder", foreignKeyConstraints4.get(0).getChildTableInfo().getTableName());
    }

    @Test
    public void testParseExternalCatalog2() {
        String constraintDescs = "iceberg_catalog_c22c9c10_35ad_11ee_abf5_c9d28a9636a8.iceberg_ssb_1g_orc_lz4" +
                ".lineorder:7920f06f-df49-472f-9662-97ac5c32da96(lo_custkey) REFERENCES " +
                "iceberg_catalog_c22c9c10_35ad_11ee_abf5_c9d28a9636a8.iceberg_ssb_1g_orc_lz4.customer:c3eb9381-3d18-4" +
                "60a-9995-de97a0e0afd3(c_custkey);iceberg_catalog_c22c9c10_35ad_11ee_abf5_c9d28a9636a8" +
                ".iceberg_ssb_1g_orc_lz4.lineorder:7920f06f-df49-472f-9662-97ac5c32da96(lo_partkey) REFERENCES " +
                "iceberg_catalog_c22c9c10_35ad_11ee_abf5_c9d28a9636a8.iceberg_ssb_1g_orc_lz4" +
                ".part:1e085393-64c7-4380-a4bb-109623ff9466(p_partkey);iceberg_catalog_c22c9c10_35ad_11ee_abf5_c9d28a" +
                "9636a8.iceberg_ssb_1g_orc_lz4.lineorder:7920f06f-df49-472f-9662-97ac5c32da96(lo_suppkey) " +
                "REFERENCES iceberg_catalog_c22c9c10_35ad_11ee_abf5_c9d28a9636a8.iceberg_ssb_1g_orc_lz4" +
                ".supplier:f217f38c-25e9-48ce-9094-39444cc60606(s_suppkey);" +
                "iceberg_catalog_c22c9c10_35ad_11ee_abf5_c9d28a9636a8.iceberg_ssb_1g_orc_lz4" +
                ".lineorder:7920f06f-df49-472f-9662-97ac5c32da9" +
                "6(lo_orderdate) REFERENCES iceberg_catalog_c22c9c10_35ad_11ee_abf5_c9d28a9636a8" +
                ".iceberg_ssb_1g_orc_lz4.dates:cfe2fcdf-e8c6-4835-9e4b-c470ea73977b(d_datekey)";
        List<ForeignKeyConstraint> foreignKeyConstraints1 = ForeignKeyConstraint.parse(constraintDescs);
        Assertions.assertEquals(4, foreignKeyConstraints1.size());

        Assertions.assertEquals("iceberg_catalog_c22c9c10_35ad_11ee_abf5_c9d28a9636a8",
                foreignKeyConstraints1.get(0).getParentTableInfo().getCatalogName());
        Assertions.assertEquals("iceberg_ssb_1g_orc_lz4", foreignKeyConstraints1.get(0).getParentTableInfo().getDbName());
        Assertions.assertEquals("customer:c3eb9381-3d18-460a-9995-de97a0e0afd3",
                foreignKeyConstraints1.get(0).getParentTableInfo().getTableIdentifier());
        Assertions.assertEquals("customer", foreignKeyConstraints1.get(0).getParentTableInfo().getTableName());

        Assertions.assertEquals(1, foreignKeyConstraints1.get(0).getColumnRefPairs().size());
        Assertions.assertEquals(ColumnId.create("lo_custkey"), foreignKeyConstraints1.get(0).getColumnRefPairs().get(0).first);
        Assertions.assertEquals(ColumnId.create("c_custkey"), foreignKeyConstraints1.get(0).getColumnRefPairs().get(0).second);
    }

    @Test
    public void testFKGetShowCreateTableConstraintDesc() {
        Assertions.assertEquals("", ForeignKeyConstraint.getShowCreateTableConstraintDesc(null, null));
        Assertions.assertEquals("", UniqueConstraint.getShowCreateTableConstraintDesc(null, null));
    }
}
