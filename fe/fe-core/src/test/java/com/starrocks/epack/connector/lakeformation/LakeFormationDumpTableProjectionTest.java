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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.optimizer.dump.DumpTableProjection;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A query dump is written to a file and replayed elsewhere, so it is one more exit an unauthorized column
 * name can leave by. These assert on the two carriers a real dump was observed to use: the column name
 * lists, and the DDL text - where the leak arrives as a Glue parameter rather than as a column.
 */
public class LakeFormationDumpTableProjectionTest {

    private static final String UNAUTHORIZED_COLUMN = "ssn";

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", null, "us-west-2", "db", "t");

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @BeforeEach
    public void stubResourceLookup() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getResourceMgr().getResource(anyString);
                result = null;
                minTimes = 0;
            }
        };
    }

    private static HiveTable physicalTable() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", IntegerType.INT));
        schema.add(new Column("region", IntegerType.INT));
        schema.add(new Column(UNAUTHORIZED_COLUMN, IntegerType.INT));
        // Spelled the way a Spark-written Glue table carries the physical schema: this is the property the
        // dump was seen to leak through, and it is not a column, so narrowing the columns alone misses it.
        Map<String, String> properties = new HashMap<>();
        properties.put("hive.table.column.names", "id,region," + UNAUTHORIZED_COLUMN);
        properties.put("EXTERNAL", "TRUE");
        return HiveTable.builder()
                .setId(1L)
                .setTableName("t")
                .setCatalogName("lf")
                .setHiveDbName("db")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/db/t")
                .setCreateTime(1600000000L)
                .setFullSchema(schema)
                .setPartitionColumnNames(new ArrayList<>(List.of("region")))
                .setDataColumnNames(new ArrayList<>(List.of("id", UNAUTHORIZED_COLUMN)))
                .setProperties(properties)
                .setSerdeProperties(new HashMap<>(Map.of("field.delim", ",")))
                .setStorageFormat(HiveStorageFormat.PARQUET)
                .build();
    }

    private static LakeFormationHiveTable governedTable() {
        return LakeFormationHiveTable.of(physicalTable(),
                List.of(new Column("id", IntegerType.INT), new Column("region", IntegerType.INT)),
                IDENTITY, new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "test-attempt"));
    }

    /** The same call the dump serializer makes, so the assertion tracks the text that really gets written. */
    private static String dumpDdl(Table table) {
        List<String> rendered = new ArrayList<>();
        AstToStringBuilder.getDdlStmt(table, rendered, null, null, false, true);
        return rendered.get(0);
    }

    @Test
    public void testGovernedTableLosesUnauthorizedColumnNames() {
        Table dumped = new LakeFormationDumpTableProjection().forDump(governedTable());

        assertTrue(dumped instanceof HiveTable);
        HiveTable hive = (HiveTable) dumped;
        assertEquals(List.of("id"), hive.getDataColumnNames());
        assertEquals(List.of("region"), hive.getPartitionColumnNames());
        assertFalse(hive.getProperties().containsKey("hive.table.column.names"),
                "a Glue parameter carrying the physical schema must not survive into a dump");
    }

    @Test
    public void testGovernedTableDdlTextCarriesNoUnauthorizedColumn() {
        String before = dumpDdl(governedTable());
        String after = dumpDdl(new LakeFormationDumpTableProjection().forDump(governedTable()));

        // The control half matters: without it, "the name is absent" could just mean nothing was rendered.
        assertTrue(before.contains(UNAUTHORIZED_COLUMN),
                "the unprojected table is what leaks; if it stops leaking, this test no longer proves anything");
        assertFalse(after.contains(UNAUTHORIZED_COLUMN), after);
        assertTrue(after.contains("`id`"), after);
    }

    @Test
    public void testPlainTableIsRecordedAsItIs() {
        HiveTable plain = physicalTable();
        Table dumped = new LakeFormationDumpTableProjection().forDump(plain);

        // Identity, not an equal copy: a non governed table must reach the dump untouched.
        assertSame(plain, dumped);
        assertEquals(dumpDdl(plain), dumpDdl(dumped));
    }

    @Test
    public void testDefaultProjectionChangesNothing() {
        LakeFormationHiveTable governed = governedTable();
        assertSame(governed, new DumpTableProjection().forDump(governed));
    }
}
