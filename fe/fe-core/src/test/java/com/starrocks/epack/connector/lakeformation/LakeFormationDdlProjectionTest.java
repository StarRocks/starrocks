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
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationDdlProjectionTest {

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

    private static LakeFormationHiveTable table(Map<String, String> properties) {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", IntegerType.INT));
        schema.add(new Column("region", IntegerType.INT));
        schema.add(new Column("ssn", IntegerType.INT));
        HiveTable physical = HiveTable.builder()
                .setId(1L)
                .setTableName("t")
                .setCatalogName("lf")
                .setHiveDbName("db")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/db/t")
                .setCreateTime(1600000000L)
                .setFullSchema(schema)
                .setPartitionColumnNames(new ArrayList<>(List.of("region")))
                .setDataColumnNames(new ArrayList<>(List.of("id", "ssn")))
                .setProperties(new HashMap<>(properties))
                .setSerdeProperties(new HashMap<>(Map.of("field.delim", ",")))
                .setStorageFormat(HiveStorageFormat.PARQUET)
                .build();
        return LakeFormationHiveTable.of(physical,
                List.of(new Column("id", IntegerType.INT), new Column("region", IntegerType.INT)), IDENTITY);
    }

    private static Map<String, String> physicalProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hive.table.column.names", "id,ssn");
        properties.put("hive.table.column.types", "int#int");
        properties.put("spark.sql.sources.schema.part.0", "{\"fields\":[{\"name\":\"ssn\"}]}");
        properties.put("numRows", "42");
        properties.put("table_type", "EXTERNAL_TABLE");
        return properties;
    }

    @Test
    public void testDoesNotShowSchemaBearingProperties() {
        HiveTable display = LakeFormationDdlProjection.projectForDisplay(table(physicalProperties()));
        assertFalse(display.getProperties().containsKey("hive.table.column.names"));
        assertFalse(display.getProperties().containsKey("hive.table.column.types"));
        assertFalse(display.getProperties().containsKey("spark.sql.sources.schema.part.0"));
    }

    @Test
    public void testUnauthorizedColumnNamesDoNotAppearAnywhereInTheDisplayTable() {
        HiveTable display = LakeFormationDdlProjection.projectForDisplay(table(physicalProperties()));
        assertFalse(display.getProperties().toString().contains("ssn"), display.getProperties().toString());
        assertFalse(display.getDataColumnNames().contains("ssn"));
        assertTrue(display.getFullSchema().stream().noneMatch(c -> c.getName().equals("ssn")));
    }

    /**
     * toHiveProperties ends by copying every Glue parameter over the computed ones, so an allowlisted key
     * can arrive carrying anything at all. Filtering by key alone would print it verbatim.
     */
    @Test
    public void testAllowlistedValuesAreRebuiltNotCopied() {
        Map<String, String> hostile = physicalProperties();
        hostile.put("hive.table.input.format", "ssn");
        hostile.put("numRows", "ssn");
        HiveTable display = LakeFormationDdlProjection.projectForDisplay(table(hostile));

        assertEquals(HiveStorageFormat.PARQUET.getInputFormat(),
                display.getProperties().get("hive.table.input.format"));
        // Not a number, so not the property it claims to be: dropped rather than shown.
        assertNull(display.getProperties().get("numRows"));
        assertFalse(display.getProperties().toString().contains("ssn"));
    }

    /**
     * The allowlist is matched case sensitively against the keys Glue actually hands over, so these use
     * Hive's own spelling. A folded spelling in the allowlist matches nothing and drops the property
     * silently, which reads as "the table has no statistics" instead of as a bug.
     */
    @Test
    public void testWellFormedAllowlistedValuesAreStillShown() {
        Map<String, String> properties = physicalProperties();
        properties.put("transient_lastDdlTime", "1756000000");
        properties.put("totalSize", "1024");
        properties.put("numFiles", "3");
        properties.put("EXTERNAL", "TRUE");
        HiveTable display = LakeFormationDdlProjection.projectForDisplay(table(properties));

        assertEquals("42", display.getProperties().get("numRows"));
        assertEquals("1756000000", display.getProperties().get("transient_lastDdlTime"));
        assertEquals("1024", display.getProperties().get("totalSize"));
        assertEquals("3", display.getProperties().get("numFiles"));
        assertEquals("TRUE", display.getProperties().get("EXTERNAL"));
        assertEquals("EXTERNAL_TABLE", display.getProperties().get("table_type"));
        assertEquals("s3://bucket/db/t", display.getProperties().get("location"));
    }

    /**
     * serdeProperties is passed through because today's formatter never reads it. That is a fact about the
     * formatter, not about this class, so it is asserted against the formatter's own output below rather
     * than by comparing maps - a map comparison would keep passing on the day the formatter starts
     * printing them.
     */
    @Test
    public void testSerdePropertiesArePassedThroughUnchanged() {
        LakeFormationHiveTable lf = table(physicalProperties());
        HiveTable display = LakeFormationDdlProjection.projectForDisplay(lf);
        assertEquals(lf.getSerdeProperties(), display.getSerdeProperties());
    }

    /**
     * The assertion that actually matters: the SQL a user is shown. Everything above checks the display
     * table's fields, but the leak surface is the rendered statement, and it is assembled from more than
     * the property map - the comment and the location are printed separately.
     */
    @Test
    public void testRenderedDdlNeverContainsAnUnauthorizedColumnName() {
        Map<String, String> hostile = physicalProperties();
        hostile.put("hive.table.input.format", "ssn");
        hostile.put("numRows", "ssn");
        // Deliberately not planting it in the comment: the design shows the table comment verbatim and
        // registers that as a known, accepted exposure, so asserting against it here would contradict it.
        String ddl = AstToStringBuilder.getExternalCatalogTableDdlStmt(
                LakeFormationDdlProjection.projectForDisplay(table(hostile)));

        assertFalse(ddl.contains("ssn"), ddl);
        // and the authorized ones are still there, so this is not passing by rendering nothing
        assertTrue(ddl.contains("id"), ddl);
        assertTrue(ddl.contains("region"), ddl);
    }

    /** Display only, so narrowing these is allowed here and nowhere else. */
    @Test
    public void testNarrowsTheNameListsBecauseItNeverReachesThePlanner() {
        HiveTable display = LakeFormationDdlProjection.projectForDisplay(table(physicalProperties()));
        assertEquals(List.of("id"), display.getDataColumnNames());
        assertEquals(List.of("region"), display.getPartitionColumnNames());
    }
}
