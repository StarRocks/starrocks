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
import com.starrocks.catalog.TableOperation;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationHiveTableTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", "123456789012", "us-west-2", "db", "t");
    private static final LakeFormationTableHandle HANDLE =
            new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "attempt-1");

    @Mocked
    private GlobalStateMgr globalStateMgr;

    /**
     * HiveTable.getProperties() is not a plain getter - it looks the table's resource up and mutates the
     * property map on the way through - so the resource lookup has to answer something for any of this to
     * run at all.
     */
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

    private static Column col(String name) {
        return new Column(name, IntegerType.INT);
    }

    private static HiveTable physicalTable(List<String> columns, List<String> partitionColumns) {
        List<Column> schema = columns.stream().map(name -> col(name)).collect(Collectors.toList());
        List<String> dataColumns = columns.stream()
                .filter(name -> !partitionColumns.contains(name))
                .collect(Collectors.toList());
        Map<String, String> properties = new HashMap<>();
        properties.put("hive.table.column.names", String.join(",", columns));
        Map<String, String> serdeProperties = new HashMap<>();
        serdeProperties.put("field.delim", ",");
        return HiveTable.builder()
                .setId(7L)
                .setTableName("t")
                .setCatalogName("lf")
                .setHiveDbName("db")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/db/t")
                .setCreateTime(1600000000L)
                .setFullSchema(schema)
                .setPartitionColumnNames(new ArrayList<>(partitionColumns))
                .setDataColumnNames(new ArrayList<>(dataColumns))
                .setProperties(properties)
                .setSerdeProperties(serdeProperties)
                .build();
    }

    private static List<String> names(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    @Test
    public void testVisibleSchemaIsTheAuthorizedSubsetAndStaysSelfConsistent() {
        // region is a partition column, so it must be authorized - the guard refuses the table otherwise,
        // which is why this class never has to cope with a hidden partition column.
        HiveTable physical = physicalTable(List.of("id", "region", "ssn"), List.of("region"));
        LakeFormationHiveTable lf =
                LakeFormationHiveTable.of(physical, List.of(col("id"), col("region")), IDENTITY, HANDLE);

        assertEquals(List.of("id", "region"), names(lf.getFullSchema()));
        assertEquals(List.of("id", "region"), names(lf.getBaseSchema()));
        assertNull(lf.getColumn("ssn"));
        assertFalse(lf.containColumn("ssn"));

        // The inherited implementation is correct on its own: every partition column resolves, no nulls.
        assertEquals(List.of("region"), names(lf.getPartitionColumns()));
        assertFalse(lf.getPartitionColumns().contains(null));

        // Name and position bearing lists stay physical.
        assertEquals(List.of("region"), lf.getPartitionColumnNames());
        assertEquals(physical.getDataColumnNames(), lf.getDataColumnNames());
    }

    @Test
    public void testDoesNotAliasThePhysicalTablesMutableState() {
        HiveTable physical = physicalTable(List.of("id", "region"), List.of("region"));
        LakeFormationHiveTable lf =
                LakeFormationHiveTable.of(physical, List.of(col("id"), col("region")), IDENTITY, HANDLE);

        assertNotSame(physical.getDataColumnNames(), lf.getDataColumnNames());
        assertNotSame(physical.getPartitionColumnNames(), lf.getPartitionColumnNames());
        assertNotSame(physical.getSerdeProperties(), lf.getSerdeProperties());

        // Copying the list does not isolate Column, which is mutable.
        assertNotSame(physical.getColumn("id"), lf.getColumn("id"));
        lf.getColumn("id").setComment("changed");
        assertNotEquals("changed", physical.getColumn("id").getComment());
    }

    /** Policies are keyed by UUID, which is built from createTime, so losing it loses the policy match. */
    @Test
    public void testPreservesTheIdentityPoliciesAreKeyedBy() {
        HiveTable physical = physicalTable(List.of("id"), List.of());
        LakeFormationHiveTable lf = LakeFormationHiveTable.of(physical, List.of(col("id")), IDENTITY,
                new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "attempt-1"));

        assertEquals(physical.getCreateTime(), lf.getCreateTime());
        assertEquals(physical.getUUID(), lf.getUUID());
        assertEquals(physical.getId(), lf.getId());
    }

    /** The full physical property set, but a snapshot: reading it must not rewrite the authorized view. */
    @Test
    public void testPropertiesAreASnapshotAndKeepThePhysicalContent() {
        HiveTable physical = physicalTable(List.of("id", "ssn"), List.of());
        LakeFormationHiveTable lf = LakeFormationHiveTable.of(physical, List.of(col("id")), IDENTITY,
                new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "attempt-1"));

        assertEquals("id,ssn", lf.getProperties().get("hive.table.column.names"));
        assertThrows(UnsupportedOperationException.class, () -> lf.getProperties().put("x", "y"));

        // The snapshot is its own map: mutating the physical table's properties afterwards must not show
        // through, which is the whole reason getProperties is overridden rather than inherited.
        assertNotSame(physical.getProperties(), lf.getProperties());
        physical.getProperties().put("hive.table.column.names", "id,ssn,added_later");
        assertEquals("id,ssn", lf.getProperties().get("hive.table.column.names"));
    }

    @Test
    public void testAuthorizedColumnNamesAreCaseInsensitiveAndImmutable() {
        HiveTable physical = physicalTable(List.of("Id", "ssn"), List.of());
        LakeFormationHiveTable lf = LakeFormationHiveTable.of(physical, List.of(col("Id")), IDENTITY,
                new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "attempt-1"));

        assertTrue(lf.isColumnAuthorized("id"));
        assertTrue(lf.isColumnAuthorized("ID"));
        assertFalse(lf.isColumnAuthorized("ssn"));
        assertThrows(UnsupportedOperationException.class, () -> lf.getAuthorizedColumnNames().add("ssn"));
    }

    @Test
    public void testOnlySupportsRead() {
        LakeFormationHiveTable lf = LakeFormationHiveTable.of(
                physicalTable(List.of("id"), List.of()), List.of(col("id")), IDENTITY, HANDLE);
        assertEquals(java.util.Set.of(TableOperation.READ), lf.getSupportedOperations());
    }

    /** getSupportedOperations does not gate INSERT; InsertAnalyzer consults supportInsert instead. */
    @Test
    public void testInsertIsRefusedByTheTableItself() {
        LakeFormationHiveTable lf = LakeFormationHiveTable.of(
                physicalTable(List.of("id"), List.of()), List.of(col("id")), IDENTITY, HANDLE);
        assertFalse(lf.supportInsert());
    }

    @Test
    public void testSchemaMutatorsAreSealed() {
        HiveTable physical = physicalTable(List.of("id", "ssn"), List.of());
        LakeFormationHiveTable lf = LakeFormationHiveTable.of(physical, List.of(col("id")), IDENTITY,
                new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "attempt-1"));

        assertThrows(LakeFormationTableAccessException.class,
                () -> lf.modifyTableSchema("db", "t", physical));
        assertThrows(LakeFormationTableAccessException.class,
                () -> lf.setNewFullSchema(new ArrayList<>(physical.getFullSchema())));
        assertThrows(LakeFormationTableAccessException.class, () -> lf.addColumn(col("ssn")));

        // The authorized view is still the authorized view after all of that.
        assertEquals(List.of("id"), names(lf.getFullSchema()));
    }
}
