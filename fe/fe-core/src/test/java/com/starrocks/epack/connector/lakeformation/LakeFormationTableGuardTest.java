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
import com.starrocks.connector.hive.HiveClassNames;
import com.starrocks.connector.hive.glue.converters.CatalogToHiveConverter;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataResponse;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationTableGuardTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", "123456789012", "us-west-2", "db", "t");

    /**
     * Every case starts from an SDK v2 Glue table and goes through the real converter, because the
     * conversion step is itself part of what is being guarded: building a Hive model table by hand here
     * would leave that step with no coverage at all.
     */
    private static org.apache.hadoop.hive.metastore.api.Table convert(Table glueTable) {
        return CatalogToHiveConverter.convertTable(glueTable, IDENTITY.dbName());
    }

    private static Table.Builder parquetTable() {
        return Table.builder()
                .name(IDENTITY.tableName())
                .databaseName(IDENTITY.dbName())
                .tableType("EXTERNAL_TABLE")
                // CatalogToHiveConverter unboxes retention(), so an absent value is an NPE there.
                .retention(0)
                .storageDescriptor(storageDescriptor()
                        .location("s3://bucket/db/t")
                        .inputFormat(HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS)
                        .serdeInfo(SerDeInfo.builder()
                                .serializationLibrary(HiveClassNames.PARQUET_HIVE_SERDE_CLASS)
                                .build())
                        .columns(glueColumn("id"), glueColumn("region"))
                        .build());
    }

    /**
     * CatalogToHiveConverter unboxes compressed(), numberOfBuckets() and storedAsSubDirectories() (and
     * retention() on the table), so a builder that leaves them absent blows up inside the converter rather
     * than reaching the guard at all.
     */
    private static StorageDescriptor.Builder storageDescriptor() {
        return StorageDescriptor.builder()
                .compressed(false)
                .numberOfBuckets(0)
                .storedAsSubDirectories(false);
    }

    private static software.amazon.awssdk.services.glue.model.Column glueColumn(String name) {
        return software.amazon.awssdk.services.glue.model.Column.builder().name(name).type("int").build();
    }

    private static AuthorizedTableMetadata metadata() {
        return AuthorizedTableMetadata.from(GetUnfilteredTableMetadataResponse.builder()
                .authorizedColumns("id", "region")
                .build());
    }

    private static LakeFormationTableAccessException check(Table glueTable) {
        return assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationTableGuard.check(metadata(), convert(glueTable), IDENTITY));
    }

    @Test
    public void testAcceptsPlainParquetTable() {
        assertDoesNotThrow(() -> LakeFormationTableGuard.check(metadata(), convert(parquetTable().build()), IDENTITY));
    }

    @Test
    public void testRejectsPartitionProjectionCaseInsensitively() {
        LakeFormationTableAccessException e =
                check(parquetTable().parameters(Map.of("PROJECTION.Enabled", "TRUE")).build());
        assertTrue(e.getMessage().contains("partition projection"), e.getMessage());
    }

    @Test
    public void testProjectionDisabledIsAccepted() {
        Table table = parquetTable().parameters(Map.of("projection.enabled", "false")).build();
        assertDoesNotThrow(() -> LakeFormationTableGuard.check(metadata(), convert(table), IDENTITY));
    }

    @Test
    public void testRejectsSymlinkTextInputFormatWithItsOwnMessage() {
        Table table = parquetTable()
                .storageDescriptor(storageDescriptor()
                        .location("s3://bucket/db/t")
                        .inputFormat("org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat")
                        .serdeInfo(SerDeInfo.builder()
                                .serializationLibrary(HiveClassNames.PARQUET_HIVE_SERDE_CLASS).build())
                        .build())
                .build();
        LakeFormationTableAccessException e = check(table);
        assertTrue(e.getMessage().contains("symlink"), e.getMessage());
        // Must not be swallowed by the generic "not Parquet" answer.
        assertFalse(e.getMessage().contains("only Parquet is supported"), e.getMessage());
    }

    @Test
    public void testRejectsNonParquetStorageFormat() {
        Table table = parquetTable()
                .storageDescriptor(storageDescriptor()
                        .location("s3://bucket/db/t")
                        .inputFormat(HiveClassNames.ORC_INPUT_FORMAT_CLASS)
                        .serdeInfo(SerDeInfo.builder()
                                .serializationLibrary(HiveClassNames.ORC_SERDE_CLASS).build())
                        .build())
                .build();
        assertTrue(check(table).getMessage().contains("only Parquet is supported"));
    }

    /**
     * The dangerous shape: the parameters say Delta, but the storage descriptor says plain Parquet. Only the
     * parameters give it away, and reading it as a Hive table would be wrong - Delta's layout is a
     * transaction log, not an enumerable set of files.
     */
    @Test
    public void testRejectsDeltaTableThatLooksLikeParquetInItsStorageDescriptor() {
        Table table = parquetTable()
                .parameters(Map.of("spark.sql.sources.provider", "delta"))
                .build();
        LakeFormationTableAccessException e = check(table);
        assertTrue(e.getMessage().contains("Delta"), e.getMessage());
        assertFalse(e.getMessage().contains("only Parquet is supported"), e.getMessage());
    }

    @Test
    public void testRejectsIcebergTableThatLooksLikeParquetInItsStorageDescriptor() {
        Table table = parquetTable()
                .parameters(Map.of("table_type", "ICEBERG"))
                .build();
        LakeFormationTableAccessException e = check(table);
        assertTrue(e.getMessage().contains("Iceberg"), e.getMessage());
        assertFalse(e.getMessage().contains("only Parquet is supported"), e.getMessage());
    }

    @Test
    public void testRejectsVirtualView() {
        assertTrue(check(parquetTable().tableType("VIRTUAL_VIEW").build()).getMessage().contains("views"));
    }

    @Test
    public void testRejectsRowOrCellFilterAsAnInternalError() {
        AuthorizedTableMetadata withFilter = AuthorizedTableMetadata.from(
                GetUnfilteredTableMetadataResponse.builder()
                        .authorizedColumns("id")
                        .rowFilter("region = 'us-west-2'")
                        .build());
        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationTableGuard.check(withFilter, convert(parquetTable().build()), IDENTITY));
        assertTrue(e.getMessage().contains("row or cell filter"), e.getMessage());
    }

    @Test
    public void testRejectsHudiTable() {
        Table table = parquetTable()
                .storageDescriptor(storageDescriptor()
                        .location("s3://bucket/db/t")
                        .inputFormat("org.apache.hudi.hadoop.HoodieParquetInputFormat")
                        .serdeInfo(SerDeInfo.builder()
                                .serializationLibrary(HiveClassNames.PARQUET_HIVE_SERDE_CLASS).build())
                        .build())
                .build();
        assertTrue(check(table).getMessage().contains("Hudi"), "expected a Hudi specific message");
    }

    /** ACID tables are ORC, so the format allowlist would also stop them - but with the wrong reason. */
    @Test
    public void testRejectsFullAcidTableWithItsOwnMessage() {
        Table table = parquetTable()
                .parameters(Map.of("transactional", "true"))
                .build();
        LakeFormationTableAccessException e = check(table);
        assertTrue(e.getMessage().contains("ACID"), e.getMessage());
        assertFalse(e.getMessage().contains("only Parquet is supported"), e.getMessage());
    }

    @Test
    public void testRejectsInsertOnlyTransactionalTable() {
        Table table = parquetTable()
                .parameters(Map.of("transactional", "true", "transactional_properties", "insert_only"))
                .build();
        assertTrue(check(table).getMessage().contains("transactional"), "expected a transactional message");
    }

    // ---- X8: every physical partition column must be authorized ----------------------------------------

    private static Table partitionedBy(String... partitionColumns) {
        List<software.amazon.awssdk.services.glue.model.Column> keys = new ArrayList<>();
        for (String name : partitionColumns) {
            keys.add(glueColumn(name));
        }
        return parquetTable().partitionKeys(keys).build();
    }

    @Test
    public void testAcceptsWhenEveryPartitionColumnIsAuthorized() {
        assertDoesNotThrow(() -> LakeFormationTableGuard.checkPartitionColumnsAuthorized(
                List.of(new Column("id", IntegerType.INT), new Column("region", IntegerType.INT)),
                convert(partitionedBy("region")), IDENTITY));
    }

    @Test
    public void testRejectsWhenAnyPhysicalPartitionColumnIsNotAuthorized() {
        assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationTableGuard.checkPartitionColumnsAuthorized(
                        List.of(new Column("id", IntegerType.INT)), convert(partitionedBy("region")), IDENTITY));
    }

    @Test
    public void testPartitionColumnMatchIsCaseInsensitive() {
        assertDoesNotThrow(() -> LakeFormationTableGuard.checkPartitionColumnsAuthorized(
                List.of(new Column("Region", IntegerType.INT)), convert(partitionedBy("region")), IDENTITY));
    }

    /**
     * Naming the column would tell a user without access that it exists, that it is a partition column and
     * what it is called - the same thing the design refuses to reveal for ordinary unauthorized columns.
     */
    @Test
    public void testX8MessageNeverNamesTheUnauthorizedPartitionColumn() {
        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationTableGuard.checkPartitionColumnsAuthorized(
                        List.of(new Column("id", IntegerType.INT)), convert(partitionedBy("ssn")), IDENTITY));
        assertFalse(e.getMessage().contains("ssn"), e.getMessage());
    }

    @Test
    public void testUnpartitionedTablePassesX8() {
        assertDoesNotThrow(() -> LakeFormationTableGuard.checkPartitionColumnsAuthorized(
                List.of(new Column("id", IntegerType.INT)), convert(parquetTable().build()), IDENTITY));
    }
}
