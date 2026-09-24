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
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.connector.hive.HiveClassNames;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.UnfilteredPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Partition level checks, which exist because a table's authorization does not automatically describe its partitions. */
public class LakeFormationPartitionSnapshotTest {

    private static final String UNAUTHORIZED_COLUMN = "ssn";
    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", null, "us-west-2", "db", "t");

    private static final Set<String> AUTHORIZED_COLUMNS = authorized("id", "dt");

    private static Set<String> authorized(String... names) {
        Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        set.addAll(List.of(names));
        return set;
    }

    private static HiveTable partitionedTable() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", IntegerType.INT));
        schema.add(new Column("dt", IntegerType.INT));
        return HiveTable.builder()
                .setId(1L)
                .setTableName("t")
                .setCatalogName("lf")
                .setHiveDbName("db")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/db/t")
                .setCreateTime(1600000000L)
                .setFullSchema(schema)
                .setPartitionColumnNames(new ArrayList<>(List.of("dt")))
                .setDataColumnNames(new ArrayList<>(List.of("id")))
                .setProperties(new java.util.HashMap<>())
                .setSerdeProperties(new java.util.HashMap<>())
                .build();
    }

    private static LakeFormationHiveTable authorizedTable() {
        HiveTable physical = partitionedTable();
        return LakeFormationHiveTable.of(physical, physical.getFullSchema(), IDENTITY,
                new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "attempt-1"));
    }

    private static StorageDescriptor sd(String location, String inputFormat, String serde) {
        return StorageDescriptor.builder()
                .location(location)
                .compressed(false)
                .numberOfBuckets(0)
                .storedAsSubDirectories(false)
                .inputFormat(inputFormat)
                .serdeInfo(SerDeInfo.builder().serializationLibrary(serde).build())
                .build();
    }

    private static StorageDescriptor parquetAt(String location) {
        return sd(location, HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS,
                HiveClassNames.PARQUET_HIVE_SERDE_CLASS);
    }

    /**
     * 带上真实响应里一定有的两个字段。省略它们曾经让每个正向用例都跳过列集比较 ——
     * 严格相等那个 bug 能活到真实环境，一半原因就在这里。
     */
    private static UnfilteredPartition partition(String value, StorageDescriptor sd) {
        return UnfilteredPartition.builder()
                .partition(Partition.builder().values(value).storageDescriptor(sd).build())
                .authorizedColumns("id")
                .isRegisteredWithLakeFormation(true)
                .build();
    }

    /** A partition carrying the column set Lake Formation really returns. */
    private static UnfilteredPartition partitionAuthorizedAsLakeFormationDoes(
            String value, StorageDescriptor sd) {
        return UnfilteredPartition.builder()
                .partition(Partition.builder().values(value).storageDescriptor(sd).build())
                .authorizedColumns("id")   // AUTHORIZED_COLUMNS 是 [id, dt]，分区里没有 dt
                .isRegisteredWithLakeFormation(true)
                .build();
    }

    private static LakeFormationPartitionSnapshot build(UnfilteredPartition... partitions) {
        return LakeFormationPartitionSnapshot.build(authorizedTable(), List.of(partitions),
                AUTHORIZED_COLUMNS, IDENTITY);
    }

    private static LakeFormationTableAccessException expectRefused(UnfilteredPartition... partitions) {
        return assertThrows(LakeFormationTableAccessException.class, () -> build(partitions));
    }

    /**
     * The regression this exists for. Every partitioned table was refused because the check compared
     * the partition's column set against the table's *unreduced* set. The mocks never caught it: they
     * built partitions from the table's own column list, so the two always agreed.
     */
    @Test
    public void testAPartitionOmittingThePartitionKeyIsAccepted() {
        LakeFormationPartitionSnapshot snapshot = LakeFormationPartitionSnapshot.build(
                authorizedTable(),
                List.of(partitionAuthorizedAsLakeFormationDoes(
                                "20260101", parquetAt("s3://bucket/db/t/dt=20260101")),
                        partitionAuthorizedAsLakeFormationDoes(
                                "20260102", parquetAt("s3://bucket/db/t/dt=20260102"))),
                AUTHORIZED_COLUMNS, IDENTITY);

        assertEquals(List.of("dt=20260101", "dt=20260102"), snapshot.partitionNames());
    }

    /** A partition that *does* list the partition key is a shape we never saw - it must still refuse. */
    @Test
    public void testAPartitionThatListsThePartitionKeyIsRefused() {
        UnfilteredPartition listsTheKey = UnfilteredPartition.builder()
                .partition(Partition.builder().values("20260101")
                        .storageDescriptor(parquetAt("s3://bucket/db/t/dt=20260101")).build())
                .authorizedColumns("id", "dt")
                .isRegisteredWithLakeFormation(true)
                .build();

        LakeFormationTableAccessException e = expectRefused(listsTheKey);
        assertTrue(e.getMessage().contains("different set of columns"), e.getMessage());
    }

    @Test
    public void testPartitionsInsideTheTableRootAreAccepted() {
        LakeFormationPartitionSnapshot snapshot = build(
                partition("20260101", parquetAt("s3://bucket/db/t/dt=20260101")),
                partition("20260102", parquetAt("s3://bucket/db/t/dt=20260102")));

        assertEquals(List.of("dt=20260101", "dt=20260102"), snapshot.partitionNames());
        assertNotNull(snapshot.partitionFor("dt=20260101"));
        assertTrue(snapshot.contains("dt=20260102"));
    }

    /**
     * A table credential covers the table's own subtree and nothing else, so a partition stored elsewhere
     * cannot be read with it. Refusing the table is the honest answer - the alternative is a query that
     * silently returns part of the data.
     */
    @Test
    public void testAPartitionOutsideTheTableRootRefusesTheWholeTable() {
        LakeFormationTableAccessException e = expectRefused(
                partition("20260101", parquetAt("s3://bucket/db/t/dt=20260101")),
                partition("20260102", parquetAt("s3://bucket/db/elsewhere/dt=20260102")));

        assertTrue(e.getMessage().contains("outside the table's own location"), e.getMessage());
        assertTrue(e.getMessage().contains("dt=20260102"),
                "the partition may be named - the user already knows which table they asked for");
        assertNoColumnLeak(e.getMessage());
    }

    /**
     * The reason this check has to look at the serde as well as the input format: the engine maps columns
     * by position against the table's column list, so a partition in another format would be read with the
     * table's column names applied to whatever happens to sit in those positions.
     */
    @Test
    public void testANonParquetPartitionRefusesTheWholeTable() {
        LakeFormationTableAccessException e = expectRefused(
                partition("20260101", sd("s3://bucket/db/t/dt=20260101",
                        HiveClassNames.ORC_INPUT_FORMAT_CLASS, HiveClassNames.ORC_SERDE_CLASS)));

        assertTrue(e.getMessage().contains("not stored as Parquet"), e.getMessage());
        assertNoColumnLeak(e.getMessage());
    }

    /**
     * The malformed shape a format check that only read the input format would wave through. Both halves
     * have to agree, exactly as they must at table level.
     */
    @Test
    public void testAPartitionWhoseSerdeDisagreesWithItsInputFormatIsRefused() {
        LakeFormationTableAccessException e = expectRefused(
                partition("20260101", sd("s3://bucket/db/t/dt=20260101",
                        HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS,
                        HiveClassNames.ORC_SERDE_CLASS)));

        assertTrue(e.getMessage().contains("not stored as Parquet"), e.getMessage());
    }

    /**
     * Lake Formation answers per partition as well as per table, and the two can disagree. When they do,
     * the projection the plan was built on is not what governs this partition, and there is no safe way to
     * reconcile that here.
     */
    @Test
    public void testAPartitionAuthorizedForOtherColumnsRefusesTheTableWithoutNamingThem() {
        UnfilteredPartition differentlyAuthorized = UnfilteredPartition.builder()
                .partition(Partition.builder().values("20260101")
                        .storageDescriptor(parquetAt("s3://bucket/db/t/dt=20260101")).build())
                .authorizedColumns("id", "dt", UNAUTHORIZED_COLUMN)
                .isRegisteredWithLakeFormation(true)
                .build();

        LakeFormationTableAccessException e = expectRefused(differentlyAuthorized);

        assertTrue(e.getMessage().contains("different set of columns"), e.getMessage());
        assertNoColumnLeak(e.getMessage());
    }

    /** Case is not a difference. */
    @Test
    public void testAPartitionAuthorizedForTheSameColumnsInAnotherCaseIsAccepted() {
        UnfilteredPartition sameButShouted = UnfilteredPartition.builder()
                .partition(Partition.builder().values("20260101")
                        .storageDescriptor(parquetAt("s3://bucket/db/t/dt=20260101")).build())
                .authorizedColumns("ID")
                .isRegisteredWithLakeFormation(true)
                .build();

        assertEquals(List.of("dt=20260101"), build(sameButShouted).partitionNames());
    }

    /** Absent fields refuse, the same way the table level does. */
    @Test
    public void testAPartitionMissingItsAuthorizationFieldsIsRefused() {
        StorageDescriptor sd = parquetAt("s3://bucket/db/t/dt=20260101");

        // 故意不设 isRegisteredWithLakeFormation —— 这条验的就是"缺这个字段必须拒"。
        LakeFormationTableAccessException noFlag = expectRefused(UnfilteredPartition.builder()
                .partition(Partition.builder().values("20260101").storageDescriptor(sd).build())
                .authorizedColumns("id")
                .build());
        assertTrue(noFlag.getMessage().contains("no registration flag"), noFlag.getMessage());

        LakeFormationTableAccessException noColumns = expectRefused(UnfilteredPartition.builder()
                .partition(Partition.builder().values("20260101").storageDescriptor(sd).build())
                .isRegisteredWithLakeFormation(true)
                .build());
        assertTrue(noColumns.getMessage().contains("no authorized columns"), noColumns.getMessage());
    }

    @Test
    public void testAPartitionReportedAsUnregisteredRefusesTheTable() {
        UnfilteredPartition unregistered = UnfilteredPartition.builder()
                .partition(Partition.builder().values("20260101")
                        .storageDescriptor(parquetAt("s3://bucket/db/t/dt=20260101")).build())
                .isRegisteredWithLakeFormation(false)
                .build();

        LakeFormationTableAccessException e = expectRefused(unregistered);
        assertTrue(e.getMessage().contains("not registered while its table is"), e.getMessage());
    }

    @Test
    public void testAPartitionWithoutALocationIsRefused() {
        LakeFormationTableAccessException e = expectRefused(
                partition("20260101", parquetAt(null)));
        assertTrue(e.getMessage().contains("no location"), e.getMessage());
    }

    /**
     * Glue stores the NULL partition as a literal marker and the name has to keep it, or the partition
     * would be looked up under a name the file listing never produces.
     */
    @Test
    public void testTheNullPartitionKeepsItsHiveDefaultName() {
        LakeFormationPartitionSnapshot snapshot = build(partition("__HIVE_DEFAULT_PARTITION__",
                parquetAt("s3://bucket/db/t/dt=__HIVE_DEFAULT_PARTITION__")));

        assertEquals(List.of("dt=__HIVE_DEFAULT_PARTITION__"), snapshot.partitionNames());
    }

    /** An absent value matches anything, a present one has to match exactly. */
    @Test
    public void testPartialSpecificationsFilterTheWayTheMetastoreCacheDoes() {
        List<String> names = List.of("dt=20260101", "dt=20260102");

        assertEquals(names, LakeFormationPartitionSnapshot.filterByValues(names, List.of(Optional.empty())));
        assertEquals(names, LakeFormationPartitionSnapshot.filterByValues(names, null));
        assertEquals(List.of("dt=20260102"),
                LakeFormationPartitionSnapshot.filterByValues(names, List.of(Optional.of("20260102"))));
        assertEquals(List.of(),
                LakeFormationPartitionSnapshot.filterByValues(names, List.of(Optional.of("20260103"))));
    }

    /**
     * Every refusal names the partition but never a column: saying which column differs would confirm to a
     * user without access that it exists and what it is called, which is what the projection hides.
     */
    private static void assertNoColumnLeak(String message) {
        assertFalse(message.contains(UNAUTHORIZED_COLUMN),
                "an unauthorized column name leaked into: " + message);
    }
}
