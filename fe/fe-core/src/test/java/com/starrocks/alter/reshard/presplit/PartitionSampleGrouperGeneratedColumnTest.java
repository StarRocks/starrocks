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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link PartitionSampleGrouper} on real list-partitioned targets keyed on a generated column, declared or the hidden
 * one an expression partition creates: the grouper must name each partition as automatic partition creation does.
 */
public class PartitionSampleGrouperGeneratedColumnTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        savedEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
        starRocksAssert.withDatabase("presplit_gencol").useDatabase("presplit_gencol");
        starRocksAssert.withTable("create table declared (account_id bigint not null, activity_date datetime null, "
                + "tenant_id bigint not null, activity_date_month datetime null as date_trunc('month', activity_date)) "
                + "duplicate key(account_id, activity_date) partition by (tenant_id, activity_date_month) "
                + "order by (account_id, activity_date) properties('replication_num' = '1')");
        starRocksAssert.withTable("create table expression (account_id bigint not null, activity_date datetime null, "
                + "tenant_id bigint not null) duplicate key(account_id, activity_date) "
                + "partition by (tenant_id, date_trunc('month', activity_date)) "
                + "order by (account_id, activity_date) properties('replication_num' = '1')");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("presplit_gencol");
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
    }

    @ParameterizedTest
    @ValueSource(strings = {"declared", "expression"})
    public void generatedPartitionColumnGroupsUnderTheLoadsPartitionNames(String tableName) {
        assertGroupsByMonth(table(tableName));
    }

    @ParameterizedTest
    @CsvSource({"declared, activity_date_month", "expression, " + FeConstants.GENERATED_PARTITION_COLUMN_PREFIX + "0"})
    public void generatedPartitionColumnIsComputedFromTheColumnItReads(String tableName, String generatedColumnName) {
        assertComputedFromActivityDate(table(tableName), generatedColumnName);
    }

    @Test
    public void staticOverwriteMapsTheComputedPartitionToItsTemporaryPartition() throws Exception {
        starRocksAssert.alterTable("alter table declared add partition p1001_20251001000000 "
                + "values in (('1001', '2025-10-01 00:00:00'))");
        starRocksAssert.alterTable("alter table declared add temporary partition tp1001_20251001000000 "
                + "values in (('1001', '2025-10-01 00:00:00'))");
        OlapTable table = table("declared");
        PreSplitPartitionScope scope = PreSplitPartitionScope.staticOverwrite(
                List.of("p1001_20251001000000"), List.of("tp1001_20251001000000"));

        List<PartitionSamples> out = PartitionSampleGrouper.groupSpecified(
                samples(row(1001L, "2025-10-01 00:00:00"), row(1001L, "2025-11-01 00:00:00")),
                table, connectContext, db.getId(), /*totalFileBytes=*/ 0L, scope);

        // The November row belongs to a partition outside the PARTITION(...) list and is dropped.
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals("tp1001_20251001000000", out.get(0).partitionName());
        Assertions.assertTrue(out.get(0).existsInCatalog());
        Assertions.assertNull(out.get(0).analyzedClause(), "an explicit target must never be auto-created");
    }

    /** Two November rows, one December row and a NULL month, which is dropped: the load creates the NULL partition. */
    private static void assertGroupsByMonth(OlapTable table) {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String reason = SkipReason.INVALID_PARTITION_VALUE.name().toLowerCase();
            long invalidBefore = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(reason).getValue();

            List<PartitionSamples> out = PartitionSampleGrouper.group(
                    samples(row(1001L, "2025-11-01 00:00:00"), row(1001L, "2025-11-01 00:00:00"),
                            row(1001L, "2025-12-01 00:00:00"), row(1001L, null)),
                    table, connectContext, db.getId(), /*totalFileBytes=*/ 0L);

            Assertions.assertEquals(List.of("p1001_20251101000000", "p1001_20251201000000"),
                    out.stream().map(PartitionSamples::partitionName).toList());
            Assertions.assertFalse(out.get(0).existsInCatalog());
            Assertions.assertNotNull(out.get(0).analyzedClause());
            Assertions.assertEquals(invalidBefore + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(reason).getValue().longValue());
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    /** Resolves the catalog's generated partition column with every base column fed by its same-named source column. */
    private static void assertComputedFromActivityDate(OlapTable table, String generatedColumnName) {
        Column generated = table.getPartitionInfo().getPartitionColumns(table.getIdToColumn()).get(1);
        Map<String, String> targetToSource = new HashMap<>();
        Map<String, Type> targetToSourceType = new HashMap<>();
        for (Column column : table.getBaseSchemaWithoutGeneratedColumn()) {
            targetToSource.put(column.getName().toLowerCase(), column.getName());
            targetToSourceType.put(column.getName().toLowerCase(), column.getType());
        }

        InsertSelectSourceColumns.Resolved resolved = InsertSelectSourceColumns.withGeneratedColumns(
                new InsertSelectSourceColumns.Resolved(targetToSource, Map.of(), Map.of(), Set.of(), targetToSourceType,
                        Set.of(), Map.of()),
                table, List.of(generated), InsertSelectSourceColumns.InputReading.AS_SELECTED, connectContext,
                /*sourceName*/ null, /*sourceAlias*/ null);

        Assertions.assertTrue(generated.isGeneratedColumn(), generated.getName());
        Assertions.assertEquals(Map.of(generatedColumnName, "date_trunc('month', CAST(`activity_date` AS DATETIME))"),
                resolved.targetToExpressionSql(), resolved.generatedColumnFailures().toString());
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), name);
    }

    private static Tuple row(long tenantId, String month) {
        return new Tuple(List.of(Variant.of(IntegerType.BIGINT, Long.toString(tenantId)),
                month == null ? Variant.nullVariant(DateType.DATETIME) : Variant.of(DateType.DATETIME, month)));
    }

    /** A sample whose sort-key tuples are placeholders: the grouper only reads the partition tuples. */
    private static SampleSet samples(Tuple... partitionTuples) {
        List<Tuple> sortKeyTuples = new ArrayList<>();
        for (int i = 0; i < partitionTuples.length; i++) {
            sortKeyTuples.add(new Tuple(List.of(Variant.of(IntegerType.BIGINT, Integer.toString(i)))));
        }
        return new SampleSet(sortKeyTuples, List.of(partitionTuples), Estimates.ZERO);
    }
}
