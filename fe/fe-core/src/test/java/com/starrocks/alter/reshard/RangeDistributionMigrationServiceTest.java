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

package com.starrocks.alter.reshard;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class RangeDistributionMigrationServiceTest {
    private static StarRocksAssert starRocksAssert;
    private static AtomicInteger sequence;

    private Database db;
    private OlapTable table;
    private CapturingJobs jobs;
    private int oldMaxSplitCount;

    @BeforeAll
    static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        Config.enable_range_distribution = true;
        sequence = new AtomicInteger();
    }

    @BeforeEach
    void setUp() throws Exception {
        int suffix = sequence.incrementAndGet();
        String dbName = "range_migration_service_" + suffix;
        String tableName = "tbl_" + suffix;
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        starRocksAssert.withTable("create table " + tableName
                + " (k1 int, k2 varchar(10), v bigint) duplicate key(k1, k2) order by(k1, k2) "
                + "properties('replication_num'='1')");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        table = (OlapTable) db.getTable(tableName);
        jobs = new CapturingJobs();
        oldMaxSplitCount = Config.tablet_reshard_max_split_count;
    }

    @AfterEach
    void tearDown() {
        Config.tablet_reshard_max_split_count = oldMaxSplitCount;
        table.setState(OlapTable.OlapTableState.NORMAL);
    }

    @Test
    void malformedInputFailsClosedWithOneLineJson() {
        Tablet tablet = onlyTablet();
        TabletRange before = tablet.getRange();

        JsonObject response = response(service().reconcile("not-base64"));

        Assertions.assertEquals("FAILED", response.get("status").getAsString());
        Assertions.assertSame(before, tablet.getRange());
        Assertions.assertFalse(service().reconcile("not-base64").contains("\n"));
        Assertions.assertTrue(jobs.jobs.isEmpty());
    }

    @Test
    void decodedPayloadCannotExceedThriftFrameSize() {
        int oldFrameSize = Config.thrift_max_frame_size;
        try {
            Config.thrift_max_frame_size = 1;
            JsonObject response = response(service().reconcile(encodeJson("{}")));

            Assertions.assertEquals("FAILED", response.get("status").getAsString());
            Assertions.assertTrue(response.get("message").getAsString().contains("bounded input size"));
            Assertions.assertTrue(jobs.jobs.isEmpty());
        } finally {
            Config.thrift_max_frame_size = oldFrameSize;
        }
    }

    @Test
    void multiColumnTypedNullReconcilesEndToEndAndLocalMetastoreEntryAligns() {
        List<TabletRange> desired = List.of(
                range(null, tuple(10, null)), range(tuple(10, null), null));
        List<Column> columns = MetaUtils.getRangeDistributionColumns(table, latestIndex().getMetaId());
        RangeDistributionMigrationService.validateRangeSequenceForTest(desired, columns);

        JsonObject response = response(service().reconcile(request("typed-null", desired)));

        Assertions.assertEquals("SUBMITTED", response.get("status").getAsString());
        Assertions.assertFalse(jobs.jobs.isEmpty());
        String aligned = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .reconcileRangeTablets(request("metastore-entry", List.of(new TabletRange())));
        Assertions.assertEquals("ALIGNED", status(aligned));
    }

    @Test
    void oneParentAndRealMultipleParentSplitsBuildOneExternalJob() {
        List<TabletRange> desired = List.of(range(null, tuple(0, "a")), range(tuple(0, "a"), null));
        Assertions.assertEquals("SUBMITTED", status(service().reconcile(request("one-parent", desired))));
        SplitTabletJob first = jobs.last();
        Assertions.assertEquals("one-parent", first.getExternalRequestId());
        Assertions.assertNotNull(first.getExternalFinalDigest());
        Assertions.assertNotNull(first.getExternalStepDigest());

        jobs.jobs.clear();
        MaterializedIndex index = latestIndex();
        Tablet leftParent = index.getTablets().get(0);
        leftParent.setRange(range(null, tuple(0, "a")));
        Tablet rightParent = addTablet(index, range(tuple(0, "a"), null), true);
        List<TabletRange> multipleParentDesired = List.of(
                range(null, tuple(-1, "z")), range(tuple(-1, "z"), tuple(0, "a")),
                range(tuple(0, "a"), tuple(2, "b")), range(tuple(2, "b"), null));

        Assertions.assertEquals("SUBMITTED",
                status(service().reconcile(request("multiple-parents", multipleParentDesired))));
        Assertions.assertEquals(1, jobs.jobs.size());
        Map<Long, List<TabletRange>> splittingRanges = splittingRanges(jobs.last());
        Assertions.assertEquals(List.of(leftParent.getId(), rightParent.getId()),
                splittingRanges.keySet().stream().sorted().toList());
        Assertions.assertEquals(2, splittingRanges.get(leftParent.getId()).size());
        Assertions.assertEquals(2, splittingRanges.get(rightParent.getId()).size());
    }

    @Test
    void completeGroupSetAndFreshIndexIdAreRequired() {
        addSecondLogicalPartition();
        JsonObject missing = decodedCatalogRequest("missing", twoRanges());
        missing.getAsJsonArray("targets").remove(1);
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(encode(missing))));

        JsonObject stale = decodedRequest("stale", twoRanges());
        stale.getAsJsonArray("targets").get(0).getAsJsonObject()
                .addProperty("currentIndexId", latestIndex().getId() + 1);
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(encode(stale))));
        Assertions.assertTrue(jobs.jobs.isEmpty());
    }

    @Test
    void rejectsSharedNothingNonRangeColocateAndMultiplePhysicalSubpartitions() {
        String encoded = request("scope", twoRanges());

        table.setType(TableType.OLAP);
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(encoded)));
        table.setType(TableType.CLOUD_NATIVE);

        table.setDefaultDistributionInfo(new HashDistributionInfo(1, List.of(table.getColumn("k1"))));
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(encoded)));
        table.setDefaultDistributionInfo(new RangeDistributionInfo());

        table.setColocateGroup("range-colocate");
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(encoded)));
        table.setColocateGroup(null);

        Partition logical = table.getPartitions().iterator().next();
        PhysicalPartition extra = new PhysicalPartition(
                GlobalStateMgr.getCurrentState().getNextId(), logical.getId());
        logical.addSubPartition(extra);
        table.addPhysicalPartition(extra);
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(encoded)));
        logical.removeSubPartition(extra.getId());
        table.removePhysicalPartition(extra);
    }

    @Test
    void completeSnapshotCoversMultipleLogicalPartitionsAndEveryVisibleIndex() {
        addSecondLogicalPartition();
        addVisibleRollupToEveryPhysicalPartition();

        JsonObject request = decodedCatalogRequest("complete", null);

        Assertions.assertEquals(4, request.getAsJsonArray("targets").size());
        Assertions.assertEquals("ALIGNED", status(service().reconcile(encode(request))));
    }

    @Test
    void invalidGapOverlapDuplicateInversionArityAndTypeAreRejected() {
        List<Column> columns = MetaUtils.getRangeDistributionColumns(table, latestIndex().getMetaId());
        List<TabletRange> gap = List.of(range(null, tuple(0, "a")), range(tuple(1, "a"), null));
        assertInvalid(gap, columns);
        assertInvalid(List.of(range(null, tuple(1, "a")), range(tuple(0, "a"), null)), columns);
        assertInvalid(List.of(range(null, tuple(0, "a")), range(null, tuple(0, "a"))), columns);
        assertInvalid(List.of(range(tuple(2, "a"), tuple(1, "a"))), columns);
        assertInvalid(List.of(new TabletRange(
                Range.lt(new Tuple(List.of(Variant.of(IntegerType.INT, "1")))))), columns);
        assertInvalid(List.of(new TabletRange(Range.lt(new Tuple(List.of(
                Variant.of(TypeFactory.createVarcharType(10), "wrong"),
                Variant.of(TypeFactory.createVarcharType(10), "type")))))), columns);

        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(request("gap", gap))));
        Tuple wrongTypes = new Tuple(List.of(
                Variant.of(TypeFactory.createVarcharType(10), "wrong"),
                Variant.of(TypeFactory.createVarcharType(10), "type")));
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(request("wrong-types",
                List.of(range(null, wrongTypes), range(wrongTypes, null))))));

        Tuple incompatibleTypes = new Tuple(List.of(
                Variant.of(DateType.DATE, "2026-08-03"),
                Variant.of(TypeFactory.createVarcharType(10), "type")));
        List<TabletRange> mixedFiniteTypes = List.of(range(null, tuple(-1, "a")),
                range(tuple(-1, "a"), incompatibleTypes), range(incompatibleTypes, null));
        Assertions.assertEquals("INCOMPATIBLE",
                status(service().reconcile(request("mixed-finite-types", mixedFiniteTypes))));
    }

    @Test
    void malformedSentinelAndDecimalBoundariesFailBeforeSubmission() throws Exception {
        Tuple minimum = new Tuple(List.of(
                Variant.minVariant(IntegerType.INT),
                Variant.of(TypeFactory.createVarcharType(10), "a")));
        Tuple maximum = new Tuple(List.of(
                Variant.maxVariant(IntegerType.INT),
                Variant.of(TypeFactory.createVarcharType(10), "a")));

        Assertions.assertEquals("FAILED", status(service().reconcile(request("minimum", List.of(
                range(null, minimum), range(minimum, null))))));
        Assertions.assertEquals("FAILED", status(service().reconcile(request("maximum", List.of(
                range(null, maximum), range(maximum, null))))));
        Assertions.assertTrue(jobs.jobs.isEmpty());

        useSingleKeyTable("decimal_boundary", "decimal(5, 2)");
        Tuple rounding = singleValueTuple(Variant.of(table.getColumn("k1").getType(), "1.234"));
        Tuple overflow = singleValueTuple(Variant.of(table.getColumn("k1").getType(), "1000.00"));

        Assertions.assertEquals("FAILED", status(service().reconcile(request("decimal-rounding", List.of(
                range(null, rounding), range(rounding, null))))));
        Assertions.assertEquals("FAILED", status(service().reconcile(request("decimal-overflow", List.of(
                range(null, overflow), range(overflow, null))))));
        Assertions.assertTrue(jobs.jobs.isEmpty());
    }

    @Test
    void varbinaryDistributionColumnIsRejectedBeforeSubmission() throws Exception {
        useSingleKeyTable("varbinary_boundary", "varbinary");

        Assertions.assertEquals("INCOMPATIBLE",
                status(service().reconcile(request("varbinary", List.of(new TabletRange())))));
        Assertions.assertTrue(jobs.jobs.isEmpty());
    }

    @Test
    void nonCanonicalBoundOrientationAndClosedPointAreRejectedBeforeSubmission() {
        Tuple boundary = tuple(0, "a");
        List<TabletRange> oppositeOrientation = List.of(
                new TabletRange(Range.of(null, boundary, false, true)),
                new TabletRange(Range.of(boundary, null, false, false)));
        Assertions.assertEquals("INCOMPATIBLE",
                status(service().reconcile(request("opposite-orientation", oppositeOrientation))));
        Assertions.assertTrue(jobs.jobs.isEmpty());

        List<TabletRange> closedPoint = List.of(
                range(null, boundary),
                new TabletRange(Range.of(boundary, boundary, true, true)),
                new TabletRange(Range.of(boundary, null, false, false)));
        Assertions.assertEquals("INCOMPATIBLE",
                status(service().reconcile(request("closed-point", closedPoint))));
        Assertions.assertTrue(jobs.jobs.isEmpty());
    }

    @Test
    void protocolRejectsUnknownDuplicateEmptyAndInvalidRangeInputs() {
        JsonObject unknown = decodedRequest("unknown", twoRanges());
        unknown.addProperty("unexpected", true);
        Assertions.assertEquals("FAILED", status(service().reconcile(encode(unknown))));

        JsonObject duplicate = decodedRequest("duplicate", twoRanges());
        duplicate.getAsJsonArray("targets")
                .add(duplicate.getAsJsonArray("targets").get(0).deepCopy());
        Assertions.assertEquals("FAILED", status(service().reconcile(encode(duplicate))));

        JsonObject empty = decodedRequest("empty", twoRanges());
        empty.getAsJsonArray("targets").get(0).getAsJsonObject().add("ranges", new JsonArray());
        Assertions.assertEquals("FAILED", status(service().reconcile(encode(empty))));

        JsonObject invalidRange = decodedRequest("invalid-range", twoRanges());
        invalidRange.getAsJsonArray("targets").get(0).getAsJsonObject()
                .getAsJsonArray("ranges").set(0, JsonParser.parseString("\"not-a-tablet-range\""));
        Assertions.assertEquals("FAILED", status(service().reconcile(encode(invalidRange))));

        JsonObject mismatchedName = decodedRequest("identity", twoRanges());
        mismatchedName.addProperty("tableName", table.getName() + "_other");
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(encode(mismatchedName))));
        Assertions.assertTrue(jobs.jobs.isEmpty());
    }

    @Test
    void protocolUsesStrictJsonTokensAndRejectsDuplicateMembers() {
        String valid = decodedRequest("strict-json", List.of(new TabletRange())).toString();
        long physicalPartitionId = table.getPartitions().iterator().next().getDefaultPhysicalPartition().getId();

        String unquoted = valid.replaceFirst("\\\"version\\\"", "version");
        String singleQuoted = valid.replace('"', '\'');
        String duplicateRequestMember = valid.replaceFirst("\\{", "{\\\"version\\\":1,");
        String physicalToken = "\"physicalPartitionId\":" + physicalPartitionId;
        String duplicateTargetMember = valid.replace(physicalToken, physicalToken + "," + physicalToken);
        String fractionalVersion = valid.replace("\"version\":1", "\"version\":1.0");
        String exponentVersion = valid.replace("\"version\":1", "\"version\":1e0");
        String fractionalTableId = valid.replace(
                "\"tableId\":" + table.getId(), "\"tableId\":" + table.getId() + ".0");
        String stringDatabaseId = valid.replace(
                "\"databaseId\":" + db.getId(), "\"databaseId\":\"" + db.getId() + "\"");
        String padded = encodeJson(valid + " ");
        while (!padded.endsWith("=")) {
            valid += " ";
            padded = encodeJson(valid);
        }
        String unpadded = padded.substring(0, padded.length() - 1);
        String invalidUtf8 = Base64.getEncoder().encodeToString(new byte[] {(byte) 0xff});

        Assertions.assertAll(
                () -> Assertions.assertEquals("FAILED", status(service().reconcile(encodeJson(unquoted)))),
                () -> Assertions.assertEquals("FAILED", status(service().reconcile(encodeJson(singleQuoted)))),
                () -> Assertions.assertEquals("FAILED",
                        status(service().reconcile(encodeJson(duplicateRequestMember)))),
                () -> Assertions.assertEquals("FAILED",
                        status(service().reconcile(encodeJson(duplicateTargetMember)))),
                () -> Assertions.assertEquals("FAILED",
                        status(service().reconcile(encodeJson(fractionalVersion)))),
                () -> Assertions.assertEquals("FAILED", status(service().reconcile(encodeJson(exponentVersion)))),
                () -> Assertions.assertEquals("FAILED",
                        status(service().reconcile(encodeJson(fractionalTableId)))),
                () -> Assertions.assertEquals("FAILED",
                        status(service().reconcile(encodeJson(stringDatabaseId)))),
                () -> Assertions.assertEquals("FAILED", status(service().reconcile(unpadded))),
                () -> Assertions.assertEquals("FAILED", status(service().reconcile(invalidUtf8))));
        Assertions.assertTrue(jobs.jobs.isEmpty());
    }

    @Test
    void coarserTargetAndRangeCrossingParentsRequireMerge() {
        List<RangeDistributionMigrationService.CurrentTablet> parents = List.of(
                new RangeDistributionMigrationService.CurrentTablet(1, range(null, tuple(0, "a"))),
                new RangeDistributionMigrationService.CurrentTablet(2, range(tuple(0, "a"), null)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> RangeDistributionMigrationService.planRefinementForTest(
                        parents, List.of(new TabletRange()), 8));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> RangeDistributionMigrationService.planRefinementForTest(
                        parents, List.of(range(null, tuple(2, "a")), range(tuple(2, "a"), null)), 8));

        MaterializedIndex index = latestIndex();
        Tablet first = index.getTablets().get(0);
        first.setRange(range(null, tuple(0, "a")));
        addTablet(index, range(tuple(0, "a"), null), false);
        Assertions.assertEquals("INCOMPATIBLE",
                status(service().reconcile(request("merge", List.of(new TabletRange())))));
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(request("crossing", List.of(
                range(null, tuple(2, "a")), range(tuple(2, "a"), null))))));
    }

    @Test
    void retryIsRunningAndRequestIdConflictIsIncompatible() {
        Assertions.assertEquals("SUBMITTED", status(service().reconcile(request("same", twoRanges()))));
        SplitTabletJob submitted = jobs.last();
        Assertions.assertEquals("RUNNING", status(service().reconcile(request("same", twoRanges()))));

        List<TabletRange> different = List.of(range(null, tuple(-1, "a")),
                range(tuple(-1, "a"), tuple(1, "a")), range(tuple(1, "a"), null));
        Assertions.assertEquals("INCOMPATIBLE", status(service().reconcile(request("same", different))));
        Assertions.assertSame(submitted, jobs.last());
    }

    @Test
    void lostSubmissionResponseRetryIsRunningAfterReplacementIndexInstalled() {
        String originalRequest = request("lost-response", twoRanges());
        Assertions.assertEquals("SUBMITTED", status(service().reconcile(originalRequest)));
        SplitTabletJob retained = jobs.last();
        long originalIndexId = latestIndex().getId();

        retained.setJobState(TabletReshardJob.JobState.CLEANING);
        PhysicalPartition physical = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        MaterializedIndex replacement = replaceLatestIndex(physical, twoRanges());
        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);
        Assertions.assertNotEquals(originalIndexId, replacement.getId());

        JsonObject response = response(service().reconcile(originalRequest));
        Assertions.assertEquals("RUNNING", response.get("status").getAsString());
        Assertions.assertEquals(retained.getJobId(), response.get("jobId").getAsLong());
        Assertions.assertEquals(1, jobs.jobs.size());
    }

    @Test
    void alignedTopologyStillHonorsRetainedJobsRequestBindingAndTableState() {
        Assertions.assertEquals("SUBMITTED", status(service().reconcile(request("conflict", twoRanges()))));
        Assertions.assertEquals("INCOMPATIBLE",
                status(service().reconcile(request("conflict", List.of(new TabletRange())))));

        jobs.jobs.clear();
        Assertions.assertEquals("SUBMITTED", status(service().reconcile(request("unrelated", twoRanges()))));
        Assertions.assertEquals("RETRYABLE_BUSY",
                status(service().reconcile(request("aligned-other", List.of(new TabletRange())))));

        jobs.jobs.clear();
        Assertions.assertEquals("SUBMITTED", status(service().reconcile(request("matching", twoRanges()))));
        PhysicalPartition physical = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        replaceLatestIndex(physical, twoRanges());
        Assertions.assertEquals("RUNNING", status(service().reconcile(request("matching", twoRanges()))));

        jobs.jobs.clear();
        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);
        Assertions.assertEquals("RETRYABLE_BUSY", status(service().reconcile(request("state", twoRanges()))));
    }

    @Test
    void unrelatedNonFinalReshardIsRetryableBusy() throws Exception {
        SplitTabletJob unrelated = (SplitTabletJob) SplitTabletJobFactory.forExternalBoundaries(
                db, table, Map.of(onlyTablet().getId(), twoRanges()));
        jobs.jobs.add(unrelated);
        Assertions.assertEquals("RETRYABLE_BUSY", status(service().reconcile(request("new", twoRanges()))));
    }

    @Test
    void boundedFanoutIsDeterministicAndFinalDigestIgnoresCurrentIndexId() {
        Config.tablet_reshard_max_split_count = 2;
        List<TabletRange> finalRanges = List.of(
                range(null, tuple(-2, "a")), range(tuple(-2, "a"), tuple(-1, "a")),
                range(tuple(-1, "a"), tuple(0, "a")), range(tuple(0, "a"), tuple(1, "a")),
                range(tuple(1, "a"), null));
        JsonObject request = decodedRequest("fanout", finalRanges);
        JsonObject changedIndexId = request.deepCopy();
        changedIndexId.getAsJsonArray("targets").get(0).getAsJsonObject()
                .addProperty("currentIndexId", latestIndex().getId() + 99);
        JsonObject reorderedRanges = request.deepCopy();
        JsonArray encodedRanges = reorderedRanges.getAsJsonArray("targets").get(0).getAsJsonObject()
                .getAsJsonArray("ranges");
        encodedRanges.add(encodedRanges.remove(0));

        Assertions.assertEquals(
                RangeDistributionMigrationService.finalDigestForTest(request),
                RangeDistributionMigrationService.finalDigestForTest(changedIndexId));
        Assertions.assertEquals(
                RangeDistributionMigrationService.finalDigestForTest(request),
                RangeDistributionMigrationService.finalDigestForTest(reorderedRanges));
        Assertions.assertEquals("SUBMITTED", status(service().reconcile(encode(request))));
        SplitTabletJob job = jobs.last();
        long splitChildren = job.getReshardingPhysicalPartitions().values().stream()
                .flatMap(partition -> partition.getReshardingIndexes().values().stream())
                .flatMap(index -> index.getReshardingTablets().stream())
                .filter(tablet -> tablet.getSplittingTablet() != null)
                .mapToLong(tablet -> tablet.getSplittingTablet().getNewTabletRanges().size())
                .sum();
        Assertions.assertEquals(2, splitChildren);

        String firstFinalDigest = job.getExternalFinalDigest();
        String firstStepDigest = job.getExternalStepDigest();
        List<TabletRange> immediate = splittingRanges(job).values().iterator().next();
        PhysicalPartition physical = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        MaterializedIndex replacement = replaceLatestIndex(physical, immediate);
        jobs.jobs.clear();
        JsonObject refreshed = decodedRequest("fanout", finalRanges);
        refreshed.getAsJsonArray("targets").get(0).getAsJsonObject()
                .addProperty("currentIndexId", replacement.getId());
        Assertions.assertEquals("SUBMITTED", status(service().reconcile(encode(refreshed))));
        Assertions.assertEquals(firstFinalDigest, jobs.last().getExternalFinalDigest());
        Assertions.assertNotEquals(firstStepDigest, jobs.last().getExternalStepDigest());
    }

    @Test
    void followerAndClosedAdmissionAreRetryableWithoutMutation() {
        RangeDistributionMigrationService closed = new RangeDistributionMigrationService(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getLocalMetastore(),
                jobs, () -> false, () -> { });
        JsonObject response = response(closed.reconcile(request("closed", twoRanges())));
        Assertions.assertEquals("RETRYABLE_BUSY", response.get("status").getAsString());
        Assertions.assertTrue(response.get("message").getAsString().contains("NOT_LEADER"));
        Assertions.assertTrue(jobs.jobs.isEmpty());
    }

    @Test
    void externalFieldsPersistAndPlanningRaceFailsBusy() {
        Assertions.assertEquals("SUBMITTED", status(service().reconcile(request("persist", twoRanges()))));
        SplitTabletJob restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(jobs.last()), SplitTabletJob.class);
        Assertions.assertEquals("persist", restored.getExternalRequestId());
        Assertions.assertEquals(jobs.last().getExternalFinalDigest(), restored.getExternalFinalDigest());
        Assertions.assertEquals(jobs.last().getExternalStepDigest(), restored.getExternalStepDigest());

        jobs.jobs.clear();
        AdmissionJobs admissionJobs = new AdmissionJobs();
        RangeDistributionMigrationService raced = new RangeDistributionMigrationService(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getLocalMetastore(), admissionJobs,
                () -> true, () -> table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE));
        Assertions.assertEquals("RETRYABLE_BUSY", status(raced.reconcile(request("race", twoRanges()))));
        Assertions.assertTrue(admissionJobs.jobs.isEmpty());
    }

    @Test
    void writeAdmissionRejectsTouchedAndInitiallyAlignedGroupRaces() {
        AdmissionJobs touchedJobs = new AdmissionJobs();
        PhysicalPartition touched = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        RangeDistributionMigrationService touchedRace = new RangeDistributionMigrationService(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getLocalMetastore(), touchedJobs,
                () -> true, () -> replaceLatestIndex(touched, List.of(new TabletRange())));
        Assertions.assertEquals("RETRYABLE_BUSY",
                status(touchedRace.reconcile(request("touched-race", twoRanges()))));
        Assertions.assertTrue(touchedJobs.jobs.isEmpty());

        // Use a fresh table because the first race intentionally replaced its latest physical index.
        table.setState(OlapTable.OlapTableState.NORMAL);
        PhysicalPartition aligned = addSecondLogicalPartition();
        JsonObject complete = decodedCatalogRequest("aligned-race", twoRanges());
        AdmissionJobs alignedJobs = new AdmissionJobs();
        RangeDistributionMigrationService alignedRace = new RangeDistributionMigrationService(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getLocalMetastore(), alignedJobs,
                () -> true, () -> replaceLatestIndex(aligned, List.of(new TabletRange())));
        Assertions.assertEquals("RETRYABLE_BUSY", status(alignedRace.reconcile(encode(complete))));
        Assertions.assertTrue(alignedJobs.jobs.isEmpty());
    }

    @Test
    void writeAdmissionRejectsTableRenameRaceBeforeMutation() {
        AdmissionJobs admissionJobs = new AdmissionJobs();
        RangeDistributionMigrationService raced = new RangeDistributionMigrationService(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getLocalMetastore(), admissionJobs,
                () -> true, () -> table.setName(table.getName() + "_renamed"));

        Assertions.assertEquals("RETRYABLE_BUSY",
                status(raced.reconcile(request("table-rename-race", twoRanges()))));
        Assertions.assertTrue(admissionJobs.jobs.isEmpty());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    void writeAdmissionRejectsRollupRenameRaceBeforeMutation() {
        addVisibleRollupToEveryPhysicalPartition();
        JsonObject complete = decodedCatalogRequest("rollup-rename-race", twoRanges());
        String rollupName = table.getIndexNameToMetaId().keySet().stream()
                .filter(name -> !name.equals(table.getName()))
                .findFirst().orElseThrow();
        AdmissionJobs admissionJobs = new AdmissionJobs();
        RangeDistributionMigrationService raced = new RangeDistributionMigrationService(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getLocalMetastore(), admissionJobs,
                () -> true, () -> table.renameIndexForSchemaChange(rollupName, rollupName + "_renamed"));

        Assertions.assertEquals("RETRYABLE_BUSY", status(raced.reconcile(encode(complete))));
        Assertions.assertTrue(admissionJobs.jobs.isEmpty());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    private RangeDistributionMigrationService service() {
        return new RangeDistributionMigrationService(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getLocalMetastore(),
                jobs, () -> true, () -> { });
    }

    private Tablet onlyTablet() {
        return latestIndex().getTablets().get(0);
    }

    private MaterializedIndex latestIndex() {
        PhysicalPartition physical = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        return physical.getLatestBaseIndex();
    }

    private void useSingleKeyTable(String suffix, String keyType) throws Exception {
        String tableName = table.getName() + '_' + suffix;
        starRocksAssert.withTable("create table " + tableName
                + " (k1 " + keyType + ", v bigint) duplicate key(k1) order by(k1) "
                + "properties('replication_num'='1')");
        table = (OlapTable) db.getTable(tableName);
    }

    private String request(String requestId, List<TabletRange> ranges) {
        return encode(decodedRequest(requestId, ranges));
    }

    private JsonObject decodedRequest(String requestId, List<TabletRange> ranges) {
        JsonObject request = new JsonObject();
        request.addProperty("version", 1);
        request.addProperty("requestId", requestId);
        request.addProperty("databaseName", db.getFullName());
        request.addProperty("databaseId", db.getId());
        request.addProperty("tableName", table.getName());
        request.addProperty("tableId", table.getId());
        JsonObject target = new JsonObject();
        PhysicalPartition physical = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        target.addProperty("physicalPartitionId", physical.getId());
        target.addProperty("indexName", table.getName());
        target.addProperty("currentIndexId", latestIndex().getId());
        JsonArray encodedRanges = new JsonArray();
        ranges.forEach(range -> encodedRanges.add(range.toEncodedString()));
        target.add("ranges", encodedRanges);
        JsonArray targets = new JsonArray();
        targets.add(target);
        request.add("targets", targets);
        return request;
    }

    private JsonObject decodedCatalogRequest(String requestId, List<TabletRange> firstGroupOverride) {
        JsonObject request = new JsonObject();
        request.addProperty("version", 1);
        request.addProperty("requestId", requestId);
        request.addProperty("databaseName", db.getFullName());
        request.addProperty("databaseId", db.getId());
        request.addProperty("tableName", table.getName());
        request.addProperty("tableId", table.getId());
        JsonArray targets = new JsonArray();
        boolean first = true;
        List<PhysicalPartition> physicalPartitions = new ArrayList<>(table.getPhysicalPartitions());
        physicalPartitions.sort((left, right) -> Long.compare(left.getId(), right.getId()));
        for (PhysicalPartition physical : physicalPartitions) {
            List<MaterializedIndex> indices = new ArrayList<>(
                    physical.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE));
            indices.sort((left, right) -> Long.compare(left.getMetaId(), right.getMetaId()));
            for (MaterializedIndex index : indices) {
                JsonObject target = new JsonObject();
                target.addProperty("physicalPartitionId", physical.getId());
                target.addProperty("indexName", table.getIndexNameByMetaId(index.getMetaId()));
                target.addProperty("currentIndexId", index.getId());
                JsonArray encodedRanges = new JsonArray();
                List<TabletRange> ranges = first && firstGroupOverride != null
                        ? firstGroupOverride : index.getTablets().stream().map(Tablet::getRange).toList();
                ranges.forEach(range -> encodedRanges.add(range.toEncodedString()));
                target.add("ranges", encodedRanges);
                targets.add(target);
                first = false;
            }
        }
        request.add("targets", targets);
        return request;
    }

    private PhysicalPartition addSecondLogicalPartition() {
        long logicalId = GlobalStateMgr.getCurrentState().getNextId();
        long physicalId = GlobalStateMgr.getCurrentState().getNextId();
        MaterializedIndex index = new MaterializedIndex(
                GlobalStateMgr.getCurrentState().getNextId(), table.getBaseIndexMetaId(),
                IndexState.NORMAL, PhysicalPartition.INVALID_SHARD_GROUP_ID);
        index.addTablet(new LakeTablet(GlobalStateMgr.getCurrentState().getNextId(), new TabletRange()), null, false);
        Partition partition = new Partition(logicalId, physicalId, "synthetic_" + logicalId,
                index, new RangeDistributionInfo());
        table.addPartition(partition);
        return partition.getDefaultPhysicalPartition();
    }

    private void addVisibleRollupToEveryPhysicalPartition() {
        long metaId = GlobalStateMgr.getCurrentState().getNextId();
        table.setIndexMeta(metaId, "rollup_" + metaId, table.getBaseSchema(), 0, 0, (short) 2,
                TStorageType.COLUMN, table.getKeysType());
        for (PhysicalPartition physical : table.getPhysicalPartitions()) {
            MaterializedIndex rollup = new MaterializedIndex(
                    GlobalStateMgr.getCurrentState().getNextId(), metaId,
                    IndexState.NORMAL, PhysicalPartition.INVALID_SHARD_GROUP_ID);
            rollup.addTablet(new LakeTablet(
                    GlobalStateMgr.getCurrentState().getNextId(), new TabletRange()), null, false);
            physical.createRollupIndex(rollup);
        }
    }

    private Tablet addTablet(MaterializedIndex index, TabletRange range, boolean updateInvertedIndex) {
        PhysicalPartition physical = table.getPhysicalPartitions().stream()
                .filter(candidate -> candidate.getIndex(index.getId()) == index)
                .findFirst().orElseThrow();
        LakeTablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId(), range);
        TabletMeta meta = new TabletMeta(db.getId(), table.getId(), physical.getId(), index.getId(),
                TStorageMedium.HDD, true);
        index.addTablet(tablet, meta, updateInvertedIndex);
        return tablet;
    }

    private MaterializedIndex replaceLatestIndex(PhysicalPartition physical, List<TabletRange> ranges) {
        MaterializedIndex current = physical.getLatestBaseIndex();
        MaterializedIndex replacement = new MaterializedIndex(
                GlobalStateMgr.getCurrentState().getNextId(), current.getMetaId(),
                IndexState.NORMAL, PhysicalPartition.INVALID_SHARD_GROUP_ID);
        for (TabletRange range : ranges) {
            LakeTablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId(), range);
            replacement.addTablet(tablet, new TabletMeta(db.getId(), table.getId(), physical.getId(),
                    replacement.getId(), TStorageMedium.HDD, true), true);
        }
        physical.addMaterializedIndex(replacement, true);
        return replacement;
    }

    private static Map<Long, List<TabletRange>> splittingRanges(SplitTabletJob job) {
        Map<Long, List<TabletRange>> result = new LinkedHashMap<>();
        job.getReshardingPhysicalPartitions().values().stream()
                .flatMap(partition -> partition.getReshardingIndexes().values().stream())
                .flatMap(index -> index.getReshardingTablets().stream())
                .filter(tablet -> tablet.getSplittingTablet() != null)
                .forEach(tablet -> result.put(tablet.getFirstOldTabletId(),
                        List.copyOf(tablet.getSplittingTablet().getNewTabletRanges())));
        return result;
    }

    private static String encode(JsonObject request) {
        return encodeJson(request.toString());
    }

    private static String encodeJson(String json) {
        return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    private static JsonObject response(String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }

    private static String status(String json) {
        return response(json).get("status").getAsString();
    }

    private static List<TabletRange> twoRanges() {
        return List.of(range(null, tuple(0, "a")), range(tuple(0, "a"), null));
    }

    private static Tuple tuple(int integer, String string) {
        return new Tuple(List.of(
                Variant.of(IntegerType.INT, Integer.toString(integer)),
                string == null ? Variant.nullVariant(TypeFactory.createVarcharType(10))
                        : Variant.of(TypeFactory.createVarcharType(10), string)));
    }

    private static Tuple singleValueTuple(Variant value) {
        return new Tuple(List.of(value));
    }

    private static TabletRange range(Tuple lower, Tuple upper) {
        return new TabletRange(Range.of(lower, upper, lower != null, false));
    }

    private static void assertInvalid(List<TabletRange> ranges, List<Column> columns) {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> RangeDistributionMigrationService.validateRangeSequenceForTest(ranges, columns));
    }

    private static final class CapturingJobs implements RangeDistributionMigrationService.JobController {
        private final List<TabletReshardJob> jobs = new ArrayList<>();

        @Override
        public Collection<TabletReshardJob> jobs() {
            return jobs;
        }

        @Override
        public void submit(TabletReshardJob job) throws StarRocksException {
            jobs.add(job);
        }

        private SplitTabletJob last() {
            return (SplitTabletJob) jobs.get(jobs.size() - 1);
        }
    }

    private static final class AdmissionJobs implements RangeDistributionMigrationService.JobController {
        private final List<TabletReshardJob> jobs = new ArrayList<>();

        @Override
        public Collection<TabletReshardJob> jobs() {
            return jobs;
        }

        @Override
        public void submit(TabletReshardJob job) throws StarRocksException {
            job.init();
            jobs.add(job);
        }
    }
}
