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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DecimalVariant;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaxVariant;
import com.starrocks.catalog.MinVariant;
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.ScalarType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;

/**
 * Reconciles the complete range-tablet topology supplied by the cross-cluster migration tool.
 *
 * <p>The entry point deliberately returns a small JSON state machine instead of throwing expected
 * compatibility or admission failures through the ADMIN EXECUTE Groovy binding. The supplied
 * topology contains stable catalog identities and encoded ranges only; parent tablet ids are always
 * resolved from the current target catalog.
 */
public final class RangeDistributionMigrationService {
    private static final Logger LOG = LogManager.getLogger(RangeDistributionMigrationService.class);
    private static final int VERSION = 1;
    private static final Pattern JSON_INTEGER = Pattern.compile("-?(0|[1-9][0-9]*)");
    private static final Set<String> REQUEST_FIELDS = Set.of(
            "version", "requestId", "databaseName", "databaseId", "tableName", "tableId", "targets");
    private static final Set<String> TARGET_FIELDS = Set.of(
            "physicalPartitionId", "indexName", "currentIndexId", "ranges");

    enum Status {
        ALIGNED,
        SUBMITTED,
        RUNNING,
        RETRYABLE_BUSY,
        INCOMPATIBLE,
        FAILED
    }

    interface JobController {
        Collection<TabletReshardJob> jobs();

        void submit(TabletReshardJob job) throws StarRocksException;
    }

    record CurrentTablet(long tabletId, TabletRange range) {
    }

    private record GroupKey(long physicalPartitionId, String indexName) implements Comparable<GroupKey> {
        @Override
        public int compareTo(GroupKey other) {
            int partitionComparison = Long.compare(physicalPartitionId, other.physicalPartitionId);
            return partitionComparison != 0 ? partitionComparison : indexName.compareTo(other.indexName);
        }
    }

    private record RequestedGroup(GroupKey key, long currentIndexId, List<TabletRange> ranges) {
    }

    private record PreparedRequest(Request request, Map<GroupKey, RequestedGroup> requestedGroups,
                                   String finalDigest) {
    }

    private record CurrentGroup(GroupKey key, long indexMetaId, long currentIndexId,
                                List<CurrentTablet> tablets, List<Column> rangeColumns) {
    }

    private record Plan(Request request, Database database, OlapTable table, String finalDigest,
                        String stepDigest, Map<Long, List<TabletRange>> splits,
                        Set<SplitTabletJob.ExternalAdmissionGroup> admissionGroups) {
    }

    private static final class Request {
        private Integer version;
        private String requestId;
        private String databaseName;
        private Long databaseId;
        private String tableName;
        private Long tableId;
        private List<Target> targets;
    }

    private static final class Target {
        private Long physicalPartitionId;
        private String indexName;
        private Long currentIndexId;
        private List<String> ranges;
    }

    private static final class BadRequestException extends RuntimeException {
        private BadRequestException(String message) {
            super(message);
        }

        private BadRequestException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final class IncompatibleException extends RuntimeException {
        private IncompatibleException(String message) {
            super(message);
        }
    }

    private static final class BadBoundaryValueException extends IllegalArgumentException {
        private BadBoundaryValueException(String message) {
            super(message);
        }

        private BadBoundaryValueException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final class BusyException extends RuntimeException {
        private BusyException(String message) {
            super(message);
        }
    }

    private final GlobalStateMgr stateMgr;
    private final LocalMetastore metastore;
    private final JobController jobController;
    private final BooleanSupplier leaderAdmissionOpen;
    private final Runnable beforeSubmitHook;

    public RangeDistributionMigrationService(GlobalStateMgr stateMgr, LocalMetastore metastore) {
        this(stateMgr, metastore, new JobController() {
            @Override
            public Collection<TabletReshardJob> jobs() {
                return stateMgr.getTabletReshardJobMgr().getTabletReshardJobs().values();
            }

            @Override
            public void submit(TabletReshardJob job) throws StarRocksException {
                stateMgr.getTabletReshardJobMgr().addTabletReshardJob(job);
            }
        }, () -> stateMgr.isLeader() && stateMgr.isLeaderWorkAdmissionOpen(), () -> { });
    }

    @VisibleForTesting
    RangeDistributionMigrationService(GlobalStateMgr stateMgr, LocalMetastore metastore,
                                      JobController jobController, BooleanSupplier leaderAdmissionOpen,
                                      Runnable beforeSubmitHook) {
        this.stateMgr = Objects.requireNonNull(stateMgr);
        this.metastore = Objects.requireNonNull(metastore);
        this.jobController = Objects.requireNonNull(jobController);
        this.leaderAdmissionOpen = Objects.requireNonNull(leaderAdmissionOpen);
        this.beforeSubmitHook = Objects.requireNonNull(beforeSubmitHook);
    }

    /** Returns one line of version-1 JSON for every expected or malformed input. */
    public String reconcile(String encodedRequest) {
        Request request = null;
        String finalDigest = "";
        try {
            if (!leaderAdmissionOpen.getAsBoolean()) {
                return respond(null, "", Status.RETRYABLE_BUSY, 0, "NOT_LEADER: leader work admission is closed");
            }
            request = decodeRequest(encodedRequest);
            PreparedRequest prepared = prepareRequest(request);
            finalDigest = prepared.finalDigest();
            JsonResponse retained = inspectRetainedJobs(request, finalDigest);
            if (retained != null) {
                return respond(request, finalDigest, retained.status(), retained.jobId(), retained.message());
            }
            Plan plan = plan(prepared);
            retained = inspectRetainedJobs(request, finalDigest);
            if (retained != null) {
                return respond(request, finalDigest, retained.status(), retained.jobId(), retained.message());
            }
            if (plan.table().getState() != OlapTable.OlapTableState.NORMAL) {
                throw new BusyException("Table is not NORMAL: " + plan.table().getState());
            }
            if (plan.splits().isEmpty()) {
                return respond(request, finalDigest, Status.ALIGNED, 0, "");
            }

            SplitTabletJob job;
            try {
                job = (SplitTabletJob) SplitTabletJobFactory.forExternalBoundaries(
                        plan.database(), plan.table(), plan.splits());
            } catch (StarRocksException | IllegalArgumentException e) {
                throw new BusyException("Catalog changed while building split plan: " + e.getMessage());
            }
            job.setExternalIdentity(request.requestId, finalDigest, plan.stepDigest());
            job.setExternalAdmissionSnapshot(new SplitTabletJob.ExternalAdmissionSnapshot(
                    request.databaseId, request.databaseName, request.tableId, request.tableName,
                    plan.admissionGroups()));

            beforeSubmitHook.run();
            if (!leaderAdmissionOpen.getAsBoolean()) {
                throw new BusyException("NOT_LEADER: leader work admission is closed");
            }
            try {
                jobController.submit(job);
            } catch (StarRocksException | RuntimeException e) {
                throw new BusyException("Split admission changed; refresh topology and retry: " + e.getMessage());
            }
            return respond(request, finalDigest, Status.SUBMITTED, job.getJobId(), "");
        } catch (BadRequestException e) {
            return respond(request, finalDigest, Status.FAILED, 0, safeMessage(e));
        } catch (IncompatibleException e) {
            return respond(request, finalDigest, Status.INCOMPATIBLE, 0, safeMessage(e));
        } catch (BusyException e) {
            return respond(request, finalDigest, Status.RETRYABLE_BUSY, 0, safeMessage(e));
        } catch (RuntimeException e) {
            LOG.warn("Range migration reconcile failed before mutation", e);
            return respond(request, finalDigest, Status.FAILED, 0, safeMessage(e));
        }
    }

    private PreparedRequest prepareRequest(Request request) {
        Map<GroupKey, RequestedGroup> decodedGroups = decodeRequestedGroups(request);
        Map<GroupKey, RequestedGroup> requestedGroups = new LinkedHashMap<>();
        Map<GroupKey, List<TabletRange>> desiredTopology = new LinkedHashMap<>();
        for (RequestedGroup decoded : decodedGroups.values()) {
            List<TabletRange> ranges = List.copyOf(decoded.ranges());
            RequestedGroup requested = new RequestedGroup(decoded.key(), decoded.currentIndexId(), ranges);
            requestedGroups.put(requested.key(), requested);
            desiredTopology.put(requested.key(), requested.ranges());
        }
        return new PreparedRequest(request, Map.copyOf(requestedGroups), finalDigest(request, desiredTopology));
    }

    private Plan plan(PreparedRequest prepared) {
        Request request = prepared.request();
        try (AutoCloseableLock ignored =
                     new AutoCloseableLock(request.databaseId, request.tableId, LockType.READ)) {
            Database database = metastore.getDb(request.databaseId);
            if (database == null || !database.getFullName().equals(request.databaseName)
                    || metastore.getDb(request.databaseName) != database) {
                throw new IncompatibleException("Database name/id identity does not match");
            }
            Table resolvedById = metastore.getTable(request.databaseId, request.tableId);
            Table resolvedByName = metastore.getTable(request.databaseName, request.tableName);
            if (!(resolvedById instanceof OlapTable) || resolvedById != resolvedByName
                    || !resolvedById.getName().equals(request.tableName)) {
                throw new IncompatibleException("Table name/id identity does not match an OLAP table");
            }
            OlapTable table = (OlapTable) resolvedById;
            validateTableScope(table);
            Map<GroupKey, RequestedGroup> requestedGroups = prepared.requestedGroups();
            Map<GroupKey, CurrentGroup> currentGroups = collectCurrentGroups(table);
            if (!requestedGroups.keySet().equals(currentGroups.keySet())) {
                throw new IncompatibleException("Target group snapshot is incomplete or stale");
            }
            Set<SplitTabletJob.ExternalAdmissionGroup> admissionGroups = new LinkedHashSet<>();
            Map<Long, List<TabletRange>> splits = new LinkedHashMap<>();
            List<GroupKey> orderedKeys = new ArrayList<>(currentGroups.keySet());
            orderedKeys.sort(Comparator.naturalOrder());
            for (GroupKey key : orderedKeys) {
                RequestedGroup requested = requestedGroups.get(key);
                CurrentGroup current = currentGroups.get(key);
                validateSupportedRangeColumns(current.rangeColumns(), key);
                List<TabletRange> desired;
                List<CurrentTablet> tablets;
                try {
                    desired = sortedRanges(requested.ranges());
                    validateRangeSequence(desired, current.rangeColumns());
                } catch (BadBoundaryValueException e) {
                    throw new BadRequestException("Invalid requested range topology for " + formatKey(key)
                            + ": " + e.getMessage(), e);
                } catch (IllegalArgumentException e) {
                    throw new IncompatibleException("Invalid range topology for " + formatKey(key)
                            + ": " + e.getMessage());
                }
                if (requested.currentIndexId() != current.currentIndexId()) {
                    throw new IncompatibleException("Current index id is stale for " + formatKey(key));
                }
                try {
                    validateRangeBounds(
                            current.tablets().stream().map(CurrentTablet::range).toList(), current.rangeColumns());
                    tablets = sortedTablets(current.tablets());
                    validateRangeSequence(
                            tablets.stream().map(CurrentTablet::range).toList(), current.rangeColumns());
                } catch (IllegalArgumentException e) {
                    throw new IncompatibleException("Invalid range topology for " + formatKey(key)
                            + ": " + e.getMessage());
                }
                admissionGroups.add(new SplitTabletJob.ExternalAdmissionGroup(
                        key.physicalPartitionId(), key.indexName(),
                        current.indexMetaId(), current.currentIndexId()));

                Map<Long, List<TabletRange>> groupPlan;
                try {
                    groupPlan = planRefinement(tablets, desired, Config.tablet_reshard_max_split_count);
                } catch (IllegalArgumentException e) {
                    throw new IncompatibleException("Target is not an exact refinement for " + formatKey(key)
                            + ": " + e.getMessage());
                }
                for (Map.Entry<Long, List<TabletRange>> entry : groupPlan.entrySet()) {
                    if (splits.put(entry.getKey(), entry.getValue()) != null) {
                        throw new IllegalStateException("Duplicate current tablet id " + entry.getKey());
                    }
                }
            }
            String stepDigest = stepDigest(splits);
            return new Plan(request, database, table, prepared.finalDigest(), stepDigest, splits, admissionGroups);
        }
    }

    private JsonResponse inspectRetainedJobs(Request request, String finalDigest) {
        JsonResponse matching = null;
        for (TabletReshardJob retained : List.copyOf(jobController.jobs())) {
            if (retained.getTableId() != request.tableId) {
                continue;
            }
            if (retained instanceof SplitTabletJob split && split.getExternalRequestId() != null
                    && split.getExternalRequestId().equals(request.requestId)) {
                if (!Objects.equals(split.getExternalFinalDigest(), finalDigest)) {
                    throw new IncompatibleException("Request id is already bound to a different final topology");
                }
                if (!split.isDone()) {
                    matching = new JsonResponse(Status.RUNNING, split.getJobId(), "");
                    continue;
                }
            }
            if (!retained.isDone()) {
                throw new BusyException("Another tablet reshard job is active for this table");
            }
        }
        return matching;
    }

    private static void validateSupportedRangeColumns(List<Column> columns, GroupKey key) {
        if (columns.stream().anyMatch(column -> column.getType().isBinaryType())) {
            throw new IncompatibleException(
                    "Range migration does not support VARBINARY distribution columns for " + formatKey(key));
        }
    }

    private record JsonResponse(Status status, long jobId, String message) {
    }

    private void validateTableScope(OlapTable table) {
        if (!table.isCloudNativeTableOrMaterializedView() || !table.isRangeDistribution()) {
            throw new IncompatibleException("Range migration requires a shared-data range-distribution table");
        }
        if (table.hasColocateGroup() || stateMgr.getColocateTableIndex().isColocateTable(table.getId())) {
            throw new IncompatibleException("Range migration does not support colocate tables");
        }
        if (table.getState() != OlapTable.OlapTableState.NORMAL
                && table.getState() != OlapTable.OlapTableState.TABLET_RESHARD) {
            throw new BusyException("Table is not NORMAL: " + table.getState());
        }
        for (Partition partition : table.getPartitions()) {
            if (partition.getSubPartitions().size() != 1) {
                throw new IncompatibleException(
                        "Range migration requires one physical partition per logical partition");
            }
        }
    }

    private Map<GroupKey, RequestedGroup> decodeRequestedGroups(Request request) {
        Map<GroupKey, RequestedGroup> groups = new LinkedHashMap<>();
        for (Target target : request.targets) {
            if (target == null || target.physicalPartitionId == null || target.currentIndexId == null
                    || Strings.isNullOrEmpty(target.indexName) || target.ranges == null || target.ranges.isEmpty()) {
                throw new BadRequestException("Every target must contain stable ids, index name, and ranges");
            }
            GroupKey key = new GroupKey(target.physicalPartitionId, target.indexName);
            List<TabletRange> decoded = new ArrayList<>(target.ranges.size());
            for (String encodedRange : target.ranges) {
                try {
                    decoded.add(TabletRange.fromEncodedString(encodedRange));
                } catch (IllegalArgumentException e) {
                    throw new BadRequestException("Invalid encoded tablet range for " + formatKey(key), e);
                }
            }
            if (groups.put(key, new RequestedGroup(key, target.currentIndexId, decoded)) != null) {
                throw new BadRequestException("Duplicate target group " + formatKey(key));
            }
        }
        return groups;
    }

    private Map<GroupKey, CurrentGroup> collectCurrentGroups(OlapTable table) {
        Map<GroupKey, CurrentGroup> groups = new LinkedHashMap<>();
        for (Partition partition : table.getPartitions()) {
            PhysicalPartition physical = partition.getDefaultPhysicalPartition();
            for (MaterializedIndex index : physical.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                String indexName = table.getIndexNameByMetaId(index.getMetaId());
                if (indexName == null) {
                    throw new IncompatibleException("Latest index metadata name is missing");
                }
                GroupKey key = new GroupKey(physical.getId(), indexName);
                List<CurrentTablet> tablets = new ArrayList<>(index.getTablets().size());
                for (Tablet tablet : index.getTablets()) {
                    if (tablet.getRange() == null) {
                        throw new IncompatibleException("Current tablet has no range: " + tablet.getId());
                    }
                    tablets.add(new CurrentTablet(tablet.getId(), tablet.getRange()));
                }
                CurrentGroup group = new CurrentGroup(key, index.getMetaId(), index.getId(), tablets,
                        MetaUtils.getRangeDistributionColumns(table, index.getMetaId()));
                if (groups.put(key, group) != null) {
                    throw new IncompatibleException("Current catalog has duplicate stable group " + formatKey(key));
                }
            }
        }
        return groups;
    }

    private static Request decodeRequest(String encodedRequest) {
        if (encodedRequest == null || encodedRequest.isEmpty()) {
            throw new BadRequestException("Request is empty");
        }
        long maxEncodedLength = ((long) Config.thrift_max_frame_size + 2) / 3 * 4;
        if (encodedRequest.length() > maxEncodedLength || encodedRequest.length() % 4 != 0) {
            throw new BadRequestException("Request exceeds the bounded input size or is not padded Base64");
        }
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(encodedRequest);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException("Request is not standard Base64", e);
        }
        if (bytes.length > Config.thrift_max_frame_size) {
            throw new BadRequestException("Request exceeds the bounded input size");
        }
        if (!Base64.getEncoder().encodeToString(bytes).equals(encodedRequest)) {
            throw new BadRequestException("Request is not canonical padded Base64");
        }
        String json;
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            json = decoded.toString();
        } catch (CharacterCodingException e) {
            throw new BadRequestException("Request is not valid UTF-8", e);
        }
        Request request;
        try {
            try (JsonReader reader = new JsonReader(new StringReader(json))) {
                reader.setLenient(false);
                request = readRequest(reader);
                if (reader.peek() != JsonToken.END_DOCUMENT) {
                    throw new BadRequestException("Request JSON has trailing content");
                }
            }
        } catch (BadRequestException e) {
            throw e;
        } catch (IOException | IllegalStateException e) {
            throw new BadRequestException("Request is not valid JSON", e);
        }
        if (request.version == null || request.version != VERSION
                || Strings.isNullOrEmpty(request.requestId) || Strings.isNullOrEmpty(request.databaseName)
                || request.databaseId == null || Strings.isNullOrEmpty(request.tableName) || request.tableId == null
                || request.targets == null || request.targets.isEmpty()) {
            throw new BadRequestException("Missing request identity, version, or targets");
        }
        return request;
    }

    private static Request readRequest(JsonReader reader) throws IOException {
        requireToken(reader, JsonToken.BEGIN_OBJECT, "request");
        reader.beginObject();
        Request request = new Request();
        Set<String> seen = new HashSet<>();
        while (reader.hasNext()) {
            String name = reader.nextName();
            requireKnownUniqueField(name, REQUEST_FIELDS, seen, "request");
            switch (name) {
                case "version" -> request.version = readStrictInt(reader, name);
                case "requestId" -> request.requestId = readStrictString(reader, name);
                case "databaseName" -> request.databaseName = readStrictString(reader, name);
                case "databaseId" -> request.databaseId = readStrictLong(reader, name);
                case "tableName" -> request.tableName = readStrictString(reader, name);
                case "tableId" -> request.tableId = readStrictLong(reader, name);
                case "targets" -> request.targets = readTargets(reader);
                default -> throw new BadRequestException("Unknown request field: " + name);
            }
        }
        reader.endObject();
        return request;
    }

    private static List<Target> readTargets(JsonReader reader) throws IOException {
        requireToken(reader, JsonToken.BEGIN_ARRAY, "targets");
        reader.beginArray();
        List<Target> targets = new ArrayList<>();
        while (reader.hasNext()) {
            targets.add(readTarget(reader));
        }
        reader.endArray();
        return targets;
    }

    private static Target readTarget(JsonReader reader) throws IOException {
        requireToken(reader, JsonToken.BEGIN_OBJECT, "target");
        reader.beginObject();
        Target target = new Target();
        Set<String> seen = new HashSet<>();
        while (reader.hasNext()) {
            String name = reader.nextName();
            requireKnownUniqueField(name, TARGET_FIELDS, seen, "target");
            switch (name) {
                case "physicalPartitionId" -> target.physicalPartitionId = readStrictLong(reader, name);
                case "indexName" -> target.indexName = readStrictString(reader, name);
                case "currentIndexId" -> target.currentIndexId = readStrictLong(reader, name);
                case "ranges" -> target.ranges = readRangeStrings(reader);
                default -> throw new BadRequestException("Unknown target field: " + name);
            }
        }
        reader.endObject();
        return target;
    }

    private static List<String> readRangeStrings(JsonReader reader) throws IOException {
        requireToken(reader, JsonToken.BEGIN_ARRAY, "ranges");
        reader.beginArray();
        List<String> ranges = new ArrayList<>();
        while (reader.hasNext()) {
            ranges.add(readStrictString(reader, "range"));
        }
        reader.endArray();
        return ranges;
    }

    private static String readStrictString(JsonReader reader, String field) throws IOException {
        requireToken(reader, JsonToken.STRING, field);
        return reader.nextString();
    }

    private static int readStrictInt(JsonReader reader, String field) throws IOException {
        String literal = readIntegerLiteral(reader, field);
        try {
            return Integer.parseInt(literal);
        } catch (NumberFormatException e) {
            throw new BadRequestException("Integer field is out of range: " + field, e);
        }
    }

    private static long readStrictLong(JsonReader reader, String field) throws IOException {
        String literal = readIntegerLiteral(reader, field);
        try {
            return Long.parseLong(literal);
        } catch (NumberFormatException e) {
            throw new BadRequestException("Long field is out of range: " + field, e);
        }
    }

    private static String readIntegerLiteral(JsonReader reader, String field) throws IOException {
        requireToken(reader, JsonToken.NUMBER, field);
        String literal = reader.nextString();
        if (!JSON_INTEGER.matcher(literal).matches()) {
            throw new BadRequestException("Field must use integral decimal JSON syntax: " + field);
        }
        return literal;
    }

    private static void requireKnownUniqueField(
            String field, Set<String> allowed, Set<String> seen, String objectName) {
        if (!allowed.contains(field)) {
            throw new BadRequestException("Unknown " + objectName + " field: " + field);
        }
        if (!seen.add(field)) {
            throw new BadRequestException("Duplicate " + objectName + " field: " + field);
        }
    }

    private static void requireToken(JsonReader reader, JsonToken expected, String field) throws IOException {
        JsonToken actual = reader.peek();
        if (actual != expected) {
            throw new BadRequestException(
                    "Field has invalid JSON token: " + field + " (expected " + expected + ", got " + actual + ')');
        }
    }

    private String respond(Request request, String finalDigest, Status status, long jobId, String message) {
        JsonObject response = new JsonObject();
        response.addProperty("version", VERSION);
        response.addProperty("status", status.name());
        response.addProperty("jobId", jobId);
        response.addProperty("message", message == null ? "" : message);
        String requestId = request == null ? "" : request.requestId;
        long tableId = request == null || request.tableId == null ? 0 : request.tableId;
        LOG.info("Range migration reconcile requestId={}, finalDigest={}, tableId={}, status={}, jobId={}",
                requestId, finalDigest, tableId, status, jobId);
        return GsonUtils.GSON.toJson(response);
    }

    private static String safeMessage(RuntimeException exception) {
        String message = exception.getMessage();
        if (Strings.isNullOrEmpty(message)) {
            return exception.getClass().getSimpleName();
        }
        return message.length() <= 1024 ? message : message.substring(0, 1024);
    }

    private static List<TabletRange> sortedRanges(List<TabletRange> ranges) {
        List<TabletRange> sorted = new ArrayList<>(ranges);
        sorted.sort(RangeDistributionMigrationService::compareRanges);
        return List.copyOf(sorted);
    }

    private static List<CurrentTablet> sortedTablets(List<CurrentTablet> tablets) {
        List<CurrentTablet> sorted = new ArrayList<>(tablets);
        sorted.sort((left, right) -> compareRanges(left.range(), right.range()));
        return List.copyOf(sorted);
    }

    private static int compareRanges(TabletRange left, TabletRange right) {
        Range<Tuple> leftRange = left.getRange();
        Range<Tuple> rightRange = right.getRange();
        int lower = compareLower(leftRange, rightRange);
        if (lower != 0) {
            return lower;
        }
        return compareUpper(leftRange, rightRange);
    }

    private static int compareLower(Range<Tuple> left, Range<Tuple> right) {
        if (left.isMinimum() != right.isMinimum()) {
            return left.isMinimum() ? -1 : 1;
        }
        if (!left.isMinimum()) {
            int value = left.getLowerBound().compareTo(right.getLowerBound());
            if (value != 0) {
                return value;
            }
        }
        return Boolean.compare(!left.isLowerBoundIncluded(), !right.isLowerBoundIncluded());
    }

    private static int compareUpper(Range<Tuple> left, Range<Tuple> right) {
        if (left.isMaximum() != right.isMaximum()) {
            return left.isMaximum() ? 1 : -1;
        }
        if (!left.isMaximum()) {
            int value = left.getUpperBound().compareTo(right.getUpperBound());
            if (value != 0) {
                return value;
            }
        }
        return Boolean.compare(left.isUpperBoundIncluded(), right.isUpperBoundIncluded());
    }

    private static void validateRangeSequence(List<TabletRange> ranges, List<Column> columns) {
        validateRangeBounds(ranges, columns);
        if (ranges == null || ranges.isEmpty()) {
            throw new IllegalArgumentException("Range sequence is empty");
        }
        if (!ranges.get(0).getRange().isMinimum()
                || !ranges.get(ranges.size() - 1).getRange().isMaximum()) {
            throw new IllegalArgumentException("Range sequence must cover both infinities");
        }
        for (int i = 0; i < ranges.size(); i++) {
            Range<Tuple> range = ranges.get(i).getRange();
            validateTuple(range.getLowerBound(), columns);
            validateTuple(range.getUpperBound(), columns);
            if (!range.isMinimum() && !range.isLowerBoundIncluded()) {
                throw new IllegalArgumentException("Finite lower bound must be inclusive");
            }
            if (!range.isMaximum() && range.isUpperBoundIncluded()) {
                throw new IllegalArgumentException("Finite upper bound must be exclusive");
            }
            if (!range.isMinimum() && !range.isMaximum()) {
                int comparison = range.getLowerBound().compareTo(range.getUpperBound());
                if (comparison >= 0) {
                    throw new IllegalArgumentException("Range is inverted or empty");
                }
            }
            if (i == 0) {
                continue;
            }
            Range<Tuple> previous = ranges.get(i - 1).getRange();
            if (previous.isMaximum() || range.isMinimum()
                    || previous.getUpperBound().compareTo(range.getLowerBound()) != 0
                    || previous.isUpperBoundIncluded() == range.isLowerBoundIncluded()) {
                throw new IllegalArgumentException("Range sequence contains a gap, overlap, or duplicate");
            }
        }
    }

    private static void validateRangeBounds(List<TabletRange> ranges, List<Column> columns) {
        if (ranges == null || ranges.isEmpty()) {
            throw new IllegalArgumentException("Range sequence is empty");
        }
        for (TabletRange tabletRange : ranges) {
            if (tabletRange == null || tabletRange.getRange() == null) {
                throw new IllegalArgumentException("Range is missing");
            }
            Range<Tuple> range = tabletRange.getRange();
            validateTuple(range.getLowerBound(), columns);
            validateTuple(range.getUpperBound(), columns);
        }
    }

    private static void validateTuple(Tuple tuple, List<Column> columns) {
        if (tuple == null) {
            return;
        }
        List<Variant> values = tuple.getValues();
        if (values == null || values.size() != columns.size()) {
            throw new IllegalArgumentException("Range tuple arity does not match distribution columns");
        }
        for (int i = 0; i < values.size(); i++) {
            Variant value = values.get(i);
            if (value == null || !value.getType().equals(columns.get(i).getType())) {
                throw new IllegalArgumentException("Range tuple type does not match distribution column " + i);
            }
            if (value instanceof MinVariant || value instanceof MaxVariant) {
                throw new BadBoundaryValueException(
                        "Finite range tuple contains an infinity sentinel at column " + i);
            }
            if (value instanceof NullVariant) {
                continue;
            }
            if (value instanceof DecimalVariant) {
                validateDecimalValue(value, (ScalarType) columns.get(i).getType(), i);
            }
        }
    }

    private static void validateDecimalValue(Variant value, ScalarType targetType, int columnIndex) {
        try {
            BigDecimal exact = new BigDecimal(value.getStringValue())
                    .setScale(targetType.getScalarScale(), RoundingMode.UNNECESSARY);
            if (exact.precision() > targetType.getScalarPrecision()) {
                throw new BadBoundaryValueException(
                        "Decimal range value exceeds target precision at column " + columnIndex);
            }
        } catch (NumberFormatException | ArithmeticException e) {
            throw new BadBoundaryValueException(
                    "Decimal range value is not exactly representable at column " + columnIndex, e);
        }
    }

    private static Map<Long, List<TabletRange>> planRefinement(
            List<CurrentTablet> currentTablets, List<TabletRange> desiredRanges, int maxFanout) {
        if (maxFanout < 2) {
            throw new IllegalStateException("tablet_reshard_max_split_count must be at least 2");
        }
        Map<Long, List<TabletRange>> result = new LinkedHashMap<>();
        int desiredIndex = 0;
        for (CurrentTablet currentTablet : currentTablets) {
            Range<Tuple> parent = currentTablet.range().getRange();
            if (desiredIndex >= desiredRanges.size()
                    || !sameLower(parent, desiredRanges.get(desiredIndex).getRange())) {
                throw new IllegalArgumentException("Desired topology would require merge or has incomplete coverage");
            }
            int start = desiredIndex;
            while (desiredIndex < desiredRanges.size()) {
                Range<Tuple> desired = desiredRanges.get(desiredIndex).getRange();
                int endpoint = compareUpperValue(desired, parent);
                if (endpoint > 0) {
                    throw new IllegalArgumentException("A desired range crosses a current parent and requires merge");
                }
                desiredIndex++;
                if (sameUpper(parent, desired)) {
                    break;
                }
                if (endpoint == 0) {
                    throw new IllegalArgumentException("Desired topology differs at a current parent boundary");
                }
            }
            if (desiredIndex == 0 || !sameUpper(parent, desiredRanges.get(desiredIndex - 1).getRange())) {
                throw new IllegalArgumentException("Desired topology does not cover a current parent");
            }
            List<TabletRange> descendants = desiredRanges.subList(start, desiredIndex);
            if (descendants.size() == 1 && descendants.get(0).getRange().equals(parent)) {
                continue;
            }
            if (descendants.size() < 2) {
                throw new IllegalArgumentException("Desired topology is not an exact refinement");
            }
            result.put(currentTablet.tabletId(), coalesce(descendants, maxFanout));
        }
        if (desiredIndex != desiredRanges.size()) {
            throw new IllegalArgumentException("Desired topology extends beyond current coverage");
        }
        return result;
    }

    private static List<TabletRange> coalesce(List<TabletRange> descendants, int maxFanout) {
        int outputCount = Math.min(descendants.size(), maxFanout);
        if (outputCount == descendants.size()) {
            return List.copyOf(descendants);
        }
        List<TabletRange> result = new ArrayList<>(outputCount);
        int offset = 0;
        for (int output = 0; output < outputCount; output++) {
            int remaining = descendants.size() - offset;
            int slots = outputCount - output;
            int take = (remaining + slots - 1) / slots;
            Range<Tuple> first = descendants.get(offset).getRange();
            Range<Tuple> last = descendants.get(offset + take - 1).getRange();
            result.add(new TabletRange(Range.of(first.getLowerBound(), last.getUpperBound(),
                    first.isLowerBoundIncluded(), last.isUpperBoundIncluded())));
            offset += take;
        }
        return List.copyOf(result);
    }

    private static boolean sameLower(Range<Tuple> left, Range<Tuple> right) {
        return left.isMinimum() == right.isMinimum()
                && (left.isMinimum() || Objects.equals(left.getLowerBound(), right.getLowerBound()))
                && left.isLowerBoundIncluded() == right.isLowerBoundIncluded();
    }

    private static boolean sameUpper(Range<Tuple> left, Range<Tuple> right) {
        return left.isMaximum() == right.isMaximum()
                && (left.isMaximum() || Objects.equals(left.getUpperBound(), right.getUpperBound()))
                && left.isUpperBoundIncluded() == right.isUpperBoundIncluded();
    }

    private static int compareUpperValue(Range<Tuple> left, Range<Tuple> right) {
        if (left.isMaximum() != right.isMaximum()) {
            return left.isMaximum() ? 1 : -1;
        }
        return left.isMaximum() ? 0 : left.getUpperBound().compareTo(right.getUpperBound());
    }

    private static String finalDigest(Request request, Map<GroupKey, List<TabletRange>> topology) {
        DigestBuilder digest = new DigestBuilder();
        digest.add(request.databaseName).add(request.databaseId).add(request.tableName).add(request.tableId);
        List<GroupKey> keys = new ArrayList<>(topology.keySet());
        keys.sort(Comparator.naturalOrder());
        for (GroupKey key : keys) {
            digest.add(key.physicalPartitionId()).add(key.indexName());
            List<String> encodedRanges = topology.get(key).stream()
                    .map(TabletRange::toEncodedString)
                    .sorted()
                    .toList();
            for (String encodedRange : encodedRanges) {
                digest.add(encodedRange);
            }
        }
        return digest.finish();
    }

    private static String stepDigest(Map<Long, List<TabletRange>> splits) {
        DigestBuilder digest = new DigestBuilder();
        List<Long> parentIds = new ArrayList<>(splits.keySet());
        parentIds.sort(Comparator.naturalOrder());
        for (Long parentId : parentIds) {
            digest.add(parentId);
            for (TabletRange range : splits.get(parentId)) {
                digest.add(range.toEncodedString());
            }
        }
        return digest.finish();
    }

    private static final class DigestBuilder {
        private final MessageDigest digest;

        private DigestBuilder() {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 is unavailable", e);
            }
        }

        private DigestBuilder add(long value) {
            return add(Long.toString(value));
        }

        private DigestBuilder add(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
            digest.update(bytes);
            return this;
        }

        private String finish() {
            return java.util.HexFormat.of().formatHex(digest.digest());
        }
    }

    private static String formatKey(GroupKey key) {
        return '(' + Long.toString(key.physicalPartitionId()) + ',' + key.indexName() + ')';
    }

    @VisibleForTesting
    static void validateRangeSequenceForTest(List<TabletRange> ranges, List<Column> columns) {
        validateRangeBounds(ranges, columns);
        validateRangeSequence(sortedRanges(ranges), columns);
    }

    @VisibleForTesting
    static Map<Long, List<TabletRange>> planRefinementForTest(
            List<CurrentTablet> currentTablets, List<TabletRange> desiredRanges, int maxFanout) {
        return planRefinement(sortedTablets(currentTablets), sortedRanges(desiredRanges), maxFanout);
    }

    @VisibleForTesting
    static String finalDigestForTest(JsonObject requestObject) {
        String encoded = Base64.getEncoder().encodeToString(
                requestObject.toString().getBytes(StandardCharsets.UTF_8));
        Request request = decodeRequest(encoded);
        Map<GroupKey, List<TabletRange>> topology = new LinkedHashMap<>();
        for (Target target : request.targets) {
            GroupKey key = new GroupKey(target.physicalPartitionId, target.indexName);
            List<TabletRange> ranges = target.ranges.stream()
                    .map(TabletRange::fromEncodedString).toList();
            if (topology.put(key, ranges) != null) {
                throw new IllegalArgumentException("Duplicate group");
            }
        }
        return finalDigest(request, topology);
    }
}
