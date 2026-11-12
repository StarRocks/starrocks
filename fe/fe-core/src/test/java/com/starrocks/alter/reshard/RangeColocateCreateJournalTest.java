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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.PlacementPolicy;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.common.io.Writable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.journal.JournalEntity;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.ColocateRangePersistInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * A CREATE TABLE that joins a range colocate group journals the group's ColocateRanges and its membership, while a
 * split of any member of the group can splice a bound into the ranges, journal them and mark every GroupId of the
 * group unstable, holding only the colocate index lock. Each test replays what the leader journaled onto a copy of
 * the leader's colocate index taken before the action, as a follower does, and checks that the follower ends where the
 * leader is (ColocateRanges, GroupIds, members, stable flags), and that no prefix of the journal leaves a member of a
 * range group without ColocateRanges. The races are driven deterministically by hooks in the CREATE's range record and
 * in the shard creation of its tablets.
 */
public class RangeColocateCreateJournalTest {
    private static final String DB_A = "rc_cj_a";
    private static final String DB_B = "rc_cj_b";

    /** The records a follower applies to its colocate index here, by name. */
    private static final Map<Short, String> TRACKED_OPS = Map.of(
            OperationType.OP_COLOCATE_RANGE_UPDATE, "RANGE_UPDATE",
            OperationType.OP_COLOCATE_ADD_TABLE_V2, "ADD_TABLE",
            OperationType.OP_COLOCATE_MARK_UNSTABLE_V2, "MARK_UNSTABLE",
            OperationType.OP_COLOCATE_MARK_STABLE_V2, "MARK_STABLE",
            OperationType.OP_DROP_TABLE_V2, "DROP_TABLE");

    private static final Type COLOCATE_RANGE_LIST = new TypeToken<List<ColocateRange>>() { }.getType();

    private static ConnectContext connectContext;
    private static boolean savedEnableRangeDistribution;

    /** Runs once, inside the next call of the createShards overload range colocate tablets are created through. */
    private static final AtomicReference<Runnable> ON_NEXT_RANGE_COLOCATE_CREATE_SHARDS = new AtomicReference<>();
    /** While not null, collects the shard groups of each shard created through that overload. */
    private static volatile List<List<Long>> createdShardGroups;
    /** The thread that runs the CREATE: only its range records fire the two hooks below. */
    private static volatile Thread hookThread;
    /** Runs once, right before the next range record of {@link #hookThread} is journaled. */
    private static final AtomicReference<Runnable> BEFORE_CREATE_RANGE_RECORD = new AtomicReference<>();
    /** Runs once, right after the next range record of {@link #hookThread} is committed. */
    private static final AtomicReference<Runnable> AFTER_CREATE_RANGE_RECORD = new AtomicReference<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        // The reshard daemon also runs the colocate checker, which would mark groups stable behind the test.
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(Invocation invocation, int numShards, FilePathInfo pathInfo,
                                           FileCacheInfo cacheInfo, List<Long> groupIds, List<Long> matchShardIds,
                                           Map<String, String> properties, ComputeResource computeResource) {
                fireOnce(ON_NEXT_RANGE_COLOCATE_CREATE_SHARDS);
                List<List<Long>> recorder = createdShardGroups;
                if (recorder != null) {
                    recorder.add(List.copyOf(groupIds));
                }
                return invocation.proceed();
            }
        };
        // CREATE journals its range record through this overload; the split splice uses the one with a WAL applier.
        new MockUp<EditLog>() {
            @Mock
            public void logColocateRangeUpdate(Invocation invocation, ColocateRangePersistInfo info) {
                boolean watched = Thread.currentThread() == hookThread;
                if (watched) {
                    fireOnce(BEFORE_CREATE_RANGE_RECORD);
                }
                invocation.proceed();
                if (watched) {
                    fireOnce(AFTER_CREATE_RANGE_RECORD);
                }
            }
        };
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.setUpForPersistTest();
        connectContext = UtFrameUtils.createDefaultCtx();
        savedEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
        new StarRocksAssert(connectContext).withDatabase(DB_A).withDatabase(DB_B);
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void resetHooks() {
        hookThread = Thread.currentThread();
        ON_NEXT_RANGE_COLOCATE_CREATE_SHARDS.set(null);
        createdShardGroups = null;
        BEFORE_CREATE_RANGE_RECORD.set(null);
        AFTER_CREATE_RANGE_RECORD.set(null);
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    @FunctionalInterface
    private interface Action {
        void run() throws Exception;
    }

    /** Runs the hook {@code hook} holds, if any, and clears it. */
    private static void fireOnce(AtomicReference<Runnable> hook) {
        Runnable action = hook.getAndSet(null);
        if (action != null) {
            action.run();
        }
    }

    private static ColocateTableIndex index() {
        return GlobalStateMgr.getCurrentState().getColocateTableIndex();
    }

    private static ReentrantReadWriteLock colocateLock() {
        return Deencapsulation.getField(index(), "lock");
    }

    private static Tuple k1(int v) {
        return new Tuple(List.of(Variant.of(IntegerType.INT, String.valueOf(v))));
    }

    /** The canonical lower bound (v, NULL) a split child at the colocate bound k1 = v carries. */
    private static Tuple canonicalLower(int v) {
        return new Tuple(List.of(Variant.of(IntegerType.INT, String.valueOf(v)), Variant.nullVariant(IntegerType.INT)));
    }

    private static OlapTable table(String dbName, String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbName, name);
    }

    private static void createTable(String dbName, String name, String group) throws Exception {
        String sql = "create table " + dbName + "." + name + " (k1 int, v int) duplicate key(k1) order by(k1) "
                + "properties('replication_num' = '1', 'colocate_with' = '" + group + ":k1')";
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .createTable((CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
    }

    private static final class Fixture {
        String group;
        long grpId;
        long packNew;
    }

    /**
     * Creates the member {@code m_<name>} of the group {@code g_<name>:k1} in each of {@code memberDbs} (one group
     * across the databases), gives the group the ColocateRanges (-inf, 100) and [100, +inf), each with its own PACK
     * shard group, marks every GroupId of the group stable, and allocates the PACK shard group a splice at 200 gives
     * the new range. The new ranges and the stable marks are not journaled.
     */
    private static Fixture group(String name, String... memberDbs) throws Exception {
        Fixture f = new Fixture();
        f.group = "g_" + name;
        for (String dbName : memberDbs) {
            createTable(dbName, "m_" + name, f.group);
        }
        OlapTable first = table(memberDbs[0], "m_" + name);
        long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(memberDbs[0]).getId();
        f.grpId = index().getGroup(first.getId()).grpId;
        long packInit = index().getColocateRanges(f.grpId).get(0).getShardGroupId();
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        long packHigh = starOSAgent.createShardGroup(dbId, first.getId(), 0L, 0L, PlacementPolicy.PACK);
        f.packNew = starOSAgent.createShardGroup(dbId, first.getId(), 0L, 0L, PlacementPolicy.PACK);
        index().getColocateRangeMgr().setColocateRanges(f.grpId, List.of(
                new ColocateRange(Range.lt(k1(100)), packInit),
                new ColocateRange(Range.ge(k1(100)), packHigh)));
        index().markAllGroupsWithSameColocateGroupIdStable(f.grpId, false);
        Assertions.assertEquals(memberDbs.length, index().getAllGroupIdsWithSameColocateGroupId(f.grpId).size());
        return f;
    }

    /**
     * What a split of a member publishing children at k1 = 200 does (SplitTabletJob.applyColocateRangeSplitResult):
     * splices the bound 200, journals the ranges and marks every GroupId of the group unstable.
     */
    private static void splice200(Fixture f) {
        try {
            index().applyRangeSplitResult(f.grpId, Set.of(canonicalLower(200)), 1, () -> f.packNew);
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    /** A thread running an action, which keeps whatever the action throws. */
    private static final class Worker {
        private final Thread thread;
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        /** Whether {@link #startAndAwaitFinishedOrBlocked} saw the worker finish while the CREATE was paused. */
        private boolean finishedWhileCreatePaused;

        Worker(String name, Action action) {
            thread = new Thread(() -> {
                try {
                    action.run();
                } catch (Throwable t) {
                    error.set(t);
                }
            }, name);
        }

        /**
         * Starts the worker from the paused CREATE and waits until it finished or, if the CREATE holds the colocate
         * index lock, until it waits for that lock.
         */
        void startAndAwaitFinishedOrBlocked() {
            boolean held = colocateLock().isWriteLockedByCurrentThread();
            thread.start();
            Awaitility.await().atMost(60, TimeUnit.SECONDS)
                    .until(() -> !thread.isAlive() || (held && colocateLock().hasQueuedThread(thread)));
            finishedWhileCreatePaused = !thread.isAlive();
        }

        void join() throws InterruptedException {
            thread.join(60_000L);
            Assertions.assertFalse(thread.isAlive(), thread.getName() + " is still running");
            if (error.get() != null) {
                Assertions.fail(thread.getName() + " failed", error.get());
            }
        }
    }

    /** A follower's colocate index after replaying what an action of the leader journaled. */
    private static final class FollowerReplay {
        private final ColocateTableIndex follower;
        /** The tracked records the action journaled, in journal order. */
        private final List<JournalEntity> ops;
        /** One entry per record after which a GroupId of the group had members but the group had no ranges. */
        private final List<String> membersWithoutRanges;

        FollowerReplay(ColocateTableIndex follower, List<JournalEntity> ops, List<String> membersWithoutRanges) {
            this.follower = follower;
            this.ops = ops;
            this.membersWithoutRanges = membersWithoutRanges;
        }
    }

    private static FollowerReplay replayOnFollower(Action action, LongSupplier grpId) throws Exception {
        return replayOnFollower(action, grpId, UnaryOperator.identity());
    }

    /**
     * Copies the leader's colocate index through an image, runs {@code action} on the leader, and applies the records
     * it journaled to the copy, each passed through {@code rewrite} first. After every record, checks that each
     * GroupId of the group {@code grpId} with members on the follower has ColocateRanges: a journal cut there (a crash
     * or a demotion of the leader) must not leave a member without them. {@code grpId} is read after the action, as a
     * CREATE of a new group allocates it.
     */
    private static FollowerReplay replayOnFollower(Action action, LongSupplier grpId,
                                                   UnaryOperator<JournalEntity> rewrite) throws Exception {
        // Loaded before the action: loading drops the members the live catalog no longer has.
        ColocateTableIndex follower = copyOfLeaderIndex();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        action.run();
        long checkedGrpId = grpId.getAsLong();
        List<JournalEntity> ops = UtFrameUtils.PseudoJournalReplayer.drainFollowerJournal().stream()
                .filter(op -> TRACKED_OPS.containsKey(op.opCode()))
                .collect(Collectors.toList());
        List<String> membersWithoutRanges = new ArrayList<>();
        for (int i = 0; i < ops.size(); i++) {
            apply(follower, rewrite.apply(ops.get(i)));
            if (!follower.getColocateRanges(checkedGrpId).isEmpty()) {
                continue;
            }
            for (GroupId groupId : follower.getAllGroupIdsWithSameColocateGroupId(checkedGrpId)) {
                if (!follower.getAllTableIds(groupId).isEmpty()) {
                    membersWithoutRanges.add(groupId + " after " + labels(ops.subList(0, i + 1)));
                }
            }
        }
        return new FollowerReplay(follower, ops, membersWithoutRanges);
    }

    private static ColocateTableIndex copyOfLeaderIndex() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        index().saveColocateTableIndexV2(image.getImageWriter());
        ColocateTableIndex copy = new ColocateTableIndex();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        copy.loadColocateTableIndexV2(reader);
        reader.close();
        return copy;
    }

    /** Applies a record to {@code follower} as the replay of that record does to the colocate index. */
    private static void apply(ColocateTableIndex follower, JournalEntity op) {
        switch (op.opCode()) {
            case OperationType.OP_COLOCATE_RANGE_UPDATE:
                follower.replayColocateRangeUpdate((ColocateRangePersistInfo) op.data());
                break;
            case OperationType.OP_COLOCATE_ADD_TABLE_V2:
                follower.replayAddTableToGroup((ColocatePersistInfo) op.data());
                break;
            case OperationType.OP_COLOCATE_MARK_UNSTABLE_V2:
                follower.replayMarkGroupUnstable((ColocatePersistInfo) op.data());
                break;
            case OperationType.OP_COLOCATE_MARK_STABLE_V2:
                follower.replayMarkGroupStable((ColocatePersistInfo) op.data());
                break;
            case OperationType.OP_DROP_TABLE_V2: {
                DropInfo info = (DropInfo) op.data();
                if (info.isForceDrop()) {
                    // LocalMetastore.replayDropTable -> table.delete(db, true) -> removeTableBinds(true)
                    follower.removeTable(info.getTableId(), null, true);
                }
                break;
            }
            default:
                break;
        }
    }

    /** Asserts that the action journaled exactly the records {@code expected}, by name, in this order. */
    private static void assertOps(FollowerReplay replay, String... expected) {
        Assertions.assertEquals(List.of(expected),
                replay.ops.stream().map(op -> TRACKED_OPS.get(op.opCode())).collect(Collectors.toList()),
                labels(replay.ops).toString());
    }

    private static List<String> labels(List<JournalEntity> ops) {
        return ops.stream().map(op -> {
            String name = TRACKED_OPS.get(op.opCode());
            if (op.data() instanceof ColocatePersistInfo) {
                return name + " " + ((ColocatePersistInfo) op.data()).getGroupId();
            }
            if (op.data() instanceof DropInfo) {
                return name + " " + ((DropInfo) op.data()).getTableId();
            }
            return name;
        }).collect(Collectors.toList());
    }

    /** Position of the first record of {@code opCode} at or after {@code from} whose payload matches, or -1. */
    private static int indexOf(List<JournalEntity> ops, int from, short opCode, Predicate<Writable> payload) {
        for (int i = from; i < ops.size(); i++) {
            if (ops.get(i).opCode() == opCode && payload.test(ops.get(i).data())) {
                return i;
            }
        }
        return -1;
    }

    /** Position of the first record of {@code opCode} about {@code groupId} at or after {@code from}, or -1. */
    private static int indexOf(List<JournalEntity> ops, int from, short opCode, GroupId groupId) {
        return indexOf(ops, from, opCode, payload -> ((ColocatePersistInfo) payload).getGroupId().equals(groupId));
    }

    /** The ColocateRanges an add-table record carries, read from its JSON (key "cr"). */
    private static List<ColocateRange> rangesCarriedBy(ColocatePersistInfo info) {
        JsonElement ranges = GsonUtils.GSON.toJsonTree(info).getAsJsonObject().get("cr");
        return ranges == null ? List.of() : GsonUtils.GSON.fromJson(ranges, COLOCATE_RANGE_LIST);
    }

    /** {@code op} as an FE without the "cr" field reads it: an add-table record loses the ranges it carries. */
    private static JournalEntity withoutCarriedRanges(JournalEntity op) {
        if (op.opCode() != OperationType.OP_COLOCATE_ADD_TABLE_V2) {
            return op;
        }
        JsonObject json = GsonUtils.GSON.toJsonTree(op.data()).getAsJsonObject();
        json.remove("cr");
        return new JournalEntity(op.opCode(), GsonUtils.GSON.fromJson(json, ColocatePersistInfo.class));
    }

    /**
     * Asserts that {@code ops} start with the range record of a CREATE of {@code table} in the group {@code grpId},
     * holding the leader's ColocateRanges, followed by its add-table record carrying the same ranges.
     */
    private static void assertCreateRecords(List<JournalEntity> ops, long grpId, OlapTable table) {
        ColocateRangePersistInfo rangeRecord = (ColocateRangePersistInfo) ops.get(0).data();
        Assertions.assertEquals(grpId, rangeRecord.getColocateGroupId());
        Assertions.assertEquals(index().getColocateRanges(grpId), rangeRecord.getColocateRanges());
        ColocatePersistInfo addTable = (ColocatePersistInfo) ops.get(1).data();
        Assertions.assertEquals(table.getId(), addTable.getTableId());
        Assertions.assertEquals(rangeRecord.getColocateRanges(), rangesCarriedBy(addTable),
                "the add-table record carries the ranges of the range record");
    }

    /**
     * Asserts that the follower has the leader's ColocateRanges of {@code grpId}, the same GroupIds of the group, and
     * for each of them the same stable flag and members, and that no prefix of the replayed journal left a member of
     * the group without ColocateRanges.
     */
    private static void assertFollowerConsistent(FollowerReplay replay, long grpId) {
        ColocateTableIndex follower = replay.follower;
        Assertions.assertEquals(index().getColocateRanges(grpId), follower.getColocateRanges(grpId),
                "ColocateRanges of group " + grpId);
        Set<GroupId> groupIds = new HashSet<>(index().getAllGroupIdsWithSameColocateGroupId(grpId));
        Assertions.assertEquals(groupIds, new HashSet<>(follower.getAllGroupIdsWithSameColocateGroupId(grpId)),
                "GroupIds of group " + grpId);
        for (GroupId groupId : groupIds) {
            Assertions.assertEquals(index().isGroupUnstable(groupId), follower.isGroupUnstable(groupId),
                    "unstable flag of " + groupId);
            Assertions.assertEquals(new HashSet<>(index().getAllTableIds(groupId)),
                    new HashSet<>(follower.getAllTableIds(groupId)), "members of " + groupId);
        }
        Assertions.assertEquals(List.of(), replay.membersWithoutRanges,
                "a prefix of the journal leaves a member of a range group without ColocateRanges");
    }

    // ---------------------------------------------------------------------------------------------
    // A split splices while the CREATE journals
    // ---------------------------------------------------------------------------------------------

    /**
     * A split of a member splices the bound 200 between the CREATE's read of the ranges and its range record: the
     * follower must end on the spliced ranges the leader has, not on the ranges the CREATE read.
     */
    @Test
    public void testSpliceDuringCreateRangeJournalKeepsLeaderRanges() throws Exception {
        Fixture f = group("1", DB_A, DB_B);
        // On its own thread: the colocate index lock is reentrant, so a splice on the CREATE's thread would pass it.
        Worker splice = new Worker("splice", () -> splice200(f));
        BEFORE_CREATE_RANGE_RECORD.set(splice::startAndAwaitFinishedOrBlocked);

        FollowerReplay replay = replayOnFollower(() -> {
            createTable(DB_A, "c1", f.group);
            splice.join();
        }, () -> f.grpId);

        Assertions.assertNull(BEFORE_CREATE_RANGE_RECORD.get(), "the hook ran");
        Assertions.assertEquals(3, index().getColocateRanges(f.grpId).size());
        assertFollowerConsistent(replay, f.grpId);
        Assertions.assertFalse(splice.finishedWhileCreatePaused,
                "the splice ran between the CREATE's read of the ranges and its range record");
    }

    /**
     * The CREATE is the first member of the group in its database, so it makes a new GroupId. A splice during its
     * tablet creation marks that GroupId unstable before it has a journaled member, which replay drops: the follower
     * must still end with the GroupId unstable, as the leader has it.
     */
    @Test
    public void testSpliceDuringCreateOfFirstMemberInDatabaseKeepsUnstable() throws Exception {
        Fixture f = group("2", DB_B);
        ON_NEXT_RANGE_COLOCATE_CREATE_SHARDS.set(() -> splice200(f));

        FollowerReplay replay = replayOnFollower(() -> createTable(DB_A, "c2", f.group), () -> f.grpId);

        Assertions.assertNull(ON_NEXT_RANGE_COLOCATE_CREATE_SHARDS.get(), "the hook ran");
        GroupId newGroupId = index().getGroup(table(DB_A, "c2").getId());
        GroupId peerGroupId = index().getGroup(table(DB_B, "m_2").getId());
        Assertions.assertNotEquals(peerGroupId, newGroupId);
        Assertions.assertTrue(index().isGroupUnstable(newGroupId));
        Assertions.assertTrue(index().isGroupUnstable(peerGroupId));
        Assertions.assertEquals(3, index().getColocateRanges(f.grpId).size());
        int mark = indexOf(replay.ops, 0, OperationType.OP_COLOCATE_MARK_UNSTABLE_V2, newGroupId);
        int add = indexOf(replay.ops, 0, OperationType.OP_COLOCATE_ADD_TABLE_V2, newGroupId);
        Assertions.assertTrue(mark >= 0 && mark < add,
                "the splice marked the new GroupId unstable before its add-table record: " + labels(replay.ops));
        assertFollowerConsistent(replay, f.grpId);
    }

    // ---------------------------------------------------------------------------------------------
    // What a CREATE journals without a race
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCreateInSameDatabaseJournalsCurrentRanges() throws Exception {
        Fixture f = group("3", DB_A);

        FollowerReplay replay = replayOnFollower(() -> createTable(DB_A, "c3", f.group), () -> f.grpId);

        assertFollowerConsistent(replay, f.grpId);
        assertOps(replay, "RANGE_UPDATE", "ADD_TABLE");
        Assertions.assertEquals(2, index().getColocateRanges(f.grpId).size());
        assertCreateRecords(replay.ops, f.grpId, table(DB_A, "c3"));
    }

    @Test
    public void testCreateNewGroupJournalsInitialRange() throws Exception {
        List<List<Long>> shardGroups = new CopyOnWriteArrayList<>();
        createdShardGroups = shardGroups;
        FollowerReplay replay = replayOnFollower(() -> createTable(DB_A, "c4", "g_new"),
                () -> index().getGroup(table(DB_A, "c4").getId()).grpId);

        OlapTable c4 = table(DB_A, "c4");
        long grpId = index().getGroup(c4.getId()).grpId;
        // The table's one tablet: its shard is created in the partition's shard group and in its range's PACK group.
        Assertions.assertEquals(1, shardGroups.size(), shardGroups.toString());
        Assertions.assertEquals(2, shardGroups.get(0).size(), shardGroups.toString());
        List<ColocateRange> initialRange = List.of(new ColocateRange(Range.all(), shardGroups.get(0).get(1)));
        Assertions.assertEquals(initialRange, index().getColocateRanges(grpId));
        assertFollowerConsistent(replay, grpId);
        assertOps(replay, "RANGE_UPDATE", "ADD_TABLE");
        // Both records hold the leader's ranges, which were just checked against the tablet's PACK shard group.
        assertCreateRecords(replay.ops, grpId, c4);
    }

    @Test
    public void testCreateIntoUnstableGroupKeepsUnstable() throws Exception {
        Fixture f = group("5", DB_A);
        GroupId groupId = index().getGroup(table(DB_A, "m_5").getId());
        index().markAllGroupsWithSameColocateGroupIdUnstable(f.grpId, true);
        Assertions.assertTrue(index().isGroupUnstable(groupId));

        FollowerReplay replay = replayOnFollower(() -> createTable(DB_A, "c5", f.group), () -> f.grpId);

        Assertions.assertTrue(index().isGroupUnstable(groupId));
        assertFollowerConsistent(replay, f.grpId);
        assertOps(replay, "RANGE_UPDATE", "ADD_TABLE", "MARK_UNSTABLE");
        assertCreateRecords(replay.ops, f.grpId, table(DB_A, "c5"));
        Assertions.assertEquals(groupId, ((ColocatePersistInfo) replay.ops.get(2).data()).getGroupId());
    }

    // ---------------------------------------------------------------------------------------------
    // The last other member is dropped while the CREATE journals
    // ---------------------------------------------------------------------------------------------

    /**
     * The only other member of the group is force dropped right after the CREATE's range record: its drop record
     * lands between the CREATE's range record and its add-table record, and its replay removes the group's ranges on a
     * follower, as no GroupId of the group is left there. The follower must still end with the ranges, and no prefix
     * of the journal may leave the CREATE's member without them.
     */
    @Test
    public void testLastOtherMemberDroppedDuringCreateKeepsRanges() throws Exception {
        Fixture f = group("6", DB_B);
        Database dbB = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_B);
        long droppedTableId = table(DB_B, "m_6").getId();
        Worker drop = new Worker("drop", () -> dbB.dropTable("m_6", false, true));
        AFTER_CREATE_RANGE_RECORD.set(drop::startAndAwaitFinishedOrBlocked);

        FollowerReplay replay = replayOnFollower(() -> {
            createTable(DB_A, "c6", f.group);
            drop.join();
        }, () -> f.grpId);

        Assertions.assertNull(AFTER_CREATE_RANGE_RECORD.get(), "the hook ran");
        int range = indexOf(replay.ops, 0, OperationType.OP_COLOCATE_RANGE_UPDATE, payload -> true);
        int dropped = indexOf(replay.ops, 0, OperationType.OP_DROP_TABLE_V2,
                payload -> ((DropInfo) payload).getTableId() == droppedTableId);
        int add = indexOf(replay.ops, 0, OperationType.OP_COLOCATE_ADD_TABLE_V2, payload -> true);
        Assertions.assertTrue(range >= 0 && range < dropped && dropped < add,
                "the drop record lands between the CREATE's range and add-table records: " + labels(replay.ops));
        Assertions.assertTrue(((DropInfo) replay.ops.get(dropped).data()).isForceDrop());
        GroupId groupId = index().getGroup(table(DB_A, "c6").getId());
        Assertions.assertEquals(2, index().getColocateRanges(f.grpId).size());
        Assertions.assertEquals(List.of(groupId), index().getAllGroupIdsWithSameColocateGroupId(f.grpId),
                "only the GroupId of c6 is left");
        assertFollowerConsistent(replay, f.grpId);
    }

    // ---------------------------------------------------------------------------------------------
    // The colocate checker marks the group stable around the CREATE
    // ---------------------------------------------------------------------------------------------

    /** A splice and a converged checker cycle both run before the CREATE journals: nothing is left unstable. */
    @Test
    public void testCheckerMarksStableBeforeCreateJournalsKeepsFollowerInSync() throws Exception {
        Fixture f = group("7", DB_B);
        ON_NEXT_RANGE_COLOCATE_CREATE_SHARDS.set(() -> {
            splice200(f);
            index().markAllGroupsWithSameColocateGroupIdStable(f.grpId, true);
        });

        FollowerReplay replay = replayOnFollower(() -> createTable(DB_A, "c7", f.group), () -> f.grpId);

        Assertions.assertNull(ON_NEXT_RANGE_COLOCATE_CREATE_SHARDS.get(), "the hook ran");
        GroupId groupId = index().getGroup(table(DB_A, "c7").getId());
        Assertions.assertEquals(2, index().getAllGroupIdsWithSameColocateGroupId(f.grpId).size());
        Assertions.assertFalse(index().isAnyGroupWithSameColocateGroupIdUnstable(f.grpId));
        int add = indexOf(replay.ops, 0, OperationType.OP_COLOCATE_ADD_TABLE_V2, groupId);
        Assertions.assertTrue(add >= 0, labels(replay.ops).toString());
        Assertions.assertEquals(-1, indexOf(replay.ops, add, OperationType.OP_COLOCATE_MARK_UNSTABLE_V2, groupId),
                labels(replay.ops).toString());
        assertFollowerConsistent(replay, f.grpId);
    }

    /**
     * The CREATE's GroupId is unstable from a splice during its tablet creation, and a checker cycle marks the group
     * stable while the CREATE journals: the follower must end with the group stable, as the leader has it.
     */
    @Test
    public void testCheckerMarkStableQueuedBehindCreateKeepsFollowerInSync() throws Exception {
        Fixture f = group("8", DB_B);
        ON_NEXT_RANGE_COLOCATE_CREATE_SHARDS.set(() -> splice200(f));
        Worker checker = new Worker("checker", () -> index().markAllGroupsWithSameColocateGroupIdStable(f.grpId, true));
        AtomicBoolean allUnstableAtCreate = new AtomicBoolean();
        BEFORE_CREATE_RANGE_RECORD.set(() -> {
            List<GroupId> groupIds = index().getAllGroupIdsWithSameColocateGroupId(f.grpId);
            allUnstableAtCreate.set(groupIds.size() == 2 && groupIds.stream().allMatch(index()::isGroupUnstable));
            checker.startAndAwaitFinishedOrBlocked();
        });

        FollowerReplay replay = replayOnFollower(() -> {
            createTable(DB_A, "c8", f.group);
            checker.join();
        }, () -> f.grpId);

        Assertions.assertNull(ON_NEXT_RANGE_COLOCATE_CREATE_SHARDS.get(), "the tablet creation hook ran");
        Assertions.assertNull(BEFORE_CREATE_RANGE_RECORD.get(), "the range record hook ran");
        Assertions.assertTrue(allUnstableAtCreate.get(), "both GroupIds were unstable when the CREATE journaled");
        Assertions.assertFalse(checker.finishedWhileCreatePaused, "the checker waited for the CREATE's records");
        GroupId groupId = index().getGroup(table(DB_A, "c8").getId());
        int add = indexOf(replay.ops, 0, OperationType.OP_COLOCATE_ADD_TABLE_V2, groupId);
        int trailingMark = indexOf(replay.ops, add + 1, OperationType.OP_COLOCATE_MARK_UNSTABLE_V2, groupId);
        int stable = indexOf(replay.ops, add + 1, OperationType.OP_COLOCATE_MARK_STABLE_V2, groupId);
        Assertions.assertTrue(add >= 0 && add < trailingMark && trailingMark < stable,
                "the CREATE's mark-unstable after its add-table record lands before the checker's mark-stable: "
                        + labels(replay.ops));
        Assertions.assertEquals(2, index().getAllGroupIdsWithSameColocateGroupId(f.grpId).size());
        Assertions.assertFalse(index().isAnyGroupWithSameColocateGroupIdUnstable(f.grpId));
        assertFollowerConsistent(replay, f.grpId);
    }

    // ---------------------------------------------------------------------------------------------
    // The add-table record and its replay
    // ---------------------------------------------------------------------------------------------

    /**
     * Records without ranges (a hash colocate member, a restore, a journal written before the add-table record carried
     * them) serialize and replay as before, and an FE that ignores the ranges in the record still gets them from the
     * range record before it.
     */
    @Test
    public void testAddTableRecordWithoutRangesReplaysAsBefore() throws Exception {
        Fixture f = group("9", DB_A);
        OlapTable m9 = table(DB_A, "m_9");
        GroupId groupId = index().getGroup(m9.getId());
        List<ColocateRange> ranges = index().getColocateRanges(f.grpId);

        // No "cr" key without ranges.
        ColocatePersistInfo withoutRanges = ColocatePersistInfo.createForAddTable(groupId, m9.getId(), new ArrayList<>());
        String json = GsonUtils.GSON.toJson(withoutRanges);
        Assertions.assertFalse(JsonParser.parseString(json).getAsJsonObject().has("cr"), json);
        Assertions.assertEquals(List.of(), GsonUtils.GSON.fromJson(json, ColocatePersistInfo.class).getColocateRanges());

        // A record with ranges round-trips them.
        ColocatePersistInfo withRanges =
                ColocatePersistInfo.createForAddTable(groupId, m9.getId(), new ArrayList<>(), ranges);
        ColocatePersistInfo copy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(withRanges), ColocatePersistInfo.class);
        Assertions.assertEquals(ranges, copy.getColocateRanges());
        Assertions.assertEquals(withRanges, copy);
        Assertions.assertNotEquals(withoutRanges, withRanges);

        // Replaying a record without ranges leaves the group's ranges as they are and adds the member.
        ColocateTableIndex fresh = new ColocateTableIndex();
        fresh.getColocateRangeMgr().setColocateRanges(f.grpId, ranges);
        fresh.replayAddTableToGroup(withoutRanges);
        Assertions.assertEquals(ranges, fresh.getColocateRanges(f.grpId));
        Assertions.assertEquals(List.of(m9.getId()), fresh.getAllTableIds(groupId));

        // A CREATE of a new group replayed by an FE that ignores the ranges in the add-table record: the follower has no
        // such group before, so only the separate range record can give it the ranges.
        long dbAId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_A).getId();
        Assertions.assertNull(index().getGroupSchema(ColocateTableIndex.getFullGroupName(dbAId, "g_9_new")));
        FollowerReplay legacy = replayOnFollower(() -> createTable(DB_A, "c9", "g_9_new"),
                () -> index().getGroup(table(DB_A, "c9").getId()).grpId,
                RangeColocateCreateJournalTest::withoutCarriedRanges);
        OlapTable c9 = table(DB_A, "c9");
        GroupId newGroupId = index().getGroup(c9.getId());
        assertOps(legacy, "RANGE_UPDATE", "ADD_TABLE");
        Assertions.assertEquals(index().getColocateRanges(newGroupId.grpId),
                ((ColocatePersistInfo) legacy.ops.get(1).data()).getColocateRanges(), "the record as journaled");
        Assertions.assertEquals(List.of(c9.getId()), legacy.follower.getAllTableIds(newGroupId),
                "the add-table record without its ranges still adds the member");
        assertFollowerConsistent(legacy, newGroupId.grpId);
    }

    /**
     * The ranges an add-table record carries are applied before the member is added: a follower that has the GroupId
     * but lost the group's ranges (the last member in another database dropped in between) would otherwise reject the
     * member as one of a range group without ranges.
     */
    @Test
    public void testReplayAddTableRestoresMissingRangeEntryOfExistingGroup() throws Exception {
        Fixture f = group("10", DB_A);
        createTable(DB_A, "c10", f.group);
        OlapTable first = table(DB_A, "m_10");
        OlapTable second = table(DB_A, "c10");
        GroupId groupId = index().getGroup(first.getId());
        Assertions.assertEquals(groupId, index().getGroup(second.getId()));
        List<ColocateRange> ranges = index().getColocateRanges(f.grpId);

        ColocateTableIndex fresh = new ColocateTableIndex();
        fresh.replayAddTableToGroup(
                ColocatePersistInfo.createForAddTable(groupId, first.getId(), new ArrayList<>(), ranges));
        Assertions.assertEquals(ranges, fresh.getColocateRanges(f.grpId));
        fresh.getColocateRangeMgr().removeColocateGroup(f.grpId);
        fresh.replayAddTableToGroup(
                ColocatePersistInfo.createForAddTable(groupId, second.getId(), new ArrayList<>(), ranges));

        Assertions.assertEquals(ranges, fresh.getColocateRanges(f.grpId));
        Assertions.assertEquals(Set.of(first.getId(), second.getId()), new HashSet<>(fresh.getAllTableIds(groupId)));
    }
}
