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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/ColocateTableIndex.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.TablePropertyInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * maintain the colocate table related indexes and meta
 */
public class ColocateTableIndex implements Writable {
    private static final Logger LOG = LogManager.getLogger(ColocateTableIndex.class);

    public static class GroupId implements Writable {
        @SerializedName("db")
        public Long dbId;
        @SerializedName("gp")
        public Long grpId;

        private GroupId() {
        }

        public GroupId(long dbId, long grpId) {
            this.dbId = dbId;
            this.grpId = grpId;
        }

        public static GroupId read(DataInput in) throws IOException {
            GroupId groupId = new GroupId();
            groupId.readFields(in);
            return groupId;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(dbId);
            out.writeLong(grpId);
        }

        public void readFields(DataInput in) throws IOException {
            dbId = in.readLong();
            grpId = in.readLong();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof GroupId)) {
                return false;
            }
            GroupId other = (GroupId) obj;
            return dbId.equals(other.dbId) && grpId.equals(other.grpId);
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + dbId.hashCode();
            result = 31 * result + grpId.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return dbId + "." + grpId;
        }
    }

    // group_name -> group_id
    @SerializedName("gn")
    private Map<String, GroupId> groupName2Id = Maps.newHashMap();
    // group_id -> table_ids
    @SerializedName("gt")
    private Multimap<GroupId, Long> group2Tables = ArrayListMultimap.create();
    // table_id -> group_id
    @SerializedName("tg")
    private Map<Long, GroupId> table2Group = Maps.newHashMap();
    // group id -> group schema
    @SerializedName("gs")
    private Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
    // group_id -> bucketSeq -> backend ids
    @SerializedName("gb")
    private Map<GroupId, List<List<Long>>> group2BackendsPerBucketSeq = Maps.newHashMap();
    // the colocate group is unstable
    @SerializedName("ug")
    private Set<GroupId> unstableGroups = Sets.newHashSet();
    // lake group, in memory
    private final Set<GroupId> lakeGroups = Sets.newHashSet();

    private final transient ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ColocateTableIndex() {

    }

    private void readLock() {
        this.lock.readLock().lock();
    }

    private void readUnlock() {
        this.lock.readLock().unlock();
    }

    private void writeLock() {
        this.lock.writeLock().lock();
    }

    private void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public boolean addTableToGroup(Database db,
                                   OlapTable olapTable, String colocateGroup, boolean expectLakeTable)
            throws DdlException {
        if (Strings.isNullOrEmpty(colocateGroup)) {
            return false;
        }

        if (olapTable.isCloudNativeTableOrMaterializedView() != expectLakeTable) {
            return false;
        }

        String fullGroupName = db.getId() + "_" + colocateGroup;
        ColocateGroupSchema groupSchema = this.getGroupSchema(fullGroupName);
        ColocateTableIndex.GroupId colocateGrpIdInOtherDb = null; /* to use GroupId.grpId */
        if (groupSchema != null) {
            // group already exist, check if this table can be added to this group
            groupSchema.checkColocateSchema(olapTable);
        } else {
            // we also need to check the schema consistency with colocate group in other database
            colocateGrpIdInOtherDb = this.checkColocateSchemaWithGroupInOtherDb(
                    colocateGroup, db.getId(), olapTable);
        }
        // Add table to this group, if group does not exist, create a new one.
        // If the to create colocate group should colocate with groups in other databases,
        // i.e. `colocateGrpIdInOtherDb` is not null, we reuse `GroupId.grpId` from those
        // groups, so that we can have a mechanism to precisely find all the groups that colocate with
        // each other in different databases.
        this.addTableToGroup(db.getId(), olapTable, colocateGroup,
                colocateGrpIdInOtherDb == null ? null :
                        new ColocateTableIndex.GroupId(db.getId(), colocateGrpIdInOtherDb.grpId),
                false /* isReplay */);
        olapTable.setColocateGroup(colocateGroup);
        return true;
    }

    // NOTICE: call 'addTableToGroup()' will not modify 'group2BackendsPerBucketSeq'
    // 'group2BackendsPerBucketSeq' need to be set manually before or after, if necessary.
    public GroupId addTableToGroup(long dbId, OlapTable tbl, String groupName, GroupId assignedGroupId,
                                   boolean isReplay)
            throws DdlException {
        Preconditions.checkArgument(tbl.getDefaultDistributionInfo().supportColocate(),
                "colocate not supported");
        writeLock();
        try {
            boolean groupAlreadyExist = true;
            GroupId groupId;
            String fullGroupName = dbId + "_" + groupName;

            if (groupName2Id.containsKey(fullGroupName)) {
                groupId = groupName2Id.get(fullGroupName);
            } else {
                if (assignedGroupId != null) {
                    // use the given group id, eg, in replay process or cross db colocation
                    groupId = assignedGroupId;
                } else {
                    // generate a new one
                    groupId = new GroupId(dbId, GlobalStateMgr.getCurrentState().getNextId());
                    groupAlreadyExist = false;
                }
                HashDistributionInfo distributionInfo = (HashDistributionInfo) tbl.getDefaultDistributionInfo();
                if (!(tbl instanceof ExternalOlapTable)) {
                    // Colocate table should keep the same bucket number across the partitions
                    if (distributionInfo.getBucketNum() == 0) {
                        int bucketNum = CatalogUtils.calBucketNumAccordingToBackends();
                        distributionInfo.setBucketNum(bucketNum);
                    }
                }
                ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId,
                        distributionInfo.getDistributionColumns(), distributionInfo.getBucketNum(),
                        tbl.getDefaultReplicationNum());
                groupName2Id.put(fullGroupName, groupId);
                group2Schema.put(groupId, groupSchema);
            }

            if (tbl.isCloudNativeTable()) {
                if (!isReplay) { // leader create or update meta group
                    LakeTable ltbl = (LakeTable) tbl;
                    List<Long> shardGroupIds = ltbl.getShardGroupIds();
                    if (!groupAlreadyExist) {
                        GlobalStateMgr.getCurrentStarOSAgent().createMetaGroup(groupId.grpId, shardGroupIds);
                    } else {
                        GlobalStateMgr.getCurrentStarOSAgent().updateMetaGroup(groupId.grpId, shardGroupIds, true /* isJoin */);
                    }
                }
                lakeGroups.add(groupId);
            }

            group2Tables.put(groupId, tbl.getId());
            table2Group.put(tbl.getId(), groupId);
            return groupId;
        } finally {
            writeUnlock();
        }
    }

    public void addBackendsPerBucketSeq(GroupId groupId, List<List<Long>> backendsPerBucketSeq) {
        writeLock();
        try {
            List<List<Long>> previous = group2BackendsPerBucketSeq.put(groupId, backendsPerBucketSeq);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Colocate group {} changed bucket seq from {} to {}, caller stack: {}",
                        groupId, previous, backendsPerBucketSeq, LogUtil.getCurrentStackTraceToList());
            }
        } finally {
            writeUnlock();
        }
    }

    public void markGroupUnstable(GroupId groupId, boolean needEditLog) {
        writeLock();
        try {
            if (!group2Tables.containsKey(groupId)) {
                return;
            }
            if (unstableGroups.add(groupId)) {
                if (needEditLog) {
                    ColocatePersistInfo info = ColocatePersistInfo.createForMarkUnstable(groupId);
                    GlobalStateMgr.getCurrentState().getEditLog().logColocateMarkUnstable(info);
                }
                LOG.info("mark group {} as unstable", groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public void markGroupStable(GroupId groupId, boolean needEditLog) {
        writeLock();
        try {
            if (!group2Tables.containsKey(groupId)) {
                return;
            }
            if (unstableGroups.remove(groupId)) {
                if (needEditLog) {
                    ColocatePersistInfo info = ColocatePersistInfo.createForMarkStable(groupId);
                    GlobalStateMgr.getCurrentState().getEditLog().logColocateMarkStable(info);
                }
                LOG.info("mark group {} as stable", groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public boolean removeTable(long tableId, OlapTable tbl, boolean isReplay) {
        writeLock();
        try {
            if (!table2Group.containsKey(tableId)) {
                return false;
            }

            GroupId groupId = table2Group.remove(tableId);

            if (tbl != null && tbl.isCloudNativeTable() && !isReplay) {
                LakeTable ltbl = (LakeTable) tbl;
                List<Long> shardGroupIds = ltbl.getShardGroupIds();
                try {
                    GlobalStateMgr.getCurrentStarOSAgent().updateMetaGroup(groupId.grpId, shardGroupIds,
                            false /* isJoin */);
                } catch (DdlException e) {
                    LOG.error(e.getMessage());
                }
            }

            group2Tables.remove(groupId, tableId);
            if (!group2Tables.containsKey(groupId)) {
                // all tables of this group are removed, remove the group
                group2BackendsPerBucketSeq.remove(groupId);
                group2Schema.remove(groupId);
                unstableGroups.remove(groupId);
                lakeGroups.remove(groupId);
                String fullGroupName = null;
                for (Map.Entry<String, GroupId> entry : groupName2Id.entrySet()) {
                    if (entry.getValue().equals(groupId)) {
                        fullGroupName = entry.getKey();
                        break;
                    }
                }
                if (fullGroupName != null) {
                    groupName2Id.remove(fullGroupName);
                }
            }
        } finally {
            writeUnlock();
        }

        return true;
    }

    public boolean isGroupUnstable(GroupId groupId) {
        readLock();
        try {
            if (lakeGroups.contains(groupId)) {
                return !GlobalStateMgr.getCurrentStarOSAgent().queryMetaGroupStable(groupId.grpId);
            } else {
                return unstableGroups.contains(groupId);
            }
        } finally {
            readUnlock();
        }
    }

    public boolean isColocateTable(long tableId) {
        readLock();
        try {
            return table2Group.containsKey(tableId);
        } finally {
            readUnlock();
        }
    }

    public boolean isLakeColocateTable(long tableId) {
        readLock();
        try {
            GroupId groupId = table2Group.get(tableId);
            if (groupId == null) {
                return false;
            }
            return lakeGroups.contains(groupId);
        } finally {
            readUnlock();
        }
    }

    public boolean isGroupExist(GroupId groupId) {
        readLock();
        try {
            return group2Schema.containsKey(groupId);
        } finally {
            readUnlock();
        }
    }

    public boolean isSameGroup(long table1, long table2) {
        readLock();
        try {
            if (table2Group.containsKey(table1) && table2Group.containsKey(table2)) {
                return Objects.equals(table2Group.get(table1).grpId, table2Group.get(table2).grpId);
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    public Set<GroupId> getUnstableGroupIds() {
        readLock();
        try {
            return Sets.newHashSet(unstableGroups);
        } finally {
            readUnlock();
        }
    }

    public GroupId getGroup(long tableId) {
        readLock();
        try {
            Preconditions.checkState(table2Group.containsKey(tableId));
            return table2Group.get(tableId);
        } finally {
            readUnlock();
        }
    }

    public Set<GroupId> getAllGroupIds() {
        readLock();
        try {
            // make a copy set to avoid ConcurrentModificationException
            return new HashSet<>(group2Tables.keySet());
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getBackendsByGroup(GroupId groupId) {
        readLock();
        try {
            Set<Long> allBackends = new HashSet<>();
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            // if create colocate table with empty partition or create colocate table
            // with dynamic_partition will cause backendsPerBucketSeq == null
            if (backendsPerBucketSeq != null) {
                for (List<Long> bes : backendsPerBucketSeq) {
                    allBackends.addAll(bes);
                }
            }
            return allBackends;
        } finally {
            readUnlock();
        }
    }

    public List<Long> getAllTableIds(GroupId groupId) {
        readLock();
        try {
            if (!group2Tables.containsKey(groupId)) {
                return Lists.newArrayList();
            }
            return Lists.newArrayList(group2Tables.get(groupId));
        } finally {
            readUnlock();
        }
    }

    public int getNumOfTabletsPerBucket(GroupId groupId) {
        List<Long> allTableIds = getAllTableIds(groupId);
        Database db = GlobalStateMgr.getCurrentState().getDb(groupId.dbId);
        int numOfTablets = 0;
        if (db != null && !allTableIds.isEmpty()) {
            try {
                db.readLock();
                for (long tableId : allTableIds) {
                    OlapTable tbl = (OlapTable) db.getTable(tableId);
                    if (tbl != null) {
                        numOfTablets += tbl.getNumberOfPartitions();
                    }
                }
            } finally {
                db.readUnlock();
            }
        }
        return numOfTablets;
    }

    public List<List<Long>> getBackendsPerBucketSeq(GroupId groupId) {
        readLock();
        try {
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            if (backendsPerBucketSeq == null) {
                return Lists.newArrayList();
            }
            return backendsPerBucketSeq;
        } finally {
            readUnlock();
        }
    }

    public List<Set<Long>> getBackendsPerBucketSeqSet(GroupId groupId) {
        readLock();
        try {
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            if (backendsPerBucketSeq == null) {
                return Lists.newArrayList();
            }
            List<Set<Long>> sets = Lists.newArrayList();
            for (List<Long> backends : backendsPerBucketSeq) {
                sets.add(Sets.newHashSet(backends));
            }
            return sets;
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getTabletBackendsByGroup(GroupId groupId, int tabletOrderIdx) {
        readLock();
        try {
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            if (backendsPerBucketSeq == null) {
                return Sets.newHashSet();
            }
            if (tabletOrderIdx >= backendsPerBucketSeq.size()) {
                return Sets.newHashSet();
            }

            return Sets.newHashSet(backendsPerBucketSeq.get(tabletOrderIdx));
        } finally {
            readUnlock();
        }
    }

    public ColocateGroupSchema getGroupSchema(String fullGroupName) {
        readLock();
        try {
            if (!groupName2Id.containsKey(fullGroupName)) {
                return null;
            }
            return group2Schema.get(groupName2Id.get(fullGroupName));
        } finally {
            readUnlock();
        }
    }

    public ColocateGroupSchema getGroupSchema(GroupId groupId) {
        readLock();
        try {
            return group2Schema.get(groupId);
        } finally {
            readUnlock();
        }
    }

    public long getTableIdByGroup(String fullGroupName) {
        readLock();
        try {
            if (groupName2Id.containsKey(fullGroupName)) {
                GroupId groupId = groupName2Id.get(fullGroupName);
                Optional<Long> tblId = group2Tables.get(groupId).stream().findFirst();
                return tblId.isPresent() ? tblId.get() : -1;
            }
        } finally {
            readUnlock();
        }
        return -1;
    }

    public GroupId changeGroup(long dbId, OlapTable tbl, String oldGroup, String newGroup, GroupId assignedGroupId,
                               boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (!Strings.isNullOrEmpty(oldGroup)) {
                // remove from old group
                removeTable(tbl.getId(), tbl, isReplay);
            }
            return addTableToGroup(dbId, tbl, newGroup, assignedGroupId, isReplay);
        } finally {
            writeUnlock();
        }
    }

    public void replayAddTableToGroup(ColocatePersistInfo info) {
        Database db = GlobalStateMgr.getCurrentState().getDb(info.getGroupId().dbId);
        Preconditions.checkNotNull(db);
        OlapTable tbl = (OlapTable) db.getTable(info.getTableId());
        Preconditions.checkNotNull(tbl);

        writeLock();
        try {
            if (!group2BackendsPerBucketSeq.containsKey(info.getGroupId())) {
                group2BackendsPerBucketSeq.put(info.getGroupId(), info.getBackendsPerBucketSeq());
            }

            addTableToGroup(info.getGroupId().dbId, tbl, tbl.getColocateGroup(), info.getGroupId(), true /* isReplay */);
        } catch (DdlException e) {
            // should not happen, just log an error here
            LOG.error(e.getMessage());
        } finally {
            writeUnlock();
        }
    }

    public void replayAddBackendsPerBucketSeq(ColocatePersistInfo info) {
        addBackendsPerBucketSeq(info.getGroupId(), info.getBackendsPerBucketSeq());
    }

    public void replayMarkGroupUnstable(ColocatePersistInfo info) {
        markGroupUnstable(info.getGroupId(), false);
    }

    public void replayMarkGroupStable(ColocatePersistInfo info) {
        markGroupStable(info.getGroupId(), false);
    }

    public void replayRemoveTable(ColocatePersistInfo info) {
        removeTable(info.getTableId(), null, true /* isReplay */);
    }

    // only for test
    public void clear() {
        writeLock();
        try {
            groupName2Id.clear();
            group2Tables.clear();
            table2Group.clear();
            group2BackendsPerBucketSeq.clear();
            group2Schema.clear();
            unstableGroups.clear();
        } finally {
            writeUnlock();
        }
    }

    protected Optional<String> getTableName(long dbId, long tableId) {

        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            return Optional.empty();
        }
        Table table = database.getTable(tableId);

        if (table == null) {
            return Optional.empty();
        }

        return Optional.of(table.getName());
    }



    /**
     * After the user executes `DROP TABLE`, we only throw tables into the recycle bin instead of deleting them
     * immediately. As a side effect, the table that stores in ColocateTableIndex may not be visible to users. We need
     * to add a marker to indicate to our user that the corresponding table is marked deleted.
     */
    public List<List<String>> getInfos() {
        List<List<String>> infos = Lists.newArrayList();

        readLock();
        try {
            for (Map.Entry<String, GroupId> entry : groupName2Id.entrySet()) {
                List<String> info = Lists.newArrayList();
                GroupId groupId = entry.getValue();
                info.add(groupId.toString());
                info.add(entry.getKey());
                StringJoiner tblIdJoiner = new StringJoiner(", ");
                StringJoiner tblNameJoiner = new StringJoiner(", ");
                for (Long tableId : group2Tables.get(groupId)) {
                    Optional<String> tblName = getTableName(groupId.dbId, tableId);
                    if (!tblName.isPresent()) {
                        tblIdJoiner.add(tableId + "*");
                        tblNameJoiner.add("[deleted]");
                    } else {
                        tblIdJoiner.add(tableId.toString());
                        tblNameJoiner.add(tblName.get());
                    }
                }
                info.add(tblIdJoiner.toString());
                info.add(tblNameJoiner.toString());
                ColocateGroupSchema groupSchema = group2Schema.get(groupId);
                info.add(String.valueOf(groupSchema.getBucketsNum()));
                info.add(String.valueOf(groupSchema.getReplicationNum()));
                List<String> cols = groupSchema.getDistributionColTypes().stream().map(
                        Type::toSql).collect(Collectors.toList());
                info.add(Joiner.on(", ").join(cols));
                info.add(String.valueOf(!isGroupUnstable(groupId)));
                infos.add(info);
            }
        } finally {
            readUnlock();
        }
        return infos;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int size = groupName2Id.size();
        out.writeInt(size);
        for (Map.Entry<String, GroupId> entry : groupName2Id.entrySet()) {
            Text.writeString(out, entry.getKey()); // group name
            entry.getValue().write(out); // group id
            Collection<Long> tableIds = group2Tables.get(entry.getValue());
            out.writeInt(tableIds.size());
            for (Long tblId : tableIds) {
                out.writeLong(tblId); // table ids
            }
            ColocateGroupSchema groupSchema = group2Schema.get(entry.getValue());
            groupSchema.write(out); // group schema

            // backend seq
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(entry.getValue());
            out.writeInt(backendsPerBucketSeq.size());
            for (List<Long> bucket2BEs : backendsPerBucketSeq) {
                out.writeInt(bucket2BEs.size());
                for (Long be : bucket2BEs) {
                    out.writeLong(be);
                }
            }
        }

        size = unstableGroups.size();
        out.writeInt(size);
        for (GroupId groupId : unstableGroups) {
            groupId.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String fullGrpName = Text.readString(in);
            GroupId grpId = GroupId.read(in);
            groupName2Id.put(fullGrpName, grpId);
            int tableSize = in.readInt();
            for (int j = 0; j < tableSize; j++) {
                long tblId = in.readLong();
                group2Tables.put(grpId, tblId);
                table2Group.put(tblId, grpId);
            }
            ColocateGroupSchema groupSchema = ColocateGroupSchema.read(in);
            group2Schema.put(grpId, groupSchema);

            List<List<Long>> backendsPerBucketSeq = Lists.newArrayList();
            int beSize = in.readInt();
            for (int j = 0; j < beSize; j++) {
                int seqSize = in.readInt();
                List<Long> seq = Lists.newArrayList();
                for (int k = 0; k < seqSize; k++) {
                    long beId = in.readLong();
                    seq.add(beId);
                }
                backendsPerBucketSeq.add(seq);
            }
            group2BackendsPerBucketSeq.put(grpId, backendsPerBucketSeq);
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            unstableGroups.add(GroupId.read(in));
        }
    }

    public void setBackendsSetByIdxForGroup(GroupId groupId, int tabletOrderIdx, Set<Long> newBackends) {
        writeLock();
        try {
            List<List<Long>> backends = group2BackendsPerBucketSeq.get(groupId);
            if (backends == null) {
                return;
            }
            Preconditions.checkState(tabletOrderIdx < backends.size(), tabletOrderIdx + " vs. " + backends.size());
            backends.set(tabletOrderIdx, Lists.newArrayList(newBackends));
            ColocatePersistInfo info = ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backends);
            GlobalStateMgr.getCurrentState().getEditLog().logColocateBackendsPerBucketSeq(info);
        } finally {
            writeUnlock();
        }
    }

    public long loadColocateTableIndex(DataInputStream dis, long checksum) throws IOException {
        GlobalStateMgr.getCurrentColocateIndex().readFields(dis);
        // clean up if dbId or tableId not found, this is actually a bug
        cleanupInvalidDbOrTable(GlobalStateMgr.getCurrentState());
        constructLakeGroups(GlobalStateMgr.getCurrentState());
        LOG.info("finished replay colocateTableIndex from image");
        return checksum;
    }

    public long saveColocateTableIndex(DataOutputStream dos, long checksum) throws IOException {
        write(dos);
        return checksum;
    }

    public void saveColocateTableIndexV2(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.COLOCATE_TABLE_INDEX, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void loadColocateTableIndexV2(SRMetaBlockReader reader)
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        ColocateTableIndex data = reader.readJson(ColocateTableIndex.class);
        this.groupName2Id = data.groupName2Id;
        this.group2Tables = data.group2Tables;
        this.table2Group = data.table2Group;
        this.group2Schema = data.group2Schema;
        this.group2BackendsPerBucketSeq = data.group2BackendsPerBucketSeq;
        this.unstableGroups = data.unstableGroups;

        cleanupInvalidDbOrTable(GlobalStateMgr.getCurrentState());
        constructLakeGroups(GlobalStateMgr.getCurrentState());
        LOG.info("finished replay colocateTableIndex from image");
    }

    private List<GroupId> getOtherGroupsWithSameOrigNameUnlocked(String origName, long dbId) {
        List<GroupId> groupIds = new ArrayList<>();
        for (Map.Entry<String, GroupId> entry : groupName2Id.entrySet()) {
            // Get existed group original name
            String existedGroupOrigName = entry.getKey().split("_", 2)[1];
            if (existedGroupOrigName.equals(origName) &&
                    entry.getValue().dbId != dbId) {
                groupIds.add(entry.getValue());
            }
        }

        return groupIds;
    }

    public GroupId checkColocateSchemaWithGroupInOtherDb(String toCreateGroupName, long dbId,
                                                         OlapTable toCreateTable) throws DdlException {
        try {
            readLock();
            List<GroupId> sameOrigNameGroups = getOtherGroupsWithSameOrigNameUnlocked(toCreateGroupName, dbId);
            if (sameOrigNameGroups.isEmpty()) {
                return null;
            }
            // Here we need to check schema consistency with all the groups in other databases, because
            // if this is the situation where user upgrades from older version, there may exist two colocate
            // groups with the same original name, but have different schema, in this case, we cannot decide
            // which group the new colocate group should colocate with, thus we should throw an error explicitly
            // and let the user handle it.
            for (GroupId gid : sameOrigNameGroups) {
                getGroupSchema(gid).checkColocateSchema(toCreateTable);
            }
            // For tables that reside in different databases and colocate with each other, there still exists
            // multi colocate groups, but these colocate groups will have the same original name and their
            // `GroupId.grpId` parts are limited to be the same, and most importantly, their bucket
            // sequences will keep in consistent when balancing or doing some kind of modification. For this limitation
            // reason, here we also need to check the `GroupId.grpId` part is the same in case that this is an upgrade.
            // If not the same, throw an error.
            for (int i = 1; i < sameOrigNameGroups.size(); i++) {
                if (!Objects.equals(sameOrigNameGroups.get(0).grpId, sameOrigNameGroups.get(i).grpId)) {
                    throw new DdlException("going to create a new group that has the same name '" +
                            toCreateGroupName + "' with " + sameOrigNameGroups +
                            ", but these existed groups don't have the same `GroupId.grpId`");
                }
            }
            return sameOrigNameGroups.get(0);
        } finally {
            readUnlock();
        }
    }

    public List<GroupId> getColocateWithGroupsInOtherDb(GroupId groupId) {
        List<GroupId> results = new ArrayList<>();
        Long grpId = groupId.grpId;
        try {
            readLock();
            for (GroupId otherGroupId : group2Schema.keySet()) {
                if (!Objects.equals(otherGroupId.dbId, groupId.dbId) &&
                        Objects.equals(otherGroupId.grpId, grpId)) {
                    results.add(otherGroupId);
                }
            }
        } finally {
            readUnlock();
        }

        return results;
    }

    // the invoker should keep db write lock
    public void modifyTableColocate(Database db, OlapTable table, String colocateGroup, boolean isReplay,
                                    GroupId assignedGroupId)
            throws DdlException {

        String oldGroup = table.getColocateGroup();
        GroupId groupId;
        if (!Strings.isNullOrEmpty(colocateGroup)) {
            GroupId colocateGrpIdInOtherDb = null;
            String fullGroupName = db.getId() + "_" + colocateGroup;
            ColocateGroupSchema groupSchema = getGroupSchema(fullGroupName);
            if (groupSchema == null) {
                // user set a new colocate group,
                // check if all partitions all this table has same buckets num and same replication number
                PartitionInfo partitionInfo = table.getPartitionInfo();
                if (partitionInfo.isRangePartition()) {
                    int bucketsNum = -1;
                    short replicationNum = -1;
                    for (Partition partition : table.getPartitions()) {
                        if (bucketsNum == -1) {
                            bucketsNum = partition.getDistributionInfo().getBucketNum();
                        } else if (bucketsNum != partition.getDistributionInfo().getBucketNum()) {
                            throw new DdlException(
                                    "Partitions in table " + table.getName() + " have different buckets number");
                        }

                        if (replicationNum == -1) {
                            replicationNum = partitionInfo.getReplicationNum(partition.getId());
                        } else if (replicationNum != partitionInfo.getReplicationNum(partition.getId())) {
                            throw new DdlException(
                                    "Partitions in table " + table.getName() + " have different replication number");
                        }
                    }
                }
                // we also need to check the schema consistency with colocate group in other database
                colocateGrpIdInOtherDb = checkColocateSchemaWithGroupInOtherDb(
                        colocateGroup, db.getId(), table);
                // `assignedGroupId == null` means that this is not a replay, but a user issued group creation
                if (assignedGroupId == null && colocateGrpIdInOtherDb != null) {
                    assignedGroupId = new GroupId(db.getId(), colocateGrpIdInOtherDb.grpId);
                }
            } else {
                // set to an already exist colocate group, check if this table can be added to this group.
                groupSchema.checkColocateSchema(table);
            }

            List<List<Long>> backendsPerBucketSeq = null;
            if (groupSchema == null) {
                if (colocateGrpIdInOtherDb == null) {
                    // assign to a newly created group, set backends sequence.
                    // we arbitrarily choose a tablet backends sequence from this table,
                    // let the colocation balancer do the work.
                    backendsPerBucketSeq = table.getArbitraryTabletBucketsSeq();
                } else {
                    // colocate with group in other database, reuse the bucket seq.
                    backendsPerBucketSeq = getBackendsPerBucketSeq(colocateGrpIdInOtherDb);
                }
            }
            // change group after getting backends sequence(if it has), in case 'getArbitraryTabletBucketsSeq' failed
            groupId = changeGroup(db.getId(), table, oldGroup, colocateGroup, assignedGroupId, isReplay);
            if (groupSchema == null) {
                Preconditions.checkNotNull(backendsPerBucketSeq);
                addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            }

            // set this group as unstable
            markGroupUnstable(groupId, false /* edit log is along with modify table log */);
            table.setColocateGroup(colocateGroup);
        } else {
            // unset colocation group
            if (Strings.isNullOrEmpty(oldGroup)) {
                // this table is not a colocate table, do nothing
                return;
            }
            // when replayModifyTableColocate, we need the groupId info
            String fullGroupName = db.getId() + "_" + oldGroup;
            groupId = getGroupSchema(fullGroupName).getGroupId();
            removeTable(table.getId(), table, isReplay);
            table.setColocateGroup(null);
        }

        if (!isReplay) {
            Map<String, String> properties = Maps.newHashMapWithExpectedSize(1);
            properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, colocateGroup);
            TablePropertyInfo info = new TablePropertyInfo(table.getId(), groupId, properties);
            GlobalStateMgr.getCurrentState().getEditLog().logModifyTableColocate(info);
        }
        table.lastSchemaUpdateTime.set(System.nanoTime());
        LOG.info("finished modify table's colocation property. table: {}, is replay: {}",
                table.getName(), isReplay);
    }

    public void replayModifyTableColocate(TablePropertyInfo info) {
        long tableId = info.getTableId();
        Map<String, String> properties = info.getPropertyMap();

        Database db = GlobalStateMgr.getCurrentState().getDb(info.getGroupId().dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            modifyTableColocate(db, table, properties.get(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH), true,
                    info.getGroupId());
        } catch (DdlException e) {
            // should not happen
            LOG.warn("failed to replay modify table colocate", e);
        } finally {
            db.writeUnlock();
        }
    }

    /**
     * for legacy reason, all the colocate group index cannot be properly removed
     * we have to add a cleanup function on start when loading image
     */
    protected void cleanupInvalidDbOrTable(GlobalStateMgr globalStateMgr) {
        List<Long> badTableIds = new ArrayList<>();
        for (Map.Entry<Long, GroupId> entry : table2Group.entrySet()) {
            long dbId = entry.getValue().dbId;
            long tableId = entry.getKey();
            Database database = globalStateMgr.getDbIncludeRecycleBin(dbId);
            if (database == null) {
                LOG.warn("cannot find db {}, will remove invalid table {} from group {}",
                        dbId, tableId, entry.getValue());
            } else {
                Table table = globalStateMgr.getTableIncludeRecycleBin(database, tableId);
                if (table != null) {
                    // this is a valid table/database, do nothing
                    continue;
                }
                LOG.warn("cannot find table {} in db {}, will remove invalid table {} from group {}",
                        tableId, dbId, tableId, entry.getValue());
            }
            badTableIds.add(tableId);
        }
        LOG.warn("remove {} invalid tableid: {}", badTableIds.size(), badTableIds);
        for (Long tableId : badTableIds) {
            removeTable(tableId, null, false /* isReplay */);
        }
    }

    public void updateLakeTableColocationInfo(OlapTable olapTable, boolean isJoin,
                                              GroupId expectGroupId) throws DdlException {
        if (olapTable == null || !olapTable.isCloudNativeTable()) { // skip non-lake table
            return;
        }

        writeLock();
        try {
            GroupId groupId = expectGroupId;
            if (expectGroupId == null) {
                if (!table2Group.containsKey(olapTable.getId())) { // skip non-colocate table
                    return;
                }
                groupId = table2Group.get(olapTable.getId());
            }

            LakeTable ltbl = (LakeTable) olapTable;
            List<Long> shardGroupIds = ltbl.getShardGroupIds();
            LOG.info("update meta group id {}, table {}, shard groups: {}, join: {}",
                    groupId.grpId, olapTable.getId(), shardGroupIds, isJoin);
            GlobalStateMgr.getCurrentStarOSAgent().updateMetaGroup(groupId.grpId, shardGroupIds, isJoin);
        } finally {
            writeUnlock();
        }
    }

    private void constructLakeGroups(GlobalStateMgr globalStateMgr) {
        if (RunMode.isSharedNothingMode()) {
            return;
        }

        for (Map.Entry<Long, GroupId> entry : table2Group.entrySet()) {
            long dbId = entry.getValue().dbId;
            long tableId = entry.getKey();
            // database and table should be valid if reach here
            Database database = globalStateMgr.getDbIncludeRecycleBin(dbId);
            Table table = globalStateMgr.getTableIncludeRecycleBin(database, tableId);
            if (table.isCloudNativeTable()) {
                lakeGroups.add(entry.getValue());
            }
        }
    }
}
