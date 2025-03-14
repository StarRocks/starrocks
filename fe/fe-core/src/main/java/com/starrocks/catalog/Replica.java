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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Replica.java

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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * This class represents the olap replica related metadata.
 */
public class Replica implements Writable {
    private static final Logger LOG = LogManager.getLogger(Replica.class);
    public static final VersionComparator<Replica> VERSION_DESC_COMPARATOR = new VersionComparator<Replica>();

    public static final int DEPRECATED_PROP_SCHEMA_HASH = 0;

    public enum ReplicaState {
        NORMAL,
        @Deprecated
        ROLLUP,
        @Deprecated
        SCHEMA_CHANGE,
        CLONE,
        ALTER, // replica is under rollup or schema change
        DECOMMISSION, // replica is ready to be deleted
        RECOVER;  // replica is recovering

        public boolean canLoad() {
            return this == NORMAL || this == SCHEMA_CHANGE || this == ALTER;
        }

        public boolean canQuery() {
            return this == NORMAL || this == SCHEMA_CHANGE;
        }
    }

    public enum ReplicaStatus {
        OK, // health
        DEAD, // backend is not available
        VERSION_ERROR, // missing version
        MISSING, // replica does not exist
        SCHEMA_ERROR, // replica's schema hash does not equal to index's schema hash
        BAD // replica is broken.
    }

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "backendId")
    private long backendId;
    // the version could be queried
    @SerializedName(value = "version")
    private volatile long version;

    // minimal readable version for this replica, the value is 0 at beginning and will change to larger values when:
    //   1. replica perform some version GC, and then tablet report from BE
    //   2. a replica clone task finishes, and report TTabletInfo to FE with the new minReadableVersion
    // the main purpose of adding this field is to avoid the situation that:
    //   1. tablet has 2 replica A has version[7,8,9,10], B: [7,8,9,10]
    //   2. a newly cloned replica C full clone from replica A, then has version [10]
    //   3. current partition visible version is still 9
    //   4. A query read this tablet at version 9, and picks replica C, but replica C only have version 10,
    //      causing `version not found` error
    @SerializedName(value = "minReadableVersion")
    private volatile long minReadableVersion = 0;

    // The last version reported from BE, this version should be increased monotonically.
    // Use this version to detect data lose on BE.
    // This version is only accessed by ReportHandler, so lock is unnecessary when updating.
    private volatile long lastReportVersion = 0;

    private int schemaHash = -1;
    @SerializedName(value = "dataSize")
    private volatile long dataSize = 0;
    @SerializedName(value = "rowCount")
    private volatile long rowCount = 0;
    @SerializedName(value = "state")
    private volatile ReplicaState state;

    // the last load failed version
    @SerializedName(value = "lastFailedVersion")
    private long lastFailedVersion = -1L;
    // not serialized, not very important
    private long lastFailedTimestamp = 0;
    // the last load successful version
    @SerializedName(value = "lastSuccessVersion")
    private long lastSuccessVersion = -1L;

    private volatile long versionCount = -1;

    private long pathHash = -1;

    // If bad and setBadForce are both true, it means this Replica is unrecoverable and we will delete it
    // if bad is true and isForceSetBad is false, it means this replica can be recover by be.
    private boolean bad = false;
    private boolean setBadForce = false;

    /**
     * If set to true, which means this replica need to be repaired explicitly.
     * This can happen when this replica is created by a balance clone task, and
     * when task finished, the version of this replica is behind the partition's visible version.
     * So this replica need a further repair.
     * If we do not do this, this replica will be treated as version stale, and will be removed,
     * so that the balance task is failed, which is unexpected.
     * <p>
     * furtherRepairSetTime set alone with needFurtherRepair.
     * This is an insurance, in case that further repair task always fail. If 20 min passed
     * since we set needFurtherRepair to true, the 'needFurtherRepair' will be set to false.
     */
    private boolean needFurtherRepair = false;
    private long furtherRepairSetTime = -1;
    private static final long FURTHER_REPAIR_TIMEOUT_MS = 20 * 60 * 1000L; // 20min

    // if this watermarkTxnId is set, which means before deleting a replica,
    // we should ensure that all txns on this replicas are finished.
    private long watermarkTxnId = -1;

    /**
     * In the following situation, a normally cloned replica could be falsely deleted:
     * <p>
     * 1. BE X generates tablet report and sends it to FE<p>
     * 2. FE creates clone task(for balance or repair) and a new replica on BE X,
     * so the corresponding tablet is not included in the report sent above.<p>
     * 3. BE X finishes clone and then FE will receive the message and set the state
     * of new replica to NORMAL<p>
     * 4. FE processes the tablet report from step 1 and finds that BE X doesn't report
     * the tablet info corresponding to the newly created replica, so it will delete
     * the replica from its meta<p>
     * 5. On the next tablet report of BE X which will include the tablet info, FE finds
     * that the tablet reported by BE X doesn't exist in its meta, so it will send a
     * request asking BE X to delete the newly cloned replica physically.
     * <p>
     * The main reason causes this problem is that FE handles tablet report task and clone
     * task concurrently, with specific timing, this will happen. So we add a new state
     * `deferReplicaDeleteToNextReport` for `Replica`, default to true. When FE meets a replica
     * only existed in FE's meta, not in the tablet report, it will check
     * `deferReplicaDeleteToNextReport` and defer the meta delete till next report of the BE.
     */
    private boolean deferReplicaDeleteToNextReport = true;

    // if lastWriteFail is true, we can not use it as replicated storage primary replica
    private volatile boolean lastWriteFail = false;

    private boolean isErrorState = false;

    // This variable will be used in Primary Key table only. It is the max rowset creation time for
    // the corresponding replica. This variable is in-memory only.
    private long maxRowsetCreationTime = -1L;

    // The data checksum of this replica
    private long checksum = -1L;

    public Replica() {
    }

    // for rollup
    // the new replica's version is -1 and last failed version is -1
    public Replica(long replicaId, long backendId, int schemaHash, ReplicaState state) {
        this(replicaId, backendId, -1, schemaHash, 0L, 0L, state, -1, -1);
    }

    // for create tablet and restore
    public Replica(long replicaId, long backendId, ReplicaState state, long version, int schemaHash) {
        this(replicaId, backendId, version, schemaHash, 0L, 0L, state, -1L, version);
    }

    public Replica(long replicaId, long backendId, long version, int schemaHash,
                   long dataSize, long rowCount, ReplicaState state,
                   long lastFailedVersion, long lastSuccessVersion) {
        this(replicaId, backendId, version, schemaHash, dataSize, rowCount, state,
                lastFailedVersion, lastSuccessVersion, 0);
    }

    public Replica(long replicaId, long backendId, long version, int schemaHash,
                   long dataSize, long rowCount, ReplicaState state,
                   long lastFailedVersion, long lastSuccessVersion, long minReadableVersion) {
        this.id = replicaId;
        this.backendId = backendId;
        this.version = version;
        this.schemaHash = schemaHash;

        this.dataSize = dataSize;
        this.rowCount = rowCount;
        this.state = state;
        if (this.state == null) {
            this.state = ReplicaState.NORMAL;
        }
        this.lastFailedVersion = lastFailedVersion;
        if (this.lastFailedVersion > 0) {
            this.lastFailedTimestamp = System.currentTimeMillis();
        }
        if (lastSuccessVersion < this.version) {
            this.lastSuccessVersion = this.version;
        } else {
            this.lastSuccessVersion = lastSuccessVersion;
        }
        this.minReadableVersion = minReadableVersion;
    }

    public void setLastWriteFail(boolean lastWriteFail) {
        this.lastWriteFail = lastWriteFail;
    }

    public boolean getLastWriteFail() {
        return this.lastWriteFail;
    }

    public void setLastFailedTime(long lastFailedTime) {
        this.lastFailedTimestamp = lastFailedTime;
    }

    public long getVersion() {
        return this.version;
    }

    public long getMinReadableVersion() {
        return this.minReadableVersion;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    // for compatibility
    public void setSchemaHash(int schemaHash) {
        this.schemaHash = schemaHash;
    }

    public long getId() {
        return this.id;
    }

    public long getBackendId() {
        return this.backendId;
    }

    public long getDataSize() {
        return dataSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getLastFailedVersion() {
        return lastFailedVersion;
    }

    public long getLastFailedTimestamp() {
        return lastFailedTimestamp;
    }

    public long getLastSuccessVersion() {
        return lastSuccessVersion;
    }

    public long getMaxRowsetCreationTime() {
        return maxRowsetCreationTime;
    }

    public long getChecksum() {
        return checksum;
    }

    public long getPathHash() {
        return pathHash;
    }

    public void setPathHash(long pathHash) {
        this.pathHash = pathHash;
    }

    public boolean isBad() {
        return bad;
    }

    public boolean setBad(boolean bad) {
        if (this.bad == bad) {
            return false;
        }
        this.bad = bad;
        return true;
    }

    public boolean setBadForce(boolean bad) {
        if (this.bad == bad) {
            return false;
        }
        this.bad = bad;
        this.setBadForce = bad;
        return true;
    }

    public boolean isSetBadForce() {
        return this.setBadForce;
    }

    public boolean isErrorState() {
        return this.isErrorState;
    }

    public boolean setIsErrorState(boolean state) {
        if (this.isErrorState == state) {
            return false;
        }
        this.isErrorState = state;
        return true;
    }

    public boolean setMaxRowsetCreationTime(long newCreationTime) {
        if (newCreationTime < maxRowsetCreationTime) {
            return false;
        }

        maxRowsetCreationTime = newCreationTime;
        return true;
    }

    public void setChecksum(long checksum) {
        this.checksum = checksum;
    }

    public boolean needFurtherRepair() {
        if (needFurtherRepair && System.currentTimeMillis() - this.furtherRepairSetTime < FURTHER_REPAIR_TIMEOUT_MS) {
            return true;
        }
        return false;
    }

    public void setNeedFurtherRepair(boolean needFurtherRepair) {
        this.needFurtherRepair = needFurtherRepair;
        this.furtherRepairSetTime = System.currentTimeMillis();
    }

    public boolean getDeferReplicaDeleteToNextReport() {
        return deferReplicaDeleteToNextReport;
    }

    public void setDeferReplicaDeleteToNextReport(boolean defer) {
        this.deferReplicaDeleteToNextReport = defer;
    }

    // only update data size and row num
    public synchronized void updateStat(long dataSize, long rowNum, long versionCount) {
        this.dataSize = dataSize;
        this.rowCount = rowNum;
        this.versionCount = versionCount;
    }

    public synchronized void updateRowCount(long newVersion, long minReadableVersion, long newDataSize,
                                            long newRowCount) {
        updateReplicaInfo(newVersion, this.lastFailedVersion,
                this.lastSuccessVersion, minReadableVersion, newDataSize, newRowCount);
    }

    public synchronized void updateRowCount(long newVersion, long newDataSize,
                                            long newRowCount) {
        updateReplicaInfo(newVersion, this.lastFailedVersion,
                this.lastSuccessVersion, this.minReadableVersion, newDataSize, newRowCount);
    }

    public synchronized void updateVersionInfo(long newVersion,
                                               long lastFailedVersion,
                                               long lastSuccessVersion) {
        updateReplicaInfo(newVersion, lastFailedVersion,
                lastSuccessVersion, this.minReadableVersion, dataSize, rowCount);
    }

    public synchronized void updateVersion(long version) {
        updateReplicaInfo(version, this.lastFailedVersion,
                this.lastSuccessVersion, this.minReadableVersion, dataSize, rowCount);
    }

    public synchronized void updateForRestore(long newVersion, long newDataSize,
                                              long newRowCount) {
        this.version = newVersion;
        this.lastFailedVersion = -1;
        this.lastSuccessVersion = newVersion;
        this.dataSize = newDataSize;
        this.rowCount = newRowCount;
        this.minReadableVersion = newVersion;
    }

    /* last failed version:  LFV
     * last success version: LSV
     * version:              V
     *
     * Case 1:
     *      If LFV > LSV, set LSV back to V, which indicates that version between LSV and LFV is invalid.
     *      Clone task will clone the version between LSV and LFV
     *
     * Case 2:
     *      LFV changed, set LSV back to V. This is just same as Case 1. Cause LFV must large than LSV.
     *
     * Case 3:
     *      LFV remains unchanged, just update LSV, and then check if it falls into Case 1.
     *
     * Case 4:
     *      V is larger or equal to LFV, reset LFV. And if V is less than LSV, just set V to LSV. This may
     *      happen when a clone task finished and report version V, but the LSV is already larger than V,
     *      And we know that version between V and LSV is valid, so move V forward to LSV.
     *
     * Case 5:
     *      This is a bug case, I don't know why, may be some previous version introduce it. It looks like
     *      the V(hash) equals to LSV(hash), and V equals to LFV, but LFV hash is 0 or some unknown number.
     *      We just reset the LFV(hash) to recovery this replica.
     */
    private void updateReplicaInfo(long newVersion,
                                   long lastFailedVersion,
                                   long lastSuccessVersion,
                                   long minReadableVersion,
                                   long newDataSize,
                                   long newRowCount) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("before update: {}", this.toString());
        }

        if (newVersion < this.version) {
            // This case means that replica meta version has been updated by ReportHandler before
            // For example, the publish version daemon has already sent some publish verison tasks to one be to publish version 2, 3, 4, 5, 6,
            // and the be finish all publish version tasks, the be's replica version is 6 now, but publish version daemon need to wait
            // for other be to finish most of publish version tasks to update replica version in fe.
            // At the moment, the replica version in fe is 4, when ReportHandler sync tablet, it find reported replica version in be is 6 and then
            // set version to 6 for replica in fe. And then publish version daemon try to finish txn, and use visible version(5)
            // to update replica. Finally, it find the newer version(5) is lower than replica version(6) in fe.
            if (LOG.isDebugEnabled()) {
                LOG.debug("replica {} on backend {}'s new version {} is lower than meta version {},"
                        + "not to continue to update replica", id, backendId, newVersion, this.version);
            }
            return;
        }

        this.version = newVersion;
        this.dataSize = newDataSize;
        this.rowCount = newRowCount;

        // just check it
        if (lastSuccessVersion <= this.version) {
            lastSuccessVersion = this.version;
        }

        // case 1:
        if (this.lastSuccessVersion <= this.lastFailedVersion) {
            this.lastSuccessVersion = this.version;
        }

        if (lastFailedVersion != this.lastFailedVersion) {
            // Case 2:
            if (lastFailedVersion > this.lastFailedVersion) {
                this.lastFailedVersion = lastFailedVersion;
                this.lastFailedTimestamp = System.currentTimeMillis();
            }

            this.lastSuccessVersion = this.version;
        } else {
            // Case 3:
            if (lastSuccessVersion >= this.lastSuccessVersion) {
                this.lastSuccessVersion = lastSuccessVersion;
            }
            if (lastFailedVersion >= this.lastSuccessVersion) {
                this.lastSuccessVersion = this.version;
            }
        }

        // Case 4:
        if (this.version >= this.lastFailedVersion) {
            this.lastFailedVersion = -1;
            this.lastFailedTimestamp = -1;
            if (this.version < this.lastSuccessVersion) {
                this.version = this.lastSuccessVersion;
            }
        }

        if (minReadableVersion <= this.version) {
            this.minReadableVersion = minReadableVersion;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("after update {}", this.toString());
        }
    }

    public synchronized void updateLastFailedVersion(long lastFailedVersion) {
        updateReplicaInfo(this.version, lastFailedVersion,
                this.lastSuccessVersion, this.minReadableVersion, dataSize, rowCount);
    }

    /*
     * Check whether the replica's version catch up with the expected version.
     * If ignoreAlter is true, and state is ALTER, and replica's version is PARTITION_INIT_VERSION,
     * just return true, ignore the version.
     *      This is for the case that when altering table, the newly created replica's version is PARTITION_INIT_VERSION,
     *      but we need to treat it as a "normal" replica which version is supposed to be "catch-up".
     *      But if state is ALTER but version larger than PARTITION_INIT_VERSION, which means this replica
     *      is already updated by load process, so we need to consider its version.
     */
    public boolean checkVersionCatchUp(long expectedVersion, boolean ignoreAlter) {
        if (ignoreAlter && state == ReplicaState.ALTER && version == Partition.PARTITION_INIT_VERSION) {
            return true;
        }

        if (expectedVersion == Partition.PARTITION_INIT_VERSION) {
            // no data is loaded into this replica, just return true
            return true;
        }

        if (this.version < expectedVersion) {
            LOG.debug("replica version does not catch up with version: {}. replica: {}",
                    expectedVersion, this);
            return false;
        }
        return true;
    }

    public void setState(ReplicaState replicaState) {
        this.state = replicaState;
    }

    public ReplicaState getState() {
        return this.state;
    }

    public long getVersionCount() {
        return versionCount;
    }

    public void setVersionCount(long versionCount) {
        this.versionCount = versionCount;
    }

    @Override
    public String toString() {
        StringBuffer strBuffer = new StringBuffer("[replicaId=");
        strBuffer.append(id);
        strBuffer.append(", BackendId=");
        strBuffer.append(backendId);
        strBuffer.append(", version=");
        strBuffer.append(version);
        strBuffer.append(", versionHash=");
        strBuffer.append(0);
        strBuffer.append(", minReadableVersion=");
        strBuffer.append(minReadableVersion);
        strBuffer.append(", lastReportVersion=");
        strBuffer.append(lastReportVersion);
        strBuffer.append(", dataSize=");
        strBuffer.append(dataSize);
        strBuffer.append(", rowCount=");
        strBuffer.append(rowCount);
        strBuffer.append(", lastFailedVersion=");
        strBuffer.append(lastFailedVersion);
        strBuffer.append(", lastFailedVersionHash=");
        strBuffer.append(0);
        strBuffer.append(", lastSuccessVersion=");
        strBuffer.append(lastSuccessVersion);
        strBuffer.append(", lastSuccessVersionHash=");
        strBuffer.append(0);
        strBuffer.append(", lastFailedTimestamp=");
        strBuffer.append(lastFailedTimestamp);
        strBuffer.append(", schemaHash=");
        strBuffer.append(schemaHash);
        strBuffer.append(", minReadableVersion=");
        strBuffer.append(minReadableVersion);
        strBuffer.append(", state=");
        strBuffer.append(state.name());
        strBuffer.append("]");
        return strBuffer.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(backendId);
        out.writeLong(version);
        out.writeLong(0); // write a version_hash for compatibility
        out.writeLong(dataSize);
        out.writeLong(rowCount);
        Text.writeString(out, state.name());

        out.writeLong(lastFailedVersion);
        out.writeLong(minReadableVersion); // originally used as version_hash, now reused as minReadableVersion
        out.writeLong(lastSuccessVersion);
        out.writeLong(0); // write a version_hash for compatibility
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        backendId = in.readLong();
        version = in.readLong();
        in.readLong(); // read a version_hash for compatibility
        dataSize = in.readLong();
        rowCount = in.readLong();
        state = ReplicaState.valueOf(Text.readString(in));
        lastFailedVersion = in.readLong();
        minReadableVersion = in.readLong(); // originally used as version_hash, now reused as minReadableVersion
        lastSuccessVersion = in.readLong();
        in.readLong(); // read a version_hash for compatibility
    }

    public static Replica read(DataInput in) throws IOException {
        Replica replica = new Replica();
        replica.readFields(in);
        return replica;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Replica)) {
            return false;
        }

        Replica replica = (Replica) obj;
        return (id == replica.id)
                && (backendId == replica.backendId)
                && (version == replica.version)
                && (dataSize == replica.dataSize)
                && (rowCount == replica.rowCount)
                && (state.equals(replica.state))
                && (lastFailedVersion == replica.lastFailedVersion)
                && (lastSuccessVersion == replica.lastSuccessVersion)
                && (minReadableVersion == replica.minReadableVersion);
    }

    private static class VersionComparator<T extends Replica> implements Comparator<T> {
        public VersionComparator() {
        }

        @Override
        public int compare(T replica1, T replica2) {
            if (replica1.getVersion() < replica2.getVersion()) {
                return 1;
            } else if (replica1.getVersion() == replica2.getVersion()) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    public void setWatermarkTxnId(long watermarkTxnId) {
        this.watermarkTxnId = watermarkTxnId;
    }

    public long getWatermarkTxnId() {
        return watermarkTxnId;
    }

    public void setLastReportVersion(long lastReportVersion) {
        this.lastReportVersion = lastReportVersion;
    }

    public long getLastReportVersion() {
        return this.lastReportVersion;
    }
}
