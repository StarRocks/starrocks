// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.pseudocluster;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTabletStat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Tablet {
    private static final Logger LOG = LogManager.getLogger(Tablet.class);

    public static volatile int maxPendingVersions = 1000;
    // default 30 minutes
    public static volatile long versionExpireSec = 1800L;

    @SerializedName(value = "id")
    long id;
    @SerializedName(value = "tableId")
    long tableId;
    @SerializedName(value = "partitionId")
    long partitionId;
    @SerializedName(value = "schemaHash")
    int schemaHash;
    @SerializedName(value = "enablePersistentIndex")
    boolean enablePersistentIndex;

    private AtomicInteger cloneExecuted = new AtomicInteger(0);

    private int readExecuted = 0;
    private long lastReadVersion = -1;
    private long lastSuccessReadVersion = -1;
    private long lastFailedReadVersion = -1;

    private static AtomicInteger totalReadExecuted = new AtomicInteger(0);
    private static AtomicInteger totalReadFailed = new AtomicInteger(0);
    private static AtomicInteger totalReadSucceed = new AtomicInteger(0);
    private static AtomicInteger totalVersionGCed = new AtomicInteger(0);

    public synchronized TTabletInfo toThrift() {
        TTabletInfo info = new TTabletInfo();
        info.setTablet_id(id);
        info.setPartition_id(partitionId);
        info.setSchema_hash(schemaHash);
        info.setStorage_medium(TStorageMedium.SSD);
        info.setPath_hash(PseudoBackend.PATH_HASH);
        info.setIs_in_memory(false);
        info.setVersion(maxContinuousVersion());
        info.setVersion_miss(!pendingRowsets.isEmpty());
        info.setRow_count(getRowCount());
        info.setData_size(getDataSize());
        // TODO: fill expire txn ids
        return info;
    }

    static class EditVersion {
        @SerializedName(value = "major")
        long major;
        @SerializedName(value = "minor")
        long minor;
        @SerializedName(value = "rowsets")
        List<Rowset> rowsets = Lists.newArrayList();
        @SerializedName(value = "delta")
        Rowset delta;
        @SerializedName(value = "createTimeMs")
        long createTimeMs;

        EditVersion(long major, long minor) {
            this.major = major;
            this.minor = minor;
        }
    }

    @SerializedName(value = "versions")
    List<EditVersion> versions;

    @SerializedName(value = "nextRssId")
    int nextRssId = 0;

    @SerializedName(value = "pendingRowsets")
    TreeMap<Long, Rowset> pendingRowsets = new TreeMap<>();

    public Tablet(long id, long tableId, long partitionId, int schemaHash, boolean enablePersistentIndex) {
        this.id = id;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.schemaHash = schemaHash;
        this.enablePersistentIndex = enablePersistentIndex;
        versions = Lists.newArrayList(new EditVersion(1, 0));
    }

    public synchronized int numRowsets() {
        return versions.get(versions.size() - 1).rowsets.size();
    }

    public long getRowCount() {
        return numRowsets() * 1000;
    }

    public long getDataSize() {
        return numRowsets() * 100000;
    }

    public synchronized List<Long> getMissingVersions() {
        if (pendingRowsets.size() == 0) {
            return Lists.newArrayList(maxContinuousVersion() + 1);
        }
        List<Long> ret = Lists.newArrayList();
        for (long v = maxContinuousVersion() + 1; v <= pendingRowsets.lastKey() + 1; v++) {
            if (!pendingRowsets.containsKey(v)) {
                ret.add(v);
            }
        }
        return ret;
    }

    private synchronized EditVersion getMaxContinuousEditVersion() {
        return versions.get(versions.size() - 1);
    }

    public synchronized long maxContinuousVersion() {
        return versions.get(versions.size() - 1).major;
    }

    public synchronized long maxVersion() {
        if (pendingRowsets.isEmpty()) {
            return maxContinuousVersion();
        } else {
            return pendingRowsets.lastKey();
        }
    }

    public synchronized long minVersion() {
        return versions.get(0).major;
    }

    public synchronized void read(long version) throws Exception {
        totalReadExecuted.incrementAndGet();
        readExecuted++;
        lastReadVersion = version;
        long currentVersion = maxContinuousVersion();
        if (version > currentVersion) {
            totalReadFailed.incrementAndGet();
            lastFailedReadVersion = version;
            String msg = String.format("be:%d read tablet:%d version:%d > currentVersion:%d",
                    PseudoBackend.getCurrentBackend().getId(), version,
                    currentVersion);
            LOG.warn(msg);
            throw new Exception(msg);
        }
        totalReadSucceed.incrementAndGet();
        lastSuccessReadVersion = version;
    }

    public int getCloneExecuted() {
        return cloneExecuted.get();
    }

    public synchronized int getReadExecuted() {
        return readExecuted;
    }

    public synchronized long getLastReadVersion() {
        return lastReadVersion;
    }

    public synchronized long getLastSuccessReadVersion() {
        return lastSuccessReadVersion;
    }

    public synchronized long getLastFailedReadVersion() {
        return lastFailedReadVersion;
    }

    public static int getTotalReadExecuted() {
        return totalReadExecuted.get();
    }

    public static int getTotalReadFailed() {
        return totalReadFailed.get();
    }

    public static int getTotalReadSucceed() {
        return totalReadSucceed.get();
    }

    public static int getTotalVersionGCed() {
        return totalVersionGCed.get();
    }

    public synchronized void commitRowset(Rowset rowset, long version) throws Exception {
        EditVersion lastVersion = versions.get(versions.size() - 1);
        if (version <= lastVersion.major) {
            LOG.info("tablet:{} ignore rowset commit, version {} <= {}", id, version, lastVersion.major);
        } else if (version == lastVersion.major + 1) {
            commitNextRowset(rowset, version, lastVersion);
            while (!pendingRowsets.isEmpty()) {
                lastVersion = versions.get(versions.size() - 1);
                Map.Entry<Long, Rowset> e = pendingRowsets.firstEntry();
                if (e.getKey() == lastVersion.major + 1) {
                    commitNextRowset(e.getValue(), e.getKey(), lastVersion);
                    pendingRowsets.remove(e.getKey());
                } else {
                    break;
                }
            }
        } else {
            if (pendingRowsets.size() >= maxPendingVersions) {
                throw new Exception(String.format("tablet:%d commit version:%d failed pendingRowsets size:%d >= %d", id, version,
                        pendingRowsets.size(), maxPendingVersions));
            }
            pendingRowsets.put(version, rowset);
            LOG.info("tablet:{} add rowset {} to pending #{}, version {}", id, rowset.rowsetid, pendingRowsets.size(), version);
        }
    }

    private void commitNextRowset(Rowset rowset, long version, EditVersion lastVersion) {
        rowset.id = ++nextRssId;
        EditVersion ev = new EditVersion(version, 0);
        ev.rowsets.addAll(lastVersion.rowsets);
        ev.rowsets.add(rowset);
        ev.delta = rowset;
        ev.createTimeMs = System.currentTimeMillis();
        versions.add(ev);
        LOG.info("txn: {} tablet:{} rowset commit, version:{} rowset:{} #version:{} #rowset:{}", rowset.txnId, id, version,
                rowset.id, versions.size(), ev.rowsets.size());
    }

    public TTabletStat getStats() {
        TTabletStat stat = new TTabletStat();
        stat.setTablet_id(id);
        stat.setData_size(getDataSize());
        stat.setRow_num(getRowCount());
        return stat;
    }

    public TTabletInfo getTabletInfo() {
        TTabletInfo info = new TTabletInfo(id, schemaHash, maxContinuousVersion(), 1, getRowCount(), getDataSize());
        return info;
    }

    private Rowset getRowsetByVersion(long version) {
        for (EditVersion ev : versions) {
            if (ev.major == version && ev.delta != null) {
                return ev.delta;
            }
        }
        return pendingRowsets.get(version);
    }

    public synchronized List<Pair<Long, Rowset>> getRowsetsByMissingVersionList(List<Long> missingVersions) {
        List<Pair<Long, Rowset>> ret = Lists.newArrayList();
        for (int i = 0; i < missingVersions.size() - 1; i++) {
            long version = missingVersions.get(i);
            Rowset rowset = getRowsetByVersion(version);
            if (rowset != null) {
                ret.add(new Pair<>(version, rowset));
            }
        }
        for (long v = missingVersions.get(missingVersions.size() - 1); v <= maxContinuousVersion(); v++) {
            Rowset rowset = getRowsetByVersion(v);
            if (rowset != null) {
                ret.add(new Pair<>(v, rowset));
            } else {
                break;
            }
        }
        return ret;
    }

    public synchronized String versionInfo() {
        return String.format("[%d-%d #pending:%d]", versions.get(0).major, maxContinuousVersion(), pendingRowsets.size());
    }

    public synchronized void cloneFrom(Tablet src) throws Exception {
        if (maxContinuousVersion() >= src.maxContinuousVersion()) {
            LOG.warn("tablet {} clone, nothing to copy src:{} dest:{}", id, src.versionInfo(),
                    versionInfo());
            return;
        }
        String oldInfo = versionInfo();
        List<Long> missingVersions = getMissingVersions();
        if (missingVersions.get(0) < src.minVersion()) {
            LOG.warn(String.format("incremental clone failed src:%d versions:[%d,%d] dest:%d missing::%s", src.id,
                    src.minVersion(), src.maxContinuousVersion(), id, missingVersions));
            fullCloneFrom(src);
            return;
        }
        List<Pair<Long, Rowset>> versionAndRowsets = src.getRowsetsByMissingVersionList(missingVersions);
        for (Pair<Long, Rowset> p : versionAndRowsets) {
            commitRowset(p.second.copy(), p.first);
        }
        cloneExecuted.incrementAndGet();
        LOG.info("tablet:{} incremental clone src:{} before:{} after:{}", id, src.versionInfo(), oldInfo, versionInfo());
    }

    private void fullCloneFrom(Tablet src) throws Exception {
        String oldInfo = versionInfo();
        // only copy the maxContinuousVersion, not pendingRowsets, to be same as current BE's behavior
        EditVersion srcVersion = src.getMaxContinuousEditVersion();
        EditVersion destVersion = new EditVersion(srcVersion.major, srcVersion.minor);
        destVersion.rowsets = srcVersion.rowsets.stream().map(Rowset::copy).collect(Collectors.toList());
        destVersion.createTimeMs = System.currentTimeMillis();
        versions = Lists.newArrayList(destVersion);
        pendingRowsets.clear();
        nextRssId = destVersion.rowsets.stream().map(Rowset::getId).reduce(Integer::max).orElse(0);
        LOG.info("tablet:{} full clone src:{} before:{} after:{}", id, src.id, oldInfo, versionInfo());
    }

    public synchronized void versionGC() {
        long expireTs = System.currentTimeMillis() - versionExpireSec * 1000L;
        int i = 0;
        for (; i < versions.size() - 1; i++) {
            EditVersion ev = versions.get(i);
            if (ev.createTimeMs > expireTs) {
                break;
            }
        }
        if (i == 0) {
            return;
        }
        LOG.info("tablet:{} versionGC [{},{}]{} -> [{},{}]{} remove {} versions",
                id, versions.get(0).major, versions.get(versions.size() - 1).major, versions.size(), versions.get(i).major,
                versions.get(versions.size() - 1).major, versions.size() - i, i);
        List<EditVersion> newVersions = new ArrayList<>(i);
        for (int j = i; j < versions.size(); j++) {
            newVersions.add(versions.get(j));
        }
        versions = newVersions;
        totalVersionGCed.addAndGet(i);
    }

    public static void main(String[] args) {
        Tablet tablet = new Tablet(1, 1, 1, 1, true);
        String json = GsonUtils.GSON.toJson(tablet);
        System.out.println(json);
        Tablet newTablet = GsonUtils.GSON.fromJson(json, Tablet.class);
    }
}
