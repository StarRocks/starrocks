package com.starrocks.pseudocluster;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTabletStat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Tablet {
    private static final Logger LOG = LogManager.getLogger(Tablet.class);

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

    public synchronized TTabletInfo toThrift() {
        TTabletInfo info = new TTabletInfo();
        info.setTablet_id(id);
        info.setPartition_id(partitionId);
        info.setSchema_hash(schemaHash);
        info.setStorage_medium(TStorageMedium.SSD);
        info.setPath_hash(1);
        info.setIs_in_memory(false);
        info.setVersion(max_continuous_version());
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

    public synchronized int num_rowsets() {
        return versions.get(versions.size() - 1).rowsets.size();
    }

    public long getRowCount() {
        return num_rowsets() * 1000;
    }

    public long getDataSize() {
        return num_rowsets() * 100000;
    }

    public synchronized long max_continuous_version() {
        return versions.get(versions.size() - 1).major;
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
            // TODO: simulate number of pending rowset limit reached
            pendingRowsets.put(version, rowset);
            LOG.info("tablet:{} add rowset {} to pending #{}, version {}", id, rowset.rowsetid, pendingRowsets.size(), version);
        }
    }

    private void commitNextRowset(Rowset rowset, long version, EditVersion lastVersion) {
        rowset.id = ++nextRssId;
        EditVersion ev = new EditVersion(version, 0);
        ev.rowsets.addAll(lastVersion.rowsets);
        ev.rowsets.add(rowset);
        versions.add(ev);
        LOG.info("txn: {} tablet:{} rowset commit, version:{} rowset:{} #rowset:{}", rowset.txnId, id, version, rowset.id,
                ev.rowsets.size());
    }

    public TTabletStat getStats() {
        TTabletStat stat = new TTabletStat();
        stat.setTablet_id(id);
        stat.setData_size(getDataSize());
        stat.setRow_num(getRowCount());
        return stat;
    }

    public static void main(String[] args) {
        Tablet tablet = new Tablet(1, 1, 1, 1, true);
        String json = GsonUtils.GSON.toJson(tablet);
        System.out.println(json);
        Tablet newTablet = GsonUtils.GSON.fromJson(json, Tablet.class);
    }
}
