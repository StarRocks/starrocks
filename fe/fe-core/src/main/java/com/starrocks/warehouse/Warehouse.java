package com.starrocks.warehouse;

import com.google.gson.annotations.SerializedName;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Warehouse {
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "id")
    private long id;

    private Enum WarehouseState {
        INITIALIZING,
                RUNNING,
                SUSPENDED,
                SCALING
    }

    @SerializedName(value = "st")
    private WarehouseState state;

    // cluster id -> cluster obj
    @SerializedName(value = "cmap")
    private Map<Long, Cluster> clusters;

    // properties
    @SerializedName(value = "size")
    private String size = "${default_size}";
    @SerializedName(value = "minc")
    private int min_cluster = 1;
    @SerializedName(value = "maxc")
    private int max_cluster = 5;
    // and others ...

    private AtomicInteger numPendingSqls;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public void addCluster() {}
    public void removeCluster() {}
    public void suspend() {}
    public void resume() {}
    public void showClusters() {}

    // property setters and getters
    public void setSize(String size) {}
    public void getSize() {}
    // and others...

    public long numTotalRunningSqls() {}
    public int getPendingSqls() {}
    public int setPendingSqls(int val) {}
    public int addAndGetPendingSqls(int delta) {}
}
