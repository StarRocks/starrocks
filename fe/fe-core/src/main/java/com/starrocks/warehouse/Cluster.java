package com.starrocks.warehouse;

import com.google.gson.annotations.SerializedName;

import java.util.concurrent.atomic.AtomicInteger;

public class Cluster {
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "wgid")
    private long workerGroupId;

    // 注：Warehouse 和 Cluster 对象只记录了running 和 pending sql 的数量,
    // 我们假设 sql 排队功能 和 Cluster 类完全解耦，Cluster 类提供接口,
    // sql 排队功能根据 sql 执行情况负责调用这些接口更新 counter
    private AtomicInteger numRunningSqls;

    public Cluster(long id, long workerGroupId) {}

    // set the associated worker group id when resizing
    public void setWorkerGroupId(long id) {}
    public long getWorkerGroupId() {}

    public int getRunningSqls() {}
    public int setRunningSqls(int val) {}
    public int addAndGetRunningSqls(int delta) {}
}
