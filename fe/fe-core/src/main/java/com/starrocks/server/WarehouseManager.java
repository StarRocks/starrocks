package com.starrocks.server;

import com.google.gson.annotations.SerializedName;
import com.starrocks.warehouse.Warehouse;

import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WarehouseManager {
    @SerializedName(value = "nmap")
    private Map<String, Warehouse> nameToWarehouse;
    @SerializedName(value = "imap")
    private Map<Long, Warehouse> idToWarehouse;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // these apis need lock protection
    public void createWarehouse() {}
    public void dropWarehouse(String name) {}
    public void alterWarehouse(String name) {}
    public void showWarehouses(String pattern) {}

    // warehouse meta persistence api
    public void loadWarehouses() {}
    public void saveWarehouses() {}
    public void replayCreateWarehouse() {}
    public void replayDropWarehouse() {}
    public void replayAlterWarehouse() {}
}
