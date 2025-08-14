// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

package com.starrocks.load.streamload;

import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.AbstractTxnStateChangeCallback;
import com.starrocks.warehouse.LoadJobWithWarehouse;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.List;

/**
 * Abstract base class for stream load tasks
 */
public abstract class AbstractStreamLoadTask extends AbstractTxnStateChangeCallback
        implements Writable, GsonPostProcessable, GsonPreProcessable, LoadJobWithWarehouse {

    public abstract void beginTxnFromFrontend(TransactionResult resp);
    public abstract void beginTxnFromFrontend(int channelId, int channelNum, TransactionResult resp);
    public abstract void beginTxnFromBackend(TUniqueId requestId, String clientIp, long backendId, TransactionResult resp);
    public abstract TNetworkAddress tryLoad(int channelId, String tableName, TransactionResult resp) throws StarRocksException;
    public abstract TNetworkAddress executeTask(int channelId, String tableName, HttpHeaders headers, TransactionResult resp);
    public abstract void prepareChannel(int channelId, String tableName, HttpHeaders headers, TransactionResult resp);
    public abstract void waitCoordFinishAndPrepareTxn(long preparedTimeoutMs, TransactionResult resp);
    public abstract void commitTxn(HttpHeaders headers, TransactionResult resp) throws StarRocksException;
    public abstract void manualCancelTask(TransactionResult resp) throws StarRocksException;
    public abstract boolean checkNeedRemove(long currentMs, boolean isForce);
    public abstract boolean checkNeedPrepareTxn();
    public abstract boolean isDurableLoadState();
    public abstract void cancelAfterRestart();
    public abstract List<TStreamLoadInfo> toStreamLoadThrift();
    public abstract List<TLoadInfo> toThrift();
    public abstract void init();

    // Common getters used by StreamLoadMgr
    public abstract long getId();
    public abstract String getLabel();
    public abstract String getDBName();
    public abstract long getDBId();
    public abstract long getTxnId();
    public abstract String getTableName();
    public abstract String getStateName();
    public abstract boolean isFinalState();
    public abstract long createTimeMs();
    public abstract long endTimeMs();
    public abstract long getFinishTimestampMs();
    public abstract List<List<String>> getShowInfo();
    public abstract List<List<String>> getShowBriefInfo();
    public abstract String getStringByType();
}
