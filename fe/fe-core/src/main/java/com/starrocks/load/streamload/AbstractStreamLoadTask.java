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

    /**
     * Returns stream load information in Thrift format for querying information_schema.stream_loads.
     * 
     * <p>The returned {@link TStreamLoadInfo} objects contain all fields that match the columns defined in
     * {@link com.starrocks.catalog.system.information.StreamLoadsSystemTable}.
     * 
     * @return List of TStreamLoadInfo objects containing stream load task information
     */
    public abstract List<TStreamLoadInfo> toStreamLoadThrift();

    /**
     * Returns load information in Thrift format for querying information_schema.loads.
     * 
     * <p>The returned {@link TLoadInfo} objects contain all fields that match the columns defined in
     * {@link com.starrocks.catalog.system.information.LoadsSystemTable}.
     * 
     * @return List of TLoadInfo objects containing load task information
     */
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

    /**
     * Returns detailed information about the stream load task for SHOW STREAM LOAD statement.
     * 
     * <p>Each inner List&lt;String&gt; represents one row. The field definitions and order
     * can be found in {@link com.starrocks.sql.ast.ShowStreamLoadStmt#TITLE_NAMES}.
     * 
     * @return List of rows, where each row is a List&lt;String&gt; containing the fields
     *         defined in ShowStreamLoadStmt.TITLE_NAMES
     */
    public abstract List<List<String>> getShowInfo();

    /**
     * Returns brief information about the stream load task for SHOW PROC "/stream_loads".
     * 
     * <p>Each inner List&lt;String&gt; represents one row. The field definitions and order
     * can be found in {@link com.starrocks.common.proc.StreamLoadsProcDir#TITLE_NAMES}.
     * 
     * @return List of rows, where each row is a List&lt;String&gt; containing the fields
     *         defined in StreamLoadsProcDir.TITLE_NAMES
     */
    public abstract List<List<String>> getShowBriefInfo();
    public abstract String getStringByType();
}
