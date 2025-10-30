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

package com.starrocks.epack.warehouse;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.WarmupLevel;
import com.starrocks.common.DdlException;
import com.starrocks.qe.GlobalVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WarehouseProperty {
    private static final Logger LOG = LogManager.getLogger(WarehouseProperty.class);

    public static final String PROPERTY_COMPUTE_REPLICA = "compute_replica";
    public static final String PROPERTY_REPLICATION_TYPE = "replication_type";
    public static final String PROPERTY_WARMUP_LEVEL = "warmup_level";
    public static final int DEFAULT_REPLICA_NUMBER = 1;

    // query queue
    public static final String PROPERTY_ENABLE_QUERY_QUEUE = "enable_query_queue";
    public static final String PROPERTY_ENABLE_QUERY_QUEUE_LOAD = "enable_query_queue_load";
    public static final String PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC = "enable_query_queue_statistic";
    public static final String PROPERTY_QUERY_QUEUE_CONCURRENCY_LIMIT = "query_queue_concurrency_limit";
    public static final String PROPERTY_QUERY_QUEUE_MAX_QUEUED_QUERIES = "query_queue_max_queued_queries";
    public static final String PROPERTY_QUERY_QUEUE_PENDING_TIMEOUT_SECOND = "query_queue_pending_timeout_second";

    public enum ReplicationType {
        NONE,
        SYNC,
        ASYNC,
    }

    public enum WarmupLevelType {
        NONE,
        META,
        INDEX,
        ALL,
    }

    @SerializedName(value = "compute_replica")
    private int computeReplica;

    @SerializedName(value = "replication_type")
    private ReplicationType replicationType;

    @SerializedName(value = "warmup_level")
    private WarmupLevelType warmupLevel;

    @SerializedName(value = "enable_query_queue")
    private boolean enableQueryQueue;
    @SerializedName(value = "enable_query_queue_load")
    private boolean enableQueryQueueLoad;
    @SerializedName(value = "enable_query_queue_statistic")
    private boolean enableQueryQueueStatistic;
    @SerializedName(value = "query_queue_max_queued_queries")
    private int queryQueueMaxQueuedQueries = GlobalVariable.getQueryQueueMaxQueuedQueries();
    // The timeout for a query to be pending in the queue, in seconds.
    // Set to 600 seconds by default because warehouse autoscale will take more than 5 minutes to scale up.
    @SerializedName(value = "query_queue_pending_timeout_second")
    private int queryQueuePendingTimeoutSecond = Math.max(600, GlobalVariable.getQueryQueuePendingTimeoutSecond());
    @SerializedName(value = "query_queue_concurrency_limit")
    private int queryQueueConcurrencyLimit = -1;

    public WarehouseProperty() {
        this.computeReplica = DEFAULT_REPLICA_NUMBER;
        this.replicationType = ReplicationType.NONE;
        this.warmupLevel = WarmupLevelType.NONE;
        this.enableQueryQueue = false;
        this.enableQueryQueueLoad = false;
        this.enableQueryQueueStatistic = false;
    }

    // deep copy
    public WarehouseProperty(WarehouseProperty that) {
        this.computeReplica = that.computeReplica;
        this.replicationType = that.replicationType;
        this.warmupLevel = that.warmupLevel;
        this.enableQueryQueue = that.enableQueryQueue;
        this.enableQueryQueueLoad = that.enableQueryQueueLoad;
        this.enableQueryQueueStatistic = that.enableQueryQueueStatistic;
        this.queryQueueMaxQueuedQueries = that.queryQueueMaxQueuedQueries;
        this.queryQueuePendingTimeoutSecond = that.queryQueuePendingTimeoutSecond;
        this.queryQueueConcurrencyLimit = that.queryQueueConcurrencyLimit;
    }

    public WarehouseProperty(int computeReplica, ReplicationType repType, WarmupLevelType warmupLevel, boolean enableQueryQueue) {
        this.computeReplica = computeReplica;
        this.replicationType = repType;
        this.warmupLevel = warmupLevel;
        this.enableQueryQueue = enableQueryQueue;
    }

    public void setComputeReplica(int computeReplica) {
        this.computeReplica = computeReplica;
    }

    public int getComputeReplica() {
        return computeReplica;
    }

    public void setReplicationType(ReplicationType type) {
        this.replicationType = type;
    }

    public ReplicationType getReplicationType() {
        return replicationType;
    }

    public void setWarmupLevel(WarmupLevelType warmupLevel) {
        this.warmupLevel = warmupLevel;
    }

    public WarmupLevelType getWarmupLevel() {
        return warmupLevel;
    }

    public boolean isEnableQueryQueue() {
        return enableQueryQueue;
    }

    public void setEnableQueryQueue(boolean enableQueryQueue) {
        this.enableQueryQueue = enableQueryQueue;
    }

    public boolean isEnableQueryQueueLoad() {
        return enableQueryQueueLoad;
    }

    public void setEnableQueryQueueLoad(boolean enableQueryQueueLoad) {
        this.enableQueryQueueLoad = enableQueryQueueLoad;
    }

    public boolean isEnableQueryQueueStatistic() {
        return enableQueryQueueStatistic;
    }

    public void setEnableQueryQueueStatistic(boolean enableQueryQueueStatistic) {
        this.enableQueryQueueStatistic = enableQueryQueueStatistic;
    }

    public int getQueryQueueMaxQueuedQueries() {
        return queryQueueMaxQueuedQueries;
    }

    public void setQueryQueueMaxQueuedQueries(int queryQueueMaxQueuedQueries) {
        this.queryQueueMaxQueuedQueries = queryQueueMaxQueuedQueries;
    }

    public int getQueryQueuePendingTimeoutSecond() {
        return queryQueuePendingTimeoutSecond;
    }

    public void setQueryQueuePendingTimeoutSecond(int queryQueuePendingTimeoutSecond) {
        this.queryQueuePendingTimeoutSecond = queryQueuePendingTimeoutSecond;
    }

    public int getQueryQueueConcurrencyLimit() {
        return queryQueueConcurrencyLimit;
    }

    public void setQueryQueueConcurrencyLimit(int queryQueueConcurrencyLimit) {
        this.queryQueueConcurrencyLimit = queryQueueConcurrencyLimit;
    }

    public String toString() {
        return new Gson().toJson(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        WarehouseProperty prop = (WarehouseProperty) obj;
        return this.computeReplica == prop.computeReplica && this.warmupLevel == prop.warmupLevel &&
                this.replicationType == prop.replicationType && this.enableQueryQueue == prop.enableQueryQueue
                && this.enableQueryQueueLoad == prop.enableQueryQueueLoad
                && this.enableQueryQueueStatistic == prop.enableQueryQueueStatistic
                && this.queryQueueMaxQueuedQueries == prop.queryQueueMaxQueuedQueries
                && this.queryQueuePendingTimeoutSecond == prop.queryQueuePendingTimeoutSecond
                && this.queryQueueConcurrencyLimit == prop.queryQueueConcurrencyLimit;
    }

    public static ReplicationType replicationTypeFromString(String strType) throws DdlException {
        if (strType.equalsIgnoreCase(ReplicationType.SYNC.toString())) {
            return ReplicationType.SYNC;
        } else if (strType.equalsIgnoreCase(ReplicationType.ASYNC.toString())) {
            return ReplicationType.ASYNC;
        } else if (strType.equalsIgnoreCase(ReplicationType.NONE.toString())) {
            return ReplicationType.NONE;
        } else {
            throw new DdlException("warehouse replication type can only be SYNC or ASYNC or NONE");
        }
    }

    public static WarmupLevelType warmupLevelTypeFromString(String strType) throws DdlException {
        if (strType.equalsIgnoreCase(WarmupLevelType.NONE.toString())) {
            return WarmupLevelType.NONE;
        } else if (strType.equalsIgnoreCase(WarmupLevelType.META.toString())) {
            return WarmupLevelType.META;
        } else if (strType.equalsIgnoreCase(WarmupLevelType.INDEX.toString())) {
            return WarmupLevelType.INDEX;
        } else if (strType.equalsIgnoreCase(WarmupLevelType.ALL.toString())) {
            return WarmupLevelType.ALL;
        } else {
            throw new DdlException(
                    "warehouse warmup level type can only be one of the following choices: {'none', 'meta', 'index' and 'all'}!");
        }
    }

    public static WarmupLevel toStarOSWarmupLevel(WarmupLevelType warmupLevelType) throws DdlException {
        return switch (warmupLevelType) {
            case NONE -> WarmupLevel.WARMUP_NOTHING;
            case META -> WarmupLevel.WARMUP_META;
            case INDEX -> WarmupLevel.WARMUP_INDEX;
            case ALL -> WarmupLevel.WARMUP_ALL;
            default -> throw new DdlException("Unknown warmup level " + warmupLevelType);
        };
    }

    public static com.staros.proto.ReplicationType toStarOSReplicationType(ReplicationType replicationType)
            throws DdlException {
        return switch (replicationType) {
            case NONE -> com.staros.proto.ReplicationType.NO_REPLICATION;
            case SYNC -> com.staros.proto.ReplicationType.SYNC;
            case ASYNC -> com.staros.proto.ReplicationType.ASYNC;
            default -> throw new DdlException("Unknown replication type " + replicationType);
        };
    }
}
