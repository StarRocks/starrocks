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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WarehouseProperty {
    private static final Logger LOG = LogManager.getLogger(WarehouseProperty.class);

    public static final String PROPERTY_COMPUTE_REPLICA = "compute_replica";
    public static final String PROPERTY_REPLICATION_TYPE = "replication_type";
    public static final String PROPERTY_WARMUP_LEVEL = "warmup_level";
    public static final int DEFAULT_REPLICA_NUMBER = 1;

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

    public WarehouseProperty() {
        this.computeReplica = DEFAULT_REPLICA_NUMBER;
        this.replicationType = ReplicationType.NONE;
        this.warmupLevel = WarmupLevelType.NONE;
    }

    // deep copy
    public WarehouseProperty(WarehouseProperty that) {
        this.computeReplica = that.computeReplica;
        this.replicationType = that.replicationType;
        this.warmupLevel = that.warmupLevel;
    }

    public WarehouseProperty(int computeReplica, ReplicationType repType, WarmupLevelType warmupLevel) {
        this.computeReplica = computeReplica;
        this.replicationType = repType;
        this.warmupLevel = warmupLevel;
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
                this.replicationType == prop.replicationType;
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
