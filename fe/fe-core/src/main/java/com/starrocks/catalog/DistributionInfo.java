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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/DistributionInfo.java

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
import com.starrocks.common.io.Writable;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.DistributionInfoPB;
import com.starrocks.sql.ast.DistributionDesc;
import org.apache.commons.lang.NotImplementedException;

import java.util.List;
import java.util.Map;

public abstract class DistributionInfo implements Writable {

    public enum DistributionInfoType {
        HASH,
        RANDOM,
        RANGE
    }

    // for Gson runtime type adaptor
    @SerializedName(value = "typeStr")
    protected String typeStr;
    @SerializedName(value = "type")
    protected DistributionInfoType type;

    public DistributionInfo() {
        // for persist
    }

    public DistributionInfo(DistributionInfoType type) {
        this.type = type;
        this.typeStr = this.type.name();
    }

    /**
     * Image v3 form. `type` and `typeStr` are implied by the kind and rebuilt on load the way the typed
     * constructors set them.
     */
    public DistributionInfoPB toProto() {
        DistributionInfoPB.Builder builder = DistributionInfoPB.newBuilder();
        fillProtoVariant(builder);
        return builder.build();
    }

    /** Sets the kind and the variant message of the concrete class. Only the persistable subclasses override it. */
    protected void fillProtoVariant(DistributionInfoPB.Builder builder) {
        throw new IllegalStateException("distribution info class " + getClass().getName() + " cannot be persisted");
    }

    public static DistributionInfo fromProto(DistributionInfoPB pb) {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        DistributionInfo info;
        switch (pb.getKind()) {
            case HASH: {
                HashDistributionInfo hash = new HashDistributionInfo();
                hash.fillFromVariantProto(pb.getHash());
                info = hash;
                break;
            }
            case RANDOM: {
                RandomDistributionInfo random = new RandomDistributionInfo();
                random.fillFromVariantProto(pb.getRandom());
                info = random;
                break;
            }
            case RANGE:
                info = new RangeDistributionInfo();
                break;
            default:
                throw new IllegalArgumentException("unknown distribution info kind " + pb.getKind());
        }
        info.type = ProtoEnums.fromProto(pb.getKind(), DistributionInfoType.class);
        info.typeStr = info.type.name();
        return info;
    }

    public String getTypeStr() {
        return typeStr;
    }

    public DistributionInfoType getType() {
        return type;
    }

    public int getBucketNum() {
        // should override in sub class
        throw new NotImplementedException("not implemented");
    }

    public abstract boolean supportColocate();

    public abstract List<ColumnId> getDistributionColumns();

    public String getDistributionKey(Map<ColumnId, Column> schema) {
        return "";
    }

    public void setBucketNum(int bucketNum) {
        throw new NotImplementedException("not implemented");
    }

    public DistributionDesc toDistributionDesc(Map<ColumnId, Column> schema) {
        throw new NotImplementedException();
    }

    public DistributionInfo copy() {
        throw new NotImplementedException();
    }
    public String toSql(Map<ColumnId, Column> idToColumn) {
        return "";
    }
}
