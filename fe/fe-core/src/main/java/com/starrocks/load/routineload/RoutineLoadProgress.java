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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadProgress.java

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

package com.starrocks.load.routineload;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.RoutineLoadProgressPB;

public abstract class RoutineLoadProgress implements Writable {

    @SerializedName("s")
    protected LoadDataSourceType loadDataSourceType;
    protected boolean isTypeRead = false;

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public RoutineLoadProgress(LoadDataSourceType loadDataSourceType) {
        this.loadDataSourceType = loadDataSourceType;
    }

    abstract void update(RoutineLoadProgress progress);

    abstract String toJsonString();

    public RoutineLoadProgressPB toProto() {
        RoutineLoadProgressPB.Builder builder = RoutineLoadProgressPB.newBuilder();
        fillProtoVariant(builder);
        return builder.build();
    }

    protected void fillProtoVariant(RoutineLoadProgressPB.Builder builder) {
        throw new IllegalStateException("cannot persist " + getClass().getName());
    }

    protected void fillFromVariantProto(RoutineLoadProgressPB pb) {
        throw new IllegalStateException("cannot load " + getClass().getName());
    }

    public static RoutineLoadProgress fromProto(RoutineLoadProgressPB pb) {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        RoutineLoadProgress value;
        switch (pb.getKind()) {
            case KAFKA_PROGRESS:
                if (!pb.hasKafkaProgress()) {
                    throw new IllegalArgumentException("missing kafkaProgress variant");
                }
                value = new KafkaProgress();
                break;
            case PULSAR_PROGRESS:
                if (!pb.hasPulsarProgress()) {
                    throw new IllegalArgumentException("missing pulsarProgress variant");
                }
                value = new PulsarProgress();
                break;
            default:
                throw new IllegalArgumentException("unknown RoutineLoadProgress kind " + pb.getKind());
        }
        value.fillFromVariantProto(pb);
        return value;
    }
}
