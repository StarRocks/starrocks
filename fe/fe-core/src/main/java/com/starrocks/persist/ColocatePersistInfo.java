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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/ColocatePersistInfo.java

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

package com.starrocks.persist;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.proto.BackendBucketsPB;
import com.starrocks.persist.proto.ColocatePersistInfoPB;
import com.starrocks.persist.proto.ColocateRangeListPB;
import com.starrocks.persist.proto.Int64ListPB;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * PersistInfo for ColocateTableIndex
 */
public class ColocatePersistInfo implements Writable {
    @SerializedName("gp")
    private GroupId groupId;
    @SerializedName("tb")
    private long tableId;
    @SerializedName("bs")
    private List<List<Long>> backendsPerBucketSeq = Lists.newArrayList();
    // Set only on the add-table record of a range colocate member: the group's ColocateRanges when the member joined,
    // applied by replay together with the membership. Absent (null) on every other record and on records from older
    // versions.
    @SerializedName("cr")
    private List<ColocateRange> colocateRanges;

    public ColocatePersistInfo() {

    }

    public static ColocatePersistInfo createForAddTable(GroupId groupId, long tableId,
                                                        List<List<Long>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(groupId, tableId, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo createForAddTable(GroupId groupId, long tableId,
                                                        List<List<Long>> backendsPerBucketSeq,
                                                        List<ColocateRange> colocateRanges) {
        ColocatePersistInfo info = new ColocatePersistInfo(groupId, tableId, backendsPerBucketSeq);
        info.colocateRanges = new ArrayList<>(colocateRanges);
        return info;
    }

    public static ColocatePersistInfo createForBackendsPerBucketSeq(GroupId groupId,
                                                                    List<List<Long>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(groupId, -1L, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo createForMarkUnstable(GroupId groupId) {
        return new ColocatePersistInfo(groupId, -1L, new ArrayList<>());
    }

    public static ColocatePersistInfo createForMarkStable(GroupId groupId) {
        return new ColocatePersistInfo(groupId, -1L, new ArrayList<>());
    }

    public static ColocatePersistInfo createForRemoveTable(long tableId) {
        return new ColocatePersistInfo(new GroupId(-1, -1), tableId, new ArrayList<>());
    }

    private ColocatePersistInfo(GroupId groupId, long tableId, List<List<Long>> backendsPerBucketSeq) {
        this.groupId = groupId;
        this.tableId = tableId;
        this.backendsPerBucketSeq = backendsPerBucketSeq;
    }

    public long getTableId() {
        return tableId;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public List<List<Long>> getBackendsPerBucketSeq() {
        return backendsPerBucketSeq;
    }

    public List<ColocateRange> getColocateRanges() {
        return colocateRanges == null ? List.of() : colocateRanges;
    }




    @Override
    public int hashCode() {
        return Objects.hashCode(groupId, tableId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ColocatePersistInfo)) {
            return false;
        }

        ColocatePersistInfo info = (ColocatePersistInfo) obj;

        return tableId == info.tableId
                && groupId.equals(info.groupId)
                && backendsPerBucketSeq.equals(info.backendsPerBucketSeq)
                && Objects.equal(colocateRanges, info.colocateRanges);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table id: ").append(tableId);
        sb.append(" group id: ").append(groupId);
        sb.append(" backendsPerBucketSeq: ").append(backendsPerBucketSeq);
        if (colocateRanges != null) {
            sb.append(" colocateRanges: ").append(colocateRanges);
        }
        return sb.toString();
    }

    public ColocatePersistInfoPB toProto() {
        ColocatePersistInfoPB.Builder builder = ColocatePersistInfoPB.newBuilder();
        if (groupId != null) {
            builder.setGroupId(groupId.toProto());
        }
        builder.setTableId(tableId);
        if (backendsPerBucketSeq != null) {
            builder.setBackendsPerBucketSeq(BackendBucketsPB.newBuilder().addAllValues(backendsPerBucketSeq.stream()
                    .map(ids -> Int64ListPB.newBuilder().addAllValues(ids).build()).collect(Collectors.toList())).build());
        }
        if (colocateRanges != null) {
            builder.setColocateRanges(ColocateRangeListPB.newBuilder().addAllValues(colocateRanges.stream()
                    .map(ColocateRange::toProto).collect(Collectors.toList())));
        }
        return builder.build();
    }

    public static ColocatePersistInfo fromProto(ColocatePersistInfoPB pb) {
        ColocatePersistInfo value = new ColocatePersistInfo();
        if (pb.hasGroupId()) {
            value.groupId = GroupId.fromProto(pb.getGroupId());
        } else {
            value.groupId = null;
        }
        if (pb.hasTableId()) {
            value.tableId = pb.getTableId();
        }
        if (pb.hasBackendsPerBucketSeq()) {
            value.backendsPerBucketSeq = pb.getBackendsPerBucketSeq().getValuesList().stream()
                    .map(ids -> new ArrayList<Long>(ids.getValuesList())).collect(Collectors.toList());
        } else {
            value.backendsPerBucketSeq = null;
        }
        value.colocateRanges = pb.hasColocateRanges() ? pb.getColocateRanges().getValuesList().stream()
                .map(ColocateRange::fromProto).collect(Collectors.toList()) : null;
        return value;
    }
}
