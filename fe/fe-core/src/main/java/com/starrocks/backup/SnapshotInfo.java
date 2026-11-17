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

package com.starrocks.backup;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.backup.LakeTableSnapshotInfo;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.SnapshotInfoCorePB;
import com.starrocks.persist.proto.SnapshotInfoPB;
import com.starrocks.persist.proto.StringListPB;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SnapshotInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tblId")
    private long tblId;
    @SerializedName(value = "partitionId")
    private long partitionId;
    @SerializedName(value = "indexId")
    private long indexId;
    @SerializedName(value = "tabletId")
    private long tabletId;
    @SerializedName(value = "beId")
    private long beId;
    @SerializedName(value = "schemaHash")
    private int schemaHash;
    // eg: /path/to/your/be/data/snapshot/20180410102311.0.86400/
    @SerializedName(value = "path")
    private String path;
    // eg:
    // 10006_0_1_0_0.dat
    // 10006_2_2_0_0.idx
    // 10006.hdr
    @SerializedName(value = "files")
    private List<String> files = Lists.newArrayList();

    public SnapshotInfo() {
        // for persist
    }

    public SnapshotInfo(long dbId, long tblId, long partitionId, long indexId, long tabletId,
                        long beId, int schemaHash, String path, List<String> files) {
        this.dbId = dbId;
        this.tblId = tblId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.beId = beId;
        this.schemaHash = schemaHash;
        this.path = path;
        this.files = files;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTblId() {
        return tblId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBeId() {
        return beId;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public String getPath() {
        return path;
    }

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

    public String getTabletPath() {
        String basePath = Joiner.on("/").join(path, tabletId, schemaHash);
        return basePath;
    }




    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId);
        sb.append(", be id: ").append(beId);
        sb.append(", path: ").append(path);
        sb.append(", files:").append(files);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SnapshotInfo that = (SnapshotInfo) o;
        return dbId == that.dbId &&
                tblId == that.tblId &&
                partitionId == that.partitionId &&
                indexId == that.indexId &&
                tabletId == that.tabletId &&
                beId == that.beId &&
                schemaHash == that.schemaHash &&
                path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tabletId, beId);
    }

    public SnapshotInfoPB toProto() {
        SnapshotInfoCorePB.Builder builder = SnapshotInfoCorePB.newBuilder();
        builder.setDbId(dbId);
        builder.setTblId(tblId);
        builder.setPartitionId(partitionId);
        builder.setIndexId(indexId);
        builder.setTabletId(tabletId);
        builder.setBeId(beId);
        builder.setSchemaHash(schemaHash);
        if (path != null) {
            builder.setPath(path);
        }
        if (files != null) {
            builder.setFiles(StringListPB.newBuilder().addAllValues(files).build());
        }
        SnapshotInfoPB.Builder parent = SnapshotInfoPB.newBuilder().setCore(builder);
        fillProtoVariant(parent);
        return parent.build();
    }

    protected void fillProtoVariant(SnapshotInfoPB.Builder builder) {
        builder.setKind(SnapshotInfoPB.Kind.SNAPSHOT_INFO)
                .setSnapshotInfo(SnapshotInfoPB.SnapshotPB.getDefaultInstance());
    }

    public static SnapshotInfo fromProto(SnapshotInfoPB parent) {
        ProtoKinds.checkKnown(parent.getKind(), parent.getKindValue());
        SnapshotInfo value;
        switch (parent.getKind()) {
            case SNAPSHOT_INFO:
                if (!parent.hasSnapshotInfo()) {
                    throw new IllegalArgumentException("missing snapshot variant");
                }
                value = new SnapshotInfo();
                break;
            case LAKE_TABLE_SNAPSHOT_INFO:
                if (!parent.hasLakeTableSnapshotInfo()) {
                    throw new IllegalArgumentException("missing lake snapshot variant");
                }
                value = new LakeTableSnapshotInfo(0, 0, 0, 0, 0, 0, 0,
                        parent.getLakeTableSnapshotInfo().getVersion());
                break;
            default:
                throw new IllegalArgumentException("unknown snapshot kind " + parent.getKind());
        }
        SnapshotInfoCorePB pb = parent.getCore();
        if (pb.hasDbId()) {
            value.dbId = pb.getDbId();
        }
        if (pb.hasTblId()) {
            value.tblId = pb.getTblId();
        }
        if (pb.hasPartitionId()) {
            value.partitionId = pb.getPartitionId();
        }
        if (pb.hasIndexId()) {
            value.indexId = pb.getIndexId();
        }
        if (pb.hasTabletId()) {
            value.tabletId = pb.getTabletId();
        }
        if (pb.hasBeId()) {
            value.beId = pb.getBeId();
        }
        if (pb.hasSchemaHash()) {
            value.schemaHash = pb.getSchemaHash();
        }
        if (pb.hasPath()) {
            value.path = pb.getPath();
        } else {
            value.path = null;
        }
        if (pb.hasFiles()) {
            value.files = new ArrayList<>(pb.getFiles().getValuesList());
        } else {
            value.files = null;
        }
        return value;
    }
}
