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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.ExternalMetadataTable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TGetIcebergSnapshotsResponse;
import com.starrocks.thrift.TIcebergSnapshot;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.iceberg.Snapshot;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SnapshotsTable extends SystemTable implements ExternalMetadataTable {
    private final IcebergTable icebergTable;
    private static final List<Column> METADATA_SCHEMAS = Lists.newArrayList(
            new Column("committed_at", ScalarType.VARCHAR),
            new Column("snapshot_id", ScalarType.BIGINT),
            new Column("parent_id", ScalarType.BIGINT, true),
            new Column("operation", ScalarType.VARCHAR, true),
            new Column("manifest_list", ScalarType.VARCHAR, true),
            new Column("summary", ScalarType.VARCHAR, true)
    );

    public SnapshotsTable(String catalogName, IcebergTable icebergTable) {
        super(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                icebergTable.getRemoteTableName(),
                TableType.SCHEMA,
                METADATA_SCHEMAS,
                TSchemaTableType.SCH_ICEBERG_SNAPSHOTS);
        this.icebergTable = requireNonNull(icebergTable, "iceberg table is null");
    }

    public String getOriginDbName() {
        return icebergTable.getRemoteDbName();
    }

    public String getOriginTableName() {
        return icebergTable.getRemoteTableName();
    }

    public TGetIcebergSnapshotsResponse getSnapshots() {
        TGetIcebergSnapshotsResponse res = new TGetIcebergSnapshotsResponse();
        List<TIcebergSnapshot> tIcebergSnapshots = new ArrayList<>();
        for (Snapshot snapshot : icebergTable.getNativeTable().snapshots()) {
            TIcebergSnapshot tIcebergSnapshot = new TIcebergSnapshot();
            tIcebergSnapshot.setCommitted_at(TimeUtils.longToTimeString(snapshot.timestampMillis()));
            tIcebergSnapshot.setSnapshot_id(snapshot.snapshotId());
            if (snapshot.parentId() != null) {
                tIcebergSnapshot.setParent_id(snapshot.parentId());
            }
            if (snapshot.operation() != null) {
                tIcebergSnapshot.setOperation(snapshot.operation());
            }
            if (snapshot.manifestListLocation() != null) {
                tIcebergSnapshot.setManifest_list(snapshot.manifestListLocation());
            }
            if (snapshot.summary() != null) {
                tIcebergSnapshot.setSummary(GsonUtils.GSON.toJson(snapshot.summary()));
            }
            tIcebergSnapshots.add(tIcebergSnapshot);
        }
        res.setIceberg_snapshots(tIcebergSnapshots);
        return res;
    }

}
