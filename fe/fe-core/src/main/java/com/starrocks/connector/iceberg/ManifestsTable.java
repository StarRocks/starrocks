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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.ExternalMetadataTable;
import com.starrocks.thrift.TGetIcebergManifestsResponse;
import com.starrocks.thrift.TIcebergManifest;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ManifestsTable extends SystemTable implements ExternalMetadataTable {
    private final IcebergTable icebergTable;
    private static final List<Column> METADATA_SCHEMAS = Lists.newArrayList(
            new Column("path", ScalarType.VARCHAR),
            new Column("length", ScalarType.BIGINT),
            new Column("partition_spec_id", ScalarType.INT),
            new Column("added_snapshot_id", ScalarType.BIGINT),
            new Column("added_data_files_count", ScalarType.INT),
            new Column("added_rows_count", ScalarType.BIGINT),
            new Column("existing_data_files_count", ScalarType.INT),
            new Column("existing_rows_count", ScalarType.BIGINT),
            new Column("deleted_data_files_count", ScalarType.INT),
            new Column("deleted_rows_count", ScalarType.BIGINT),
            new Column("partition_summaries", ScalarType.VARCHAR, true)
    );

    public ManifestsTable(String catalogName, IcebergTable icebergTable) {
        super(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                icebergTable.getRemoteTableName(),
                TableType.SCHEMA,
                METADATA_SCHEMAS,
                TSchemaTableType.SCH_ICEBERG_MANIFESTS);
        this.icebergTable = requireNonNull(icebergTable, "iceberg table is null");
    }

    public String getOriginDbName() {
        return icebergTable.getRemoteDbName();
    }

    public String getOriginTableName() {
        return icebergTable.getRemoteTableName();
    }

    public TGetIcebergManifestsResponse getManifests() {
        TGetIcebergManifestsResponse res = new TGetIcebergManifestsResponse();
        List<TIcebergManifest> tIcebergManifests = new ArrayList<>();
        res.setIceberg_manifests(tIcebergManifests);
        if (!icebergTable.getSnapshot().isPresent()) {
            return res;
        }

        List<ManifestFile> manifestFiles = icebergTable.getSnapshot().get().allManifests(icebergTable.getNativeTable().io());
        for (ManifestFile manifest : manifestFiles) {
            TIcebergManifest tIcebergManifest = new TIcebergManifest();
            tIcebergManifest.setPath(manifest.path());
            tIcebergManifest.setLength(manifest.length());
            tIcebergManifest.setAdded_snapshot_id(manifest.snapshotId());
            tIcebergManifest.setAdded_data_files_count(manifest.addedFilesCount());
            tIcebergManifest.setAdded_rows_count(manifest.addedRowsCount());
            tIcebergManifest.setExisting_data_files_count(manifest.existingFilesCount());
            tIcebergManifest.setExisting_rows_count(manifest.existingRowsCount());
            tIcebergManifest.setDeleted_data_files_count(manifest.deletedFilesCount());
            tIcebergManifest.setDeleted_rows_count(manifest.deletedRowsCount());
            tIcebergManifest.setPartition_summaries(getPartitionSummaries(manifest, icebergTable.getNativeTable().spec()));
            tIcebergManifests.add(tIcebergManifest);
        }
        return res;
    }

    private String getPartitionSummaries(ManifestFile manifestFile, PartitionSpec partitionSpec) {
        JsonArray res = new JsonArray();
        List<ManifestFile.PartitionFieldSummary> summaries = manifestFile.partitions();
        for (int i = 0; i < summaries.size(); i++) {
            ManifestFile.PartitionFieldSummary summary = summaries.get(i);
            JsonObject jsonObject = new JsonObject();
            PartitionField field = partitionSpec.fields().get(i);
            Type nestedType = partitionSpec.partitionType().fields().get(i).type();
            jsonObject.addProperty("contains_null", summary.containsNull());
            jsonObject.addProperty("contains_nan", summary.containsNaN());
            if (summary.lowerBound() != null) {
                jsonObject.addProperty("lower_bound", field.transform().toHumanString(
                        nestedType, Conversions.fromByteBuffer(nestedType, summary.lowerBound())));
            }
            if (summary.upperBound() != null) {
                jsonObject.addProperty("upper_bound", field.transform().toHumanString(
                        nestedType, Conversions.fromByteBuffer(nestedType, summary.upperBound())));
            }

            res.add(jsonObject);
        }
        return res.toString();
    }
}
