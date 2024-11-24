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

import com.starrocks.jni.connector.ColumnValue;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IcebergManifestsTableScanner extends AbstractIcebergMetadataScanner {
    private Iterator<ManifestFile> reader;
    private Map<Integer, PartitionSpec> partitionSpecsById;

    public IcebergManifestsTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
    }

    @Override
    public void doOpen() {
        partitionSpecsById = table.specs();
    }

    @Override
    public int doGetNext() {
        int numRows = 0;
        for (; numRows < getTableSize(); numRows++) {
            if (reader == null || !reader.hasNext()) {
                break;
            }

            ManifestFile manifestFile = reader.next();
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = get(requiredFields[i], manifestFile);
                if (fieldData == null) {
                    appendData(i, null);
                } else {
                    ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData, timezone);
                    appendData(i, fieldValue);
                }
            }
        }
        return numRows;
    }

    @Override
    public void doClose() {
        reader = null;
    }

    @Override
    protected void initReader() {
        if (table.currentSnapshot() != null) {
            reader = table.currentSnapshot().allManifests(table.io()).iterator();
        }
    }

    private Object get(String columnName, ManifestFile file) {
        switch (columnName) {
            case "path":
                return file.path();
            case "length":
                return file.length();
            case "partition_spec_id":
                return file.partitionSpecId();
            case "added_snapshot_id":
                return file.snapshotId();
            case "added_data_files_count":
                return file.addedFilesCount();
            case "added_rows_count":
                return file.addedRowsCount();
            case "existing_data_files_count":
                return file.existingFilesCount();
            case "existing_rows_count":
                return file.existingRowsCount();
            case "deleted_data_files_count":
                return file.deletedFilesCount();
            case "deleted_rows_count":
                return file.deletedRowsCount();
            case "partitions":
                return buildPartitionSummaries(file.partitions(), partitionSpecsById.get(file.partitionSpecId()));
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

    private Object buildPartitionSummaries(List<ManifestFile.PartitionFieldSummary> summaries, PartitionSpec partitionSpec) {
        List<GenericRecord> results = new ArrayList<>();
        for (int i = 0; i < summaries.size(); i++) {
            ManifestFile.PartitionFieldSummary summary = summaries.get(i);
            PartitionField field = partitionSpec.fields().get(i);
            Type nestedType = partitionSpec.partitionType().fields().get(i).type();
            Types.StructType summarySchema = ManifestFile.PARTITION_SUMMARY_TYPE;
            GenericRecord record = GenericRecord.create(summarySchema);

            record.setField("contains_null", summary.containsNull() ? "true" : "false");
            record.setField("contains_nan", summary.containsNaN() ? "true" : "false");
            record.setField("lower_bound", field.transform().toHumanString(
                    nestedType, Conversions.fromByteBuffer(nestedType, summary.lowerBound())));
            record.setField("upper_bound", field.transform().toHumanString(
                    nestedType, Conversions.fromByteBuffer(nestedType, summary.upperBound())));
            results.add(record);
        }
        return results;
    }
}
