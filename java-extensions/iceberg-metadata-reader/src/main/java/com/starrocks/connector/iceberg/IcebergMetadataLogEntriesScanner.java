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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.mapWithIndex;
import static org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance;

public class IcebergMetadataLogEntriesScanner extends AbstractIcebergMetadataScanner {

    private CloseableIterator<StructLike> reader;
    Map<String, Integer> columnNameToPosition = new HashMap<>();

    public IcebergMetadataLogEntriesScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
    }

    @Override
    public void doOpen() {
    }

    @Override
    public int doGetNext() {
        int numRows = 0;
        for (; numRows < getTableSize(); numRows++) {
            if (reader == null) {
                break;
            }
            if (!reader.hasNext()) {
                break;
            }

            StructLike dataRow = reader.next();
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = get(requiredFields[i], dataRow);
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
    public void doClose() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected void initReader() throws IOException {
        TableScan tableScan = createMetadataTableInstance(table, MetadataTableType.METADATA_LOG_ENTRIES).newScan();

        this.columnNameToPosition = mapWithIndex(tableScan.schema().columns().stream(),
                (column, position) -> immutableEntry(column.name(), Long.valueOf(position).intValue()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        for (String requiredField : requiredFields) {
            if (!columnNameToPosition.containsKey(requiredField)) {
                throw new IOException("can not find column name in the metadata table" + requiredField);
            }
        }

        try (CloseableIterator<FileScanTask> iterator = tableScan.planFiles().iterator()) {
            // only one scan task for metadata log entries scan
            if (iterator.hasNext()) {
                this.reader = iterator.next().asDataTask().rows().iterator();
            }
        }
    }

    private Object get(String columnName, StructLike dataRow) {
        switch (columnName) {
            case "timestamp":
                return dataRow.get(columnNameToPosition.get(columnName), Long.class) / 1000;
            case "file":
                return dataRow.get(columnNameToPosition.get(columnName), String.class);
            case "latest_snapshot_id":
                return dataRow.get(columnNameToPosition.get(columnName), Long.class);
            case "latest_schema_id":
                return dataRow.get(columnNameToPosition.get(columnName), Integer.class);
            case "latest_sequence_number":
                return dataRow.get(columnNameToPosition.get(columnName), Long.class);
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }
}
