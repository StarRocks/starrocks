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

import com.google.common.collect.ImmutableList;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

public class IcebergPartitionsTableScanner extends AbstractIcebergMetadataScanner {
    protected static final List<String> SCAN_COLUMNS =
            ImmutableList.of("content", "partition", "file_size_in_bytes", "record_count");

    private final String manifestBean;
    private ManifestFile manifestFile;
    private CloseableIterator<? extends ContentFile<?>> reader;
    private Long lastUpdateTime;
    private Integer spedId;
    private IcebergPartitionValueReader partitionValueReader;

    public IcebergPartitionsTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
        this.manifestBean = params.get("split_info");
    }

    @Override
    public void doOpen() {
        this.manifestFile = deserializeFromBase64(manifestBean);
        this.spedId = manifestFile.partitionSpecId();
        this.lastUpdateTime = table.snapshot(manifestFile.snapshotId()) != null ?
                table.snapshot(manifestFile.snapshotId()).timestampMillis() : null;
        this.partitionValueReader = new IcebergPartitionValueReader(table);
    }

    @Override
    public int doGetNext() {
        int numRows = 0;
        for (; numRows < getTableSize(); numRows++) {
            if (!reader.hasNext()) {
                break;
            }
            ContentFile<?> file = reader.next();
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = get(requiredFields[i], file);
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
        partitionValueReader = null;
    }

    @Override
    protected void initReader() {
        Map<Integer, PartitionSpec> specs = table.specs();
        if (manifestFile.content() == ManifestContent.DATA) {
            reader = ManifestFiles.read(manifestFile, fileIO, specs)
                    .select(SCAN_COLUMNS)
                    .caseSensitive(false)
                    .iterator();
        } else {
            reader = ManifestFiles.readDeleteManifest(manifestFile, fileIO, specs)
                    .select(SCAN_COLUMNS)
                    .caseSensitive(false)
                    .iterator();
        }
    }

    private Object get(String columnName, ContentFile<?> file) {
        FileContent content = file.content();
        switch (columnName) {
            case "partition_value":
                return partitionValueReader.read((PartitionData) file.partition());
            case "spec_id":
                return spedId;
            case "record_count":
                return content == FileContent.DATA ? file.recordCount() : 0;
            case "file_count":
                return content == FileContent.DATA ? 1L : 0L;
            case "total_data_file_size_in_bytes":
                return content == FileContent.DATA ? file.fileSizeInBytes() : 0;
            case "position_delete_record_count":
                return content == FileContent.POSITION_DELETES ? file.recordCount() : 0;
            case "position_delete_file_count":
                return content == FileContent.POSITION_DELETES ? 1L : 0L;
            case "equality_delete_record_count":
                return content == FileContent.EQUALITY_DELETES ? file.recordCount() : 0;
            case "equality_delete_file_count":
                return content == FileContent.EQUALITY_DELETES ? 1L : 0L;
            case "last_updated_at":
                return lastUpdateTime;
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }
}
