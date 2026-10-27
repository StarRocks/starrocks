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
import org.apache.iceberg.ManifestEntryIterator;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Scans every entry of a manifest, live or DELETED, with the file as the data_file struct. The data_file fields
// are the $files columns, in the order of the FE IcebergEntriesTable.
public class IcebergEntriesTableScanner extends IcebergFilesTableScanner {
    private ManifestEntryIterator reader;
    private List<String> dataFileFieldNames;
    private GenericRecord reusedDataFile;

    public IcebergEntriesTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
    }

    @Override
    public void doOpen() {
        super.doOpen();
        Types.StructType dataFileType = getDataFileType(partitionValueReader.type());
        this.dataFileFieldNames = new ArrayList<>();
        for (Types.NestedField field : dataFileType.fields()) {
            dataFileFieldNames.add(field.name());
        }
        this.reusedDataFile = GenericRecord.create(dataFileType);
    }

    private static Types.StructType getDataFileType(Types.StructType partitionType) {
        List<Types.NestedField> fields = new ArrayList<>();
        int id = 0;
        fields.add(Types.NestedField.optional(++id, "content", Types.IntegerType.get()));
        fields.add(Types.NestedField.optional(++id, "file_path", Types.StringType.get()));
        fields.add(Types.NestedField.optional(++id, "file_format", Types.StringType.get()));
        fields.add(Types.NestedField.optional(++id, "spec_id", Types.IntegerType.get()));
        if (!partitionType.fields().isEmpty()) {
            fields.add(Types.NestedField.optional(++id, "partition", partitionType));
        }
        fields.add(Types.NestedField.optional(++id, "record_count", Types.LongType.get()));
        fields.add(Types.NestedField.optional(++id, "file_size_in_bytes", Types.LongType.get()));
        for (String name : List.of("column_sizes", "value_counts", "null_value_counts", "nan_value_counts")) {
            fields.add(Types.NestedField.optional(++id, name,
                    Types.MapType.ofOptional(++id, ++id, Types.IntegerType.get(), Types.LongType.get())));
        }
        for (String name : List.of("lower_bounds", "upper_bounds")) {
            fields.add(Types.NestedField.optional(++id, name,
                    Types.MapType.ofOptional(++id, ++id, Types.IntegerType.get(), Types.StringType.get())));
        }
        fields.add(Types.NestedField.optional(++id, "key_metadata", Types.BinaryType.get()));
        fields.add(Types.NestedField.optional(++id, "split_offsets",
                Types.ListType.ofOptional(++id, Types.LongType.get())));
        fields.add(Types.NestedField.optional(++id, "equality_ids",
                Types.ListType.ofOptional(++id, Types.IntegerType.get())));
        fields.add(Types.NestedField.optional(++id, "sort_order_id", Types.IntegerType.get()));
        fields.add(Types.NestedField.optional(++id, "first_row_id", Types.LongType.get()));
        fields.add(Types.NestedField.optional(++id, "referenced_data_file", Types.StringType.get()));
        fields.add(Types.NestedField.optional(++id, "content_offset", Types.LongType.get()));
        fields.add(Types.NestedField.optional(++id, "content_size_in_bytes", Types.LongType.get()));
        return Types.StructType.of(fields);
    }

    @Override
    public int doGetNext() {
        int numRows = 0;
        for (; numRows < getTableSize(); numRows++) {
            if (!reader.hasNext()) {
                break;
            }
            reader.next();
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = getEntryField(requiredFields[i]);
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

    private Object getEntryField(String columnName) {
        switch (columnName) {
            case "status":
                return reader.status();
            case "snapshot_id":
                return reader.snapshotId();
            case "sequence_number":
                return reader.dataSequenceNumber();
            case "file_sequence_number":
                return reader.fileSequenceNumber();
            case "data_file":
                for (int i = 0; i < dataFileFieldNames.size(); i++) {
                    String name = dataFileFieldNames.get(i);
                    // $files names the sort order id column sort_id
                    reusedDataFile.set(i, get(name.equals("sort_order_id") ? "sort_id" : name, reader.file()));
                }
                return reusedDataFile;
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

    @Override
    public void doClose() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected void initReader() {
        reader = new ManifestEntryIterator(manifestFile, fileIO, table.specs());
    }
}
