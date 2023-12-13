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

package com.starrocks.paimon.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.SelectedFields;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PaimonSplitScanner extends ConnectorScanner {

    private static final Logger LOG = LogManager.getLogger(PaimonSplitScanner.class);
    private final String splitInfo;
    private final String predicateInfo;
    private final String[] requiredFields;
    private final String encodedTable;
    private ColumnType[] requiredTypes;
    private DataType[] logicalTypes;
    private Table table;
    private RecordReaderIterator<InternalRow> iterator;
    private final int fetchSize;
    private final ClassLoader classLoader;
    private final String[] nestedFields;

    public PaimonSplitScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.requiredFields = params.get("required_fields").split(",");
        this.nestedFields = params.getOrDefault("nested_fields", "").split(",");
        this.splitInfo = params.get("split_info");
        this.predicateInfo = params.get("predicate_info");
        this.encodedTable = params.get("native_table");
        this.classLoader = this.getClass().getClassLoader();
    }

    private void parseRequiredTypes() {
        List<String> fieldNames = PaimonScannerUtils.fieldNames(table.rowType());
        requiredTypes = new ColumnType[requiredFields.length];
        logicalTypes = new DataType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            int index = fieldNames.indexOf(requiredFields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        requiredFields[i], fieldNames));
            }
            DataType dataType = table.rowType().getTypeAt(index);
            String type = PaimonTypeUtils.fromPaimonType(dataType);
            requiredTypes[i] = new ColumnType(type);
            logicalTypes[i] = dataType;
        }

        // prune fields
        SelectedFields ssf = new SelectedFields();
        for (String nestField : nestedFields) {
            ssf.addNestedPath(nestField);
        }
        for (int i = 0; i < requiredFields.length; i++) {
            ColumnType type = requiredTypes[i];
            String name = requiredFields[i];
            type.pruneOnField(ssf, name);
        }
    }

    private void initReader() throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();
        RowType rowType = table.rowType();
        List<String> fieldNames = PaimonScannerUtils.fieldNames(rowType);
        int[] projected = Arrays.stream(requiredFields).mapToInt(fieldNames::indexOf).toArray();
        readBuilder.withProjection(projected);
        List<Predicate> predicates = PaimonScannerUtils.decodeStringToObject(predicateInfo);
        readBuilder.withFilter(predicates);
        Split split = PaimonScannerUtils.decodeStringToObject(splitInfo);
        RecordReader<InternalRow> reader = readBuilder.newRead().executeFilter().createReader(split);
        iterator = new RecordReaderIterator<>(reader);
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            table = PaimonScannerUtils.decodeStringToObject(encodedTable);
            parseRequiredTypes();
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
            initReader();
        } catch (Exception e) {
            close();
            String msg = "Failed to open the paimon reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (iterator != null) {
                iterator.close();
            }
        } catch (Exception e) {
            String msg = "Failed to close the paimon reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            int numRows = 0;
            while (iterator.hasNext() && numRows < fetchSize) {
                InternalRow row = iterator.next();
                if (row == null) {
                    break;
                }
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = InternalRowUtils.get(row, i, logicalTypes[i]);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new PaimonColumnValue(fieldData, logicalTypes[i]);
                        appendData(i, fieldValue);
                    }
                }
                numRows++;
            }
            return numRows;
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk of paimon.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }

    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("splitInfo: ");
        sb.append(splitInfo);
        sb.append("\n");
        sb.append("requiredFields: ");
        sb.append(Arrays.toString(requiredFields));
        sb.append("\n");
        sb.append("fetchSize: ");
        sb.append(fetchSize);
        sb.append("\n");
        return sb.toString();
    }
}
