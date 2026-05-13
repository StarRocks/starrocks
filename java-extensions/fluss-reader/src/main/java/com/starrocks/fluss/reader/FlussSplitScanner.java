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

package com.starrocks.fluss.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.lake.paimon.source.PaimonLakeSource;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FlussSplitScanner extends ConnectorScanner {
    private static final Logger LOG = LogManager.getLogger(FlussSplitScanner.class);
    private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

    private final String splitInfo;
    private final String tableConf;
    private final String dbName;
    private final String tableName;
    private final String timeZone;
    private final String[] requiredFields;
    private final int fetchSize;
    private final ClassLoader classLoader;

    private Connection connection;
    private Table table;
    private ColumnType[] requiredTypes;
    private DataType[] logicalTypes;
    private int[] projectedFields;
    private InternalRow.FieldGetter[] fieldGetters;
    private ConnectorScannerProxy delegateScanner;

    public FlussSplitScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.requiredFields = ScannerHelper.splitAndOmitEmptyStrings(params.get("required_fields"), ",");
        this.splitInfo = params.get("split_info");
        this.tableConf = params.get("table_conf");
        this.dbName = params.get("db_name");
        this.tableName = params.get("table_name");
        this.timeZone = params.get("time_zone");
        this.classLoader = this.getClass().getClassLoader();
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Configuration conf = decodeStringToObject(tableConf);
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TablePath.of(dbName, tableName));

            RowType rowType = table.getTableInfo().getRowType();
            List<String> fieldNames = rowType.getFields().stream()
                    .map(DataField::getName).collect(Collectors.toList());

            requiredTypes = new ColumnType[requiredFields.length];
            logicalTypes = new DataType[requiredFields.length];
            projectedFields = new int[requiredFields.length];
            fieldGetters = new InternalRow.FieldGetter[requiredFields.length];

            for (int i = 0; i < requiredFields.length; i++) {
                int index = fieldNames.indexOf(requiredFields[i]);
                if (index == -1) {
                    throw new RuntimeException(String.format(
                            "Cannot find field %s in schema %s", requiredFields[i], fieldNames));
                }
                projectedFields[i] = index;
                DataType dataType = rowType.getTypeAt(index);
                logicalTypes[i] = dataType;
                requiredTypes[i] = new ColumnType(FlussTypeUtils.fromFlussType(dataType));
                fieldGetters[i] = InternalRow.createFieldGetter(dataType, i);
            }

            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);

            String dataLakePrefix = "table.datalake.paimon.";
            Map<String, String> paimonProps = new HashMap<>();
            for (Map.Entry<String, String> entry : conf.toMap().entrySet()) {
                if (entry.getKey().startsWith(dataLakePrefix)) {
                    paimonProps.put(entry.getKey().substring(dataLakePrefix.length()), entry.getValue());
                }
            }
            Configuration paimonConfig = Configuration.fromMap(paimonProps);
            @SuppressWarnings("unchecked")
            LakeSource<LakeSplit> lakeSource = (LakeSource<LakeSplit>) (LakeSource<?>)
                    new PaimonLakeSource(paimonConfig, TablePath.of(dbName, tableName));
            SourceSplitSerializer serializer = new SourceSplitSerializer(lakeSource);
            byte[] splitBytes = BASE64_DECODER.decode(splitInfo.getBytes(UTF_8));
            SourceSplitBase split = serializer.deserialize(0, splitBytes);

            if (split.isLogSplit()) {
                delegateScanner = new FlussLogScanner(table, split.asLogSplit(), projectedFields);
            } else if (split.isLakeSplit()) {
                if (split instanceof LakeSnapshotSplit) {
                    delegateScanner = new FlussSnapshotScanner(lakeSource, (LakeSnapshotSplit) split);
                } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
                    delegateScanner = new FlussSnapshotAndLogScanner(
                            table, lakeSource, (LakeSnapshotAndFlussLogSplit) split, projectedFields);
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported lake split type: " + split.getClass().getName());
                }
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported split type: " + split.getClass().getName());
            }

            delegateScanner.open();
        } catch (Exception e) {
            close();
            String msg = "Failed to open the fluss reader for " + dbName + "." + tableName;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (delegateScanner != null) {
                delegateScanner.close();
            }
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            String msg = "Failed to close the fluss reader for " + dbName + "." + tableName;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            int numRows = 0;
            while (delegateScanner.hasNext() && numRows < fetchSize) {
                InternalRow row = delegateScanner.getNext();
                if (row == null) {
                    break;
                }
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = fieldGetters[i].getFieldOrNull(row);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new FlussColumnValue(fieldData, logicalTypes[i], timeZone);
                        appendData(i, fieldValue);
                    }
                }
                numRows++;
            }
            return numRows;
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk for " + dbName + "." + tableName;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T decodeStringToObject(String encodedStr) {
        byte[] bytes = BASE64_DECODER.decode(encodedStr.getBytes(UTF_8));
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to decode object from string", e);
        }
    }

    @Override
    public String toString() {
        return "FlussSplitScanner{" +
                "dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", requiredFields=" + Arrays.toString(requiredFields) +
                ", fetchSize=" + fetchSize +
                '}';
    }
}
