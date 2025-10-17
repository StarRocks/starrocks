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
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.lake.paimon.source.PaimonLakeSource;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FlussSplitScanner extends ConnectorScanner  {
    private static final Logger LOG = LogManager.getLogger(FlussSplitScanner.class);
    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(500);

    private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

    private final String[] requiredFields;
    private final String timeZone;
    private final String dbName;
    private final String tableName;
    private final String tableConf;
    private final String splitInfo;
    private final int fetchSize;
    private final ClassLoader classLoader;

    private Table flussTable;
    private ConnectorScannerProxy delegate;
    private ColumnType[] requiredTypes;
    private DataType[] logicalTypes;
    private InternalRow.FieldGetter[] flussFieldGetters;
    private int[] projectedFields;

    public FlussSplitScanner(int fetchSize, Map<String, String> params) {
        this.requiredFields = ScannerHelper.splitAndOmitEmptyStrings(params.get("required_fields"), ",");
        this.splitInfo = params.get("split_info");
        this.dbName = params.get("db_name");
        this.tableName = params.get("table_name");
        this.tableConf = params.get("table_conf");
        this.timeZone = params.get("time_zone");
        this.fetchSize = fetchSize;
        this.classLoader = this.getClass().getClassLoader();
    }

    public void initProxy() {
        Configuration tableConfig = ScannerHelper.decodeStringToObject(tableConf);
        Connection connection = ConnectionFactory.createConnection(tableConfig);
        this.flussTable = connection.getTable(new TablePath(dbName, tableName));

        SourceSplitBase split = decodeFlussSplit(splitInfo);
        if (split instanceof LakeSnapshotAndFlussLogSplit) {
            this.delegate = new FlussSnapshotAndLogScanner(fetchSize, requiredFields,
                    split, flussTable, tableConfig, timeZone);
        } else if (split instanceof LogSplit) {
            this.delegate = new FlussLogScanner(fetchSize, requiredFields, split, flussTable, timeZone);
        } else if (split instanceof LakeSnapshotSplit) {
            this.delegate = new FlussSnapshotScanner(fetchSize, requiredFields, split, flussTable, tableConfig, timeZone);
        } else {
            LOG.error("Unknown split type: {}", split.getClass().getName());
        }
    }

    public void initTypes() {
        this.projectedFields = new int[requiredFields.length];
        this.flussFieldGetters = new InternalRow.FieldGetter[requiredFields.length];

        List<String> fieldNames = flussTable.getTableInfo().getRowType().getFieldNames();
        this.requiredTypes = new ColumnType[requiredFields.length];
        this.logicalTypes = new DataType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            int index = fieldNames.indexOf(requiredFields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s of table %s",
                        this.requiredFields[i], fieldNames, flussTable.getTableInfo().getTablePath()));
            }
            projectedFields[i] = index;
            DataType dataType = flussTable.getTableInfo().getRowType().getTypeAt(index);
            String type = FlussTypeUtils.fromFlussType(dataType);
            this.requiredTypes[i] = new ColumnType(type);
            logicalTypes[i] = dataType;
            flussFieldGetters[i] = InternalRow.createFieldGetter(flussTable.getTableInfo().getRowType().getTypeAt(index), i);
        }
    }

    public ColumnType[] getRequiredTypes() {
        return requiredTypes;
    }

    public DataType[] getLogicalTypes() {
        return logicalTypes;
    }

    public InternalRow.FieldGetter[] getFlussFieldGetters() {
        return flussFieldGetters;
    }

    public int[] getProjectedFields() {
        return projectedFields;
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            initProxy();
            initTypes();
            delegate.openProxy(this);
        } catch (Exception e) {
            close();
            String msg = "Failed to open the fluss reader for table " +
                    this.flussTable.getTableInfo().getTablePath();
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getNextProxy(this);
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk for table " +
                    this.flussTable.getTableInfo().getTablePath();
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.closeProxy(this);
        } catch (Exception e) {
            String msg = "Failed to close the fluss reader for table " +
                    this.flussTable.getTableInfo().getTablePath();
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    public SourceSplitBase decodeFlussSplit(String encodedStr) {
        try {
            byte[] bytes = BASE64_DECODER.decode(encodedStr.getBytes(UTF_8));
            LakeSource lakeSource = new PaimonLakeSource(null, null);
            SourceSplitSerializer serializer = new SourceSplitSerializer(lakeSource);
            return serializer.deserialize(serializer.getVersion(), bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
