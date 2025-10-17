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

import com.starrocks.jni.connector.ColumnValue;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.reader.LakeSnapshotAndLogSplitScanner;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.fluss.reader.FlussSplitScanner.DEFAULT_POLL_TIMEOUT;
import static org.apache.fluss.flink.utils.LakeSourceUtils.createLakeSource;

public class FlussSnapshotAndLogScanner extends ConnectorScannerProxy {
    private static final Logger LOG = LogManager.getLogger(FlussSnapshotAndLogScanner.class);

    private final int fetchSize;
    private final String[] requiredFields;
    private LakeSnapshotAndFlussLogSplit snapshotAndFlussLogSplit;
    private final Table flussTable;
    private final Configuration tableConfig;
    private final String timeZone;

    private LakeSnapshotAndLogSplitScanner scanner;
    private CloseableIterator<InternalRow> iterator;

    public FlussSnapshotAndLogScanner(int fetchSize, String[] requiredFields, SourceSplitBase split,
                                      Table flussTable, Configuration tableConfig, String timeZone) {
        this.fetchSize = fetchSize;
        this.requiredFields = requiredFields;
        this.snapshotAndFlussLogSplit = (LakeSnapshotAndFlussLogSplit) split;
        this.flussTable = flussTable;
        this.tableConfig = tableConfig;
        this.timeZone = timeZone;
    }

    public void openProxy(FlussSplitScanner parent) {
        Map<String, String> properties = new HashMap<>(flussTable.getTableInfo().getProperties().toMap());
        properties.putAll(this.tableConfig.toMap());
        this.scanner = new LakeSnapshotAndLogSplitScanner(flussTable, createLakeSource(flussTable.getTableInfo().getTablePath(),
                properties), this.snapshotAndFlussLogSplit, parent.getProjectedFields());
        parent.initOffHeapTableWriter(parent.getRequiredTypes(), requiredFields, fetchSize);
    }

    public int getNextProxy(FlussSplitScanner parent) throws IOException {
        if (this.iterator == null) {
            while (true) {
                this.iterator = this.scanner.pollBatch(DEFAULT_POLL_TIMEOUT);
                if (iterator == null) {
                    parent.close();
                    return 0;
                } else {
                    break;
                }
            }
        }

        int numRows = 0;
        while (iterator.hasNext() && numRows < fetchSize) {
            InternalRow row = iterator.next();
            if (row == null) {
                break;
            }
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = parent.getFlussFieldGetters()[i].getFieldOrNull(row);
                if (fieldData == null) {
                    parent.appendData(i, null);
                } else {
                    ColumnValue fieldValue = new FlussColumnValue(fieldData, parent.getLogicalTypes()[i], timeZone);
                    parent.appendData(i, fieldValue);
                }
            }
            numRows++;
        }
        return numRows;
    }

    public void closeProxy(FlussSplitScanner parent) throws IOException {
        if (this.scanner != null) {
            this.scanner.close();
        }
        try {
            this.flussTable.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
