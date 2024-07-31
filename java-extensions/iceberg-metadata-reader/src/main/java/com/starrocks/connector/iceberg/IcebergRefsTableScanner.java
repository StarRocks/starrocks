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
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.iceberg.SnapshotRef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class IcebergRefsTableScanner extends AbstractIcebergMetadataScanner {
    private static final Logger LOG = LogManager.getLogger(IcebergRefsTableScanner.class);
    private Iterator<Map.Entry<String, SnapshotRef>> reader;

    public IcebergRefsTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
    }

    @Override
    public void doOpen() {

    }

    @Override
    public void close() {
        if (reader != null) {
            reader = null;
        }
    }

    @Override
    protected void initReader() {
        reader = table.refs().entrySet().iterator();
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            int numRows = 0;
            for (; numRows < getTableSize(); numRows++) {
                if (!reader.hasNext()) {
                    break;
                }
                Map.Entry<String, SnapshotRef> refEntry = reader.next();
                String refName = refEntry.getKey();
                SnapshotRef ref = refEntry.getValue();
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = get(requiredFields[i], refName, ref);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData);
                        appendData(i, fieldValue);
                    }
                }
            }
            return numRows;
        } catch (Exception e) {
            close();
            LOG.error("Failed to get the next off-heap table chunk of iceberg refs table.", e);
            throw new IOException("Failed to get the next off-heap table chunk of iceberg refs table.", e);
        }
    }

    private Object get(String columnName, String refName, SnapshotRef ref) {
        switch (columnName) {
            case "name":
                return refName;
            case "type":
                return ref.isBranch() ? "BRANCH" : "TAG";
            case "snapshot_id":
                return ref.snapshotId();
            case "max_reference_age_in_ms":
                return ref.maxRefAgeMs();
            case "min_snapshots_to_keep":
                return ref.minSnapshotsToKeep();
            case "max_snapshot_age_in_ms":
                return ref.maxSnapshotAgeMs();
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }
}
