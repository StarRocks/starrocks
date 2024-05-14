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

package com.starrocks.kudu.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScannerIterator;
import org.apache.kudu.client.RowResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KuduSplitScanner extends ConnectorScanner {

    private static final Logger LOG = LogManager.getLogger(KuduSplitScanner.class);
    private static final ConcurrentHashMap<String, KuduClient> KUDU_CLIENTS = new ConcurrentHashMap<>();
    private final String encodedToken;
    private final String master;
    private KuduScanner scanner;
    private final String[] requiredFields;
    private ColumnType[] requiredTypes;
    private KuduScannerIterator iterator;
    private final int fetchSize;
    private final ClassLoader classLoader;

    public KuduSplitScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.requiredFields = ScannerHelper.splitAndOmitEmptyStrings(params.get("required_fields"), ",");
        this.encodedToken = params.get("kudu_scan_token");
        this.master = params.get("kudu_master");
        this.classLoader = this.getClass().getClassLoader();
    }

    private void parseRequiredTypes() {
        Schema schema = scanner.getProjectionSchema();
        requiredTypes = new ColumnType[requiredFields.length];
        Type[] logicalTypes = new Type[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            int index = schema.getColumnIndex(requiredFields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        requiredFields[i], schema));
            }
            ColumnSchema columnSchema = schema.getColumnByIndex(index);
            Type type = columnSchema.getType();
            String columnType = KuduTypeUtils.fromKuduType(columnSchema);
            requiredTypes[i] = new ColumnType(columnType);
            logicalTypes[i] = type;
        }
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            LOG.info("Open kudu reader with master: {}, token: {}", master, encodedToken);
            KuduClient client = KUDU_CLIENTS.computeIfAbsent(master, m -> new KuduClient.KuduClientBuilder(m).build());
            byte[] token = KuduScannerUtils.decodeBase64(encodedToken);
            scanner = KuduScanToken.deserializeIntoScanner(token, client);
            LOG.info("Open kudu scanner with projection schema: {}", scanner.getProjectionSchema());
            iterator = scanner.iterator();
            parseRequiredTypes();
            LOG.info("init off-heap table writer with requiredFields: {}, requiredTypes: {}, fetchSize: {}",
                    Arrays.toString(requiredFields), Arrays.toString(requiredTypes), fetchSize);
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
        } catch (Exception e) {
            close();
            String msg = "Failed to open the kudu reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (scanner != null) {
                scanner.close();
            }
        } catch (Exception e) {
            String msg = "Failed to close the kudu scanner.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            int numRows = 0;
            while (iterator.hasNext() && numRows < fetchSize) {
                RowResult row = iterator.next();
                if (row == null) {
                    break;
                }
                for (int i = 0; i < requiredFields.length; i++) {
                    if (row.isNull(i)) {
                        appendData(i, null);
                    } else {
                        KuduColumnValue fieldValue = new KuduColumnValue(row, i);
                        appendData(i, fieldValue);
                    }
                }
                numRows++;
            }
            return numRows;
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk of kudu.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    public String toString() {
        return "scanner: "
                + scanner
                + "\n"
                + "requiredFields: "
                + Arrays.toString(requiredFields)
                + "\n"
                + "fetchSize: "
                + fetchSize
                + "\n";
    }
}
