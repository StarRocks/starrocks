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

// language: java
package com.starrocks.planner;

import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDataSink;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;

public class IcebergTableSinkTest {

    @Test
    public void testCompressionFromNativeTableProperty(@Mocked GlobalStateMgr globalStateMgr, @Mocked ConnectorMgr connectorMgr,
                                                       @Mocked CatalogConnector connector, @Mocked IcebergTable icebergTable,
                                                       @Mocked Table nativeTable, @Mocked FileIO fileIO,
                                                       @Mocked SessionVariable sessionVariable) {
        // nativeTable.properties contains parquet compression -> should use it
        Map<String, String> nativeProps = Maps.newHashMap();
        nativeProps.put(PARQUET_COMPRESSION, "zstd");
        nativeProps.put(PARQUET_PAGE_SIZE_BYTES, "65536");
        nativeProps.put(PARQUET_DICT_SIZE_BYTES, "131072");
        nativeProps.put(PARQUET_ROW_GROUP_SIZE_BYTES, "1048576");

        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());

        new Expectations() {
            {
                // iceberg table / native table basic
                icebergTable.getNativeTable();
                result = nativeTable;

                icebergTable.getCatalogName();
                result = "catA";

                icebergTable.getId();
                result = 101L;

                icebergTable.getUUID();
                result = "uuidA";

                nativeTable.location();
                result = "s3://bucket/a";

                nativeTable.properties();
                result = nativeProps;

                nativeTable.io();
                result = fileIO;

                fileIO.properties();
                result = new HashMap<String, String>();

                // Global state and connector metadata fallback path
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getConnectorMgr();
                result = connectorMgr;

                connectorMgr.getConnector("catA");
                result = connector;

                connector.getMetadata().getCloudConfiguration();
                result = cc;

                // session variable should not be used for compression in this case, but stub anyway
                sessionVariable.getConnectorSinkCompressionCodec();
                result = "gzip";

                sessionVariable.getConnectorSinkTargetMaxFileSize();
                result = 0L;
            }
        };

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        IcebergTableSink sink = new IcebergTableSink(icebergTable, desc, false, sessionVariable, null);
        TDataSink t = sink.toThrift();
        // compression_type should map "zstd" -> TCompressionType.ZSTD
        Assertions.assertEquals(TCompressionType.ZSTD, t.getIceberg_table_sink().getCompression_type());
        Assertions.assertEquals(65536, t.getIceberg_table_sink().getParquet_page_size_bytes());
        Assertions.assertEquals(131072, t.getIceberg_table_sink().getParquet_dict_size_bytes());
        Assertions.assertEquals(1048576, t.getIceberg_table_sink().getParquet_row_group_size_bytes());
    }

    @Test
    public void testCompressionFromSessionVariableFallback(@Mocked GlobalStateMgr globalStateMgr,
                                                           @Mocked ConnectorMgr connectorMgr, @Mocked CatalogConnector connector,
                                                           @Mocked IcebergTable icebergTable, @Mocked Table nativeTable,
                                                           @Mocked FileIO fileIO, @Mocked SessionVariable sessionVariable) {
        // nativeTable.properties does NOT contain parquet compression -> should fallback to sessionVariable
        Map<String, String> nativeProps = Maps.newHashMap(); // empty

        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());

        new Expectations() {
            {
                icebergTable.getNativeTable();
                result = nativeTable;

                icebergTable.getCatalogName();
                result = "catB";

                icebergTable.getId();
                result = 102L;

                icebergTable.getUUID();
                result = "uuidB";

                nativeTable.location();
                result = "s3://bucket/b";

                nativeTable.properties();
                result = nativeProps;

                nativeTable.io();
                result = fileIO;

                fileIO.properties();
                result = new HashMap<String, String>();

                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getConnectorMgr();
                result = connectorMgr;

                connectorMgr.getConnector("catB");
                result = connector;

                connector.getMetadata().getCloudConfiguration();
                result = cc;

                // session variable provides fallback compression codec
                sessionVariable.getConnectorSinkCompressionCodec();
                result = "gzip";
                sessionVariable.getConnectorSinkTargetMaxFileSize();
                result = 0L;
            }
        };

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        IcebergTableSink sink = new IcebergTableSink(icebergTable, desc, false, sessionVariable, null);
        TDataSink t = sink.toThrift();
        // fallback "gzip" -> TCompressionType.GZIP
        Assertions.assertEquals(TCompressionType.GZIP, t.getIceberg_table_sink().getCompression_type());
        Assertions.assertFalse(t.getIceberg_table_sink().isSetParquet_page_size_bytes());
        Assertions.assertFalse(t.getIceberg_table_sink().isSetParquet_dict_size_bytes());
        Assertions.assertFalse(t.getIceberg_table_sink().isSetParquet_row_group_size_bytes());
    }

    @Test
    public void testInvalidParquetWriterSizes() {
        Map<String, String> invalidPageSizeProperties = Map.of(PARQUET_PAGE_SIZE_BYTES, "0");
        Map<String, String> invalidDictSizeProperties = Map.of(PARQUET_DICT_SIZE_BYTES, "2147483648");
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                com.starrocks.connector.iceberg.IcebergUtil.getPositiveIntProperty(
                        invalidPageSizeProperties, PARQUET_PAGE_SIZE_BYTES));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                com.starrocks.connector.iceberg.IcebergUtil.getPositiveIntProperty(
                        invalidDictSizeProperties, PARQUET_DICT_SIZE_BYTES));
    }
}
