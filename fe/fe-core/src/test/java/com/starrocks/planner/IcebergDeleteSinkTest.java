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

package com.starrocks.planner;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TIcebergTableSink;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

public class IcebergDeleteSinkTest {

    private MockedStatic<IcebergUtil> mockedIcebergUtil;

    @BeforeEach
    public void setUp() {
        // Mock the static method
        mockedIcebergUtil = mockStatic(IcebergUtil.class);
        CloudConfiguration mockCloudConfig = mock(CloudConfiguration.class);

        // Mock toThrift method to avoid NPE during test
        doAnswer(invocation -> {
            com.starrocks.thrift.TCloudConfiguration tConfig = invocation.getArgument(0);
            tConfig.setCloud_type(com.starrocks.thrift.TCloudType.AWS);
            return null;
        }).when(mockCloudConfig).toThrift(any(com.starrocks.thrift.TCloudConfiguration.class));

        when(IcebergUtil.getVendedCloudConfiguration(anyString(), any(IcebergTable.class)))
                .thenReturn(mockCloudConfig);
        when(IcebergUtil.getVendedCloudConfiguration(isNull(), any(IcebergTable.class)))
                .thenReturn(mockCloudConfig);
    }

    @AfterEach
    public void tearDown() {
        // Close the static mock
        if (mockedIcebergUtil != null) {
            mockedIcebergUtil.close();
        }
    }

    @Test
    public void testValidDeleteTuple() {
        // Create a valid tuple descriptor with _file and _pos columns
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        // Add _file column (STRING type)
        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        // Add _pos column (BIGINT type)
        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Mock IcebergTable
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");

        // Should not throw exception
        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
        assertNotNull(sink);
    }

    @Test
    public void testMissingFileColumn() {
        // Create a tuple descriptor without _file column
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        // Add only _pos column
        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(0), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Mock IcebergTable
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");

        // Should throw exception
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new IcebergDeleteSink(icebergTable, desc, new SessionVariable()));
        assertTrue(exception.getMessage().contains("_file"));
    }

    @Test
    public void testThriftSerialization() {
        // Create a valid tuple descriptor
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Mock IcebergTable
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("hdfs://localhost:9000/iceberg");
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());

        // Check thrift serialization
        TDataSink tDataSink = sink.toThrift();
        assertNotNull(tDataSink);
        assertTrue(tDataSink.isSetIceberg_table_sink());

        TIcebergTableSink icebergSink = tDataSink.getIceberg_table_sink();
        assertEquals("hdfs://localhost:9000/iceberg", icebergSink.getLocation());
        assertEquals("parquet", icebergSink.getFile_format());
        assertFalse(icebergSink.isIs_static_partition_sink());
    }

    @Test
    public void testGetExplainString() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Mock IcebergTable
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());

        String explainString = sink.getExplainString("", TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG DELETE SINK"));
        assertTrue(explainString.contains("iceberg_catalog.db.table"));
        assertTrue(explainString.contains("/tmp/iceberg"));
    }
}
