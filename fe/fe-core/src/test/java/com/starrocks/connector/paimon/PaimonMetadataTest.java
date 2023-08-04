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

package com.starrocks.connector.paimon;

import com.google.common.collect.Lists;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.connector.RemoteFileInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileMeta.DUMMY_LEVEL;
import static org.apache.paimon.io.DataFileMeta.EMPTY_KEY_STATS;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MAX_KEY;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MIN_KEY;

public class PaimonMetadataTest {
    @Mocked
    Catalog paimonNativeCatalog;
    private PaimonMetadata metadata;
    private final List<DataSplit> splits = new ArrayList<>();

    @Before
    public void setUp() {
        this.metadata = new PaimonMetadata("paimon_catalog", paimonNativeCatalog,
                "filesystem", null, "hdfs://127.0.0.1:9999/warehouse");

        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        BinaryRow row2 = new BinaryRow(2);
        writer = new BinaryRowWriter(row2, 10);
        writer.writeInt(0, 3000);
        writer.writeInt(1, 5555);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();
        meta1.add(new DataFileMeta("file1", 100, 200, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_KEY_STATS, null,
                1, 1, 1, DUMMY_LEVEL));
        meta1.add(new DataFileMeta("file2", 100, 300, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_KEY_STATS, null,
                1, 1, 1, DUMMY_LEVEL));

        List<DataFileMeta> meta2 = new ArrayList<>();
        meta2.add(new DataFileMeta("file3", 100, 400, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_KEY_STATS, null,
                1, 1, 1, DUMMY_LEVEL));

        this.splits.add(new DataSplit(1L, row1, 1, meta1, false));
        this.splits.add(new DataSplit(1L, row2, 1, meta2, false));
    }

    @Test
    public void testGetTable(@Mocked AbstractFileStoreTable paimonNativeTable) throws Catalog.TableNotExistException {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(1, "col2", new IntType()));
        new Expectations() {
            {
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonNativeTable;
                paimonNativeTable.rowType().getFields();
                result = fields;
                paimonNativeTable.partitionKeys();
                result = new ArrayList<>(Collections.singleton("col1"));
                paimonNativeTable.location().toString();
                result = "hdfs://127.0.0.1:10000/paimon";
            }
        };
        com.starrocks.catalog.Table table = metadata.getTable("db1", "tbl1");
        PaimonTable paimonTable = (PaimonTable) table;
        Assert.assertEquals("db1", paimonTable.getDbName());
        Assert.assertEquals("tbl1", paimonTable.getTableName());
        Assert.assertEquals(Lists.newArrayList("col1"), paimonTable.getPartitionColumnNames());
        Assert.assertEquals("hdfs://127.0.0.1:10000/paimon", paimonTable.getTableLocation());
        Assert.assertEquals(ScalarType.INT, paimonTable.getBaseSchema().get(0).getType());
        Assert.assertEquals("paimon_catalog", paimonTable.getCatalogName());
    }

    @Test
    public void testListPartitionNames(@Mocked AbstractFileStoreTable paimonNativeTable,
                                       @Mocked ReadBuilder readBuilder) throws Catalog.TableNotExistException {

        RowType partitionRowType = RowType.of(
                new DataType[] {
                        new DateType(false),
                        new IntType(true)
                },
                new String[] {"dt", "hr"});

        List<String> partitionNames = Lists.newArrayList("dt", "hr");

        new Expectations() {
            {
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonNativeTable;
                paimonNativeTable.partitionKeys();
                result = partitionNames;
                paimonNativeTable.schema().logicalPartitionType();
                result = partitionRowType;
                paimonNativeTable.newReadBuilder();
                result = readBuilder;
                readBuilder.newScan().plan().splits();
                result = splits;
            }
        };
        List<String> result = metadata.listPartitionNames("db1", "tbl1");
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.contains("dt=2000/hr=4444"));
        Assert.assertTrue(result.contains("dt=3000/hr=5555"));
    }

    @Test
    public void testGetRemoteFileInfos(@Mocked AbstractFileStoreTable paimonNativeTable,
                                       @Mocked ReadBuilder readBuilder)
            throws Catalog.TableNotExistException {
        new Expectations() {
            {
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonNativeTable;
                paimonNativeTable.newReadBuilder();
                result = readBuilder;
                readBuilder.withFilter((List<Predicate>) any).withProjection((int[]) any).newScan().plan().splits();
                result = splits;
            }
        };
        PaimonTable paimonTable = (PaimonTable) metadata.getTable("db1", "tbl1");
        List<String> requiredNames = Lists.newArrayList("f2", "dt");
        List<RemoteFileInfo> result = metadata.getRemoteFileInfos(paimonTable, null, -1, null, requiredNames);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).getFiles().size());
        Assert.assertEquals(2, result.get(0).getFiles().get(0).getPaimonSplitsInfo().getPaimonSplits().size());
    }

    @Test
    public void testUUID(@Mocked AbstractFileStoreTable paimonNativeTable,
                         @Mocked ReadBuilder readBuilder) {
        PaimonTable paimonTable = (PaimonTable) metadata.getTable("db1", "tbl1");
        Assert.assertTrue(paimonTable.getUUID().startsWith("paimon_catalog.db1.tbl1"));
    }
}
