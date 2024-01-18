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

package com.starrocks.catalog;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.exception.DdlException;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.connector.hive.TextFileFormatDesc;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.TableFactoryProvider;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.common.EngineType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileTableTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @Before
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("db");
    }

    Table createTable(CreateTableStmt stmt) throws DdlException {
        return TableFactoryProvider.getFactory(EngineType.FILE.name()).createTable(null, null, stmt);
    }

    @Test
    public void testCreateExternalTable() throws Exception {
        String hdfsPath = "hdfs://127.0.0.1:10000/hive/";

        String createTableSql =
                "create external table if not exists db.file_tbl (col1 int, col2 int) engine=file properties " +
                        "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"orc\")";
        CreateTableStmt
                createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);

        Assert.assertTrue(table instanceof FileTable);
        FileTable fileTable = (FileTable) table;
        Assert.assertEquals("file_tbl", fileTable.getName());
        Assert.assertEquals(hdfsPath, fileTable.getTableLocation());
        Assert.assertEquals(RemoteFileInputFormat.ORC, fileTable.getFileFormat());
        Assert.assertEquals(hdfsPath, fileTable.getFileProperties().get("path"));
        Assert.assertEquals("orc", fileTable.getFileProperties().get("format"));

        String createTableSql2 = "create external table if not exists db.file_tbl_parq (col1 int, col2 int) " +
                "engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"parquet\")";
        CreateTableStmt
                createTableStmt2 =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql2, connectContext);
        com.starrocks.catalog.Table table2 = createTable(createTableStmt2);

        Assert.assertTrue(table2 instanceof FileTable);
        FileTable fileTable2 = (FileTable) table2;
        Assert.assertEquals("file_tbl_parq", fileTable2.getName());
        Assert.assertEquals(hdfsPath, fileTable2.getTableLocation());
        Assert.assertEquals(RemoteFileInputFormat.PARQUET, fileTable2.getFileFormat());

        String createTableSql3 = "create external table  if not exists  db.file_tbl_parq (col1 int, col2 int) " +
                "engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\")";
        CreateTableStmt
                createTableStmt3 =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql3, connectContext);
        Assert.assertThrows(DdlException.class,
                () -> createTable(createTableStmt3));

        String createTableSql4 = "create external table if not exists  db.file_tbl_parq (col1 int, col2 int) " +
                "engine=file properties " +
                "(\"format\"=\"parquet\")";
        CreateTableStmt
                createTableStmt4 =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql4, connectContext);
        Assert.assertThrows(DdlException.class,
                () -> createTable(createTableStmt4));

        String createTableSql5 = "create external table if not exists  db.file_tbl_parq (col1 int, col2 int) " +
                "engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"haha\")";
        CreateTableStmt
                createTableStmt5 =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql5, connectContext);
        Assert.assertThrows(DdlException.class,
                () -> createTable(createTableStmt5));
    }

    @Test
    public void testCreateTextExternalTable() throws Exception {
        String hdfsPath = "hdfs://127.0.0.1:10000/hive/";

        {
            String createTableSql =
                    "create external table if not exists db.file_tbl (col1 int, col2 int) engine=file properties " +
                            "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"text\", \"csv_separator\"=\",\", " +
                            "\"row_delimiter\"=\"xx\", \"collection_delimiter\"=\"yy\", \"map_delimiter\"=\"zz\")";
            CreateTableStmt
                    createTableStmt =
                    (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
            com.starrocks.catalog.Table table = createTable(createTableStmt);

            Assert.assertTrue(table instanceof FileTable);
            FileTable fileTable = (FileTable) table;
            Assert.assertEquals("file_tbl", fileTable.getName());
            Assert.assertEquals(hdfsPath, fileTable.getTableLocation());
            Assert.assertEquals(RemoteFileInputFormat.TEXT, fileTable.getFileFormat());
            Assert.assertEquals(hdfsPath, fileTable.getFileProperties().get("path"));
            Assert.assertEquals("text", fileTable.getFileProperties().get("format"));
        }
    }

    @Test
    public void testAddTextFileFormatDescr() throws Exception {
        class ExtFileTable extends FileTable {
            public ExtFileTable(Map<String, String> properties)
                    throws DdlException {
                super(0, "XX", new ArrayList<>(), properties);
            }

            @Override
            public List<RemoteFileDesc> getFileDescsFromHdfs() throws DdlException {
                List<RemoteFileDesc> fileDescList = new ArrayList<>();
                RemoteFileDesc fileDesc = new RemoteFileDesc("aa", "snappy", 0, 0, null, null);
                fileDescList.add(fileDesc);
                return fileDescList;
            }
        }

        Map<String, String> properties = new HashMap<String, String>() {
            {
                put(FileTable.JSON_KEY_FILE_PATH, "hdfs://127.0.0.1:10000/hive/");
                put(FileTable.JSON_KEY_COLUMN_SEPARATOR, "XXX");
                put(FileTable.JSON_KEY_ROW_DELIMITER, "YYY");
                put(FileTable.JSON_KEY_COLLECTION_DELIMITER, "ZZZ");
                put(FileTable.JSON_KEY_MAP_DELIMITER, "MMM");
                put(FileTable.JSON_KEY_FORMAT, "text");
            }
        };

        FileTable f = new ExtFileTable(properties);
        List<RemoteFileDesc> files = f.getFileDescs();
        Assert.assertEquals(files.size(), 1);
        TextFileFormatDesc desc = files.get(0).getTextFileFormatDesc();
        Assert.assertEquals(desc.getFieldDelim(), "XXX");
        Assert.assertEquals(desc.getLineDelim(), "YYY");
        Assert.assertEquals(desc.getCollectionDelim(), "ZZZ");
        Assert.assertEquals(desc.getMapkeyDelim(), "MMM");
    }

    @Test
    public void testCreateTextExternalTableFormat() throws Exception {
        String createTableSql =
                "create external table if not exists db.file_tbl (col1 int, col2 int, col3 string) engine=file properties " +
                        "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"avro\")";
        CreateTableStmt
                createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);

        Assert.assertTrue(table instanceof FileTable);
        FileTable fileTable = (FileTable) table;
        List<DescriptorTable.ReferencedPartitionInfo> partitions = new ArrayList<>();
        TTableDescriptor tTableDescriptor = fileTable.toThrift(partitions);

        Assert.assertEquals(tTableDescriptor.getFileTable().getInput_format(),
                HiveStorageFormat.get("avro").getInputFormat());
        Assert.assertEquals(tTableDescriptor.getFileTable().getSerde_lib(), HiveStorageFormat.get("avro").getSerde());
        Assert.assertEquals(tTableDescriptor.getFileTable().getHive_column_names(), "col1,col2,col3");
        Assert.assertEquals(tTableDescriptor.getFileTable().getHive_column_types(), "int#int#string");
    }
}
