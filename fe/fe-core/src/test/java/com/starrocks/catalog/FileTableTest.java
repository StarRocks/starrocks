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

import com.starrocks.common.DdlException;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.connector.hive.TextFileFormatDesc;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.TableFactoryProvider;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.common.EngineType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileTableTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeEach
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

        Assertions.assertTrue(table instanceof FileTable);
        FileTable fileTable = (FileTable) table;
        Assertions.assertEquals("file_tbl", fileTable.getName());
        Assertions.assertEquals(hdfsPath, fileTable.getTableLocation());
        Assertions.assertEquals(RemoteFileInputFormat.ORC, fileTable.getFileFormat());
        Assertions.assertEquals(hdfsPath, fileTable.getFileProperties().get("path"));
        Assertions.assertEquals("orc", fileTable.getFileProperties().get("format"));

        String createTableSql2 = "create external table if not exists db.file_tbl_parq (col1 int, col2 int) " +
                "engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"parquet\")";
        CreateTableStmt
                createTableStmt2 =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql2, connectContext);
        com.starrocks.catalog.Table table2 = createTable(createTableStmt2);

        Assertions.assertTrue(table2 instanceof FileTable);
        FileTable fileTable2 = (FileTable) table2;
        Assertions.assertEquals("file_tbl_parq", fileTable2.getName());
        Assertions.assertEquals(hdfsPath, fileTable2.getTableLocation());
        Assertions.assertEquals(RemoteFileInputFormat.PARQUET, fileTable2.getFileFormat());

        String createTableSql3 = "create external table  if not exists  db.file_tbl_parq (col1 int, col2 int) " +
                "engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\")";
        CreateTableStmt
                createTableStmt3 =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql3, connectContext);
        Assertions.assertThrows(DdlException.class,
                () -> createTable(createTableStmt3));

        String createTableSql4 = "create external table if not exists  db.file_tbl_parq (col1 int, col2 int) " +
                "engine=file properties " +
                "(\"format\"=\"parquet\")";
        CreateTableStmt
                createTableStmt4 =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql4, connectContext);
        Assertions.assertThrows(DdlException.class,
                () -> createTable(createTableStmt4));

        String createTableSql5 = "create external table if not exists  db.file_tbl_parq (col1 int, col2 int) " +
                "engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"haha\")";
        CreateTableStmt
                createTableStmt5 =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql5, connectContext);
        Assertions.assertThrows(DdlException.class,
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

            Assertions.assertTrue(table instanceof FileTable);
            FileTable fileTable = (FileTable) table;
            Assertions.assertEquals("file_tbl", fileTable.getName());
            Assertions.assertEquals(hdfsPath, fileTable.getTableLocation());
            Assertions.assertEquals(RemoteFileInputFormat.TEXTFILE, fileTable.getFileFormat());
            Assertions.assertEquals(hdfsPath, fileTable.getFileProperties().get("path"));
            Assertions.assertEquals("text", fileTable.getFileProperties().get("format"));
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
                RemoteFileDesc fileDesc = new RemoteFileDesc("aa", "snappy", 0, 0, null);
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
        Assertions.assertEquals(files.size(), 1);
        TextFileFormatDesc desc = files.get(0).getTextFileFormatDesc();
        Assertions.assertEquals(desc.getFieldDelim(), "XXX");
        Assertions.assertEquals(desc.getLineDelim(), "YYY");
        Assertions.assertEquals(desc.getCollectionDelim(), "ZZZ");
        Assertions.assertEquals(desc.getMapkeyDelim(), "MMM");
    }

    @Test
    public void testCreateTextExternalTableFormat() throws Exception {
        String createTableSql =
                "create external table if not exists db.file_tbl (col1 int, col2 int, col3 string) engine=file properties " +
                        "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"avro\")";
        CreateTableStmt
                createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);

        Assertions.assertTrue(table instanceof FileTable);
        FileTable fileTable = (FileTable) table;
        List<DescriptorTable.ReferencedPartitionInfo> partitions = new ArrayList<>();
        TTableDescriptor tTableDescriptor = fileTable.toThrift(partitions);

        Assertions.assertEquals(tTableDescriptor.getFileTable().getInput_format(),
                HiveStorageFormat.get("avro").getInputFormat());
        Assertions.assertEquals(tTableDescriptor.getFileTable().getSerde_lib(), HiveStorageFormat.get("avro").getSerde());
        Assertions.assertEquals(tTableDescriptor.getFileTable().getHive_column_names(), "col1,col2,col3");
        Assertions.assertEquals(tTableDescriptor.getFileTable().getHive_column_types(), "int#int#string");
    }
}
