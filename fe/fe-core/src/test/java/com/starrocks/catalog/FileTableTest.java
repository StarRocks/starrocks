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
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TableFactoryProvider;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.common.EngineType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    public void testCreateExternalTable(@Mocked MetadataMgr metadataMgr) throws Exception {
        String hdfsPath = "hdfs://127.0.0.1:10000/hive/";

        String createTableSql = "create external table db.file_tbl (col1 int, col2 int) engine=file properties " +
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

        String createTableSql2 = "create external table db.file_tbl_parq (col1 int, col2 int) engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"parquet\")";
        CreateTableStmt
                createTableStmt2 = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql2, connectContext);
        com.starrocks.catalog.Table table2 = createTable(createTableStmt2);

        Assert.assertTrue(table2 instanceof FileTable);
        FileTable fileTable2 = (FileTable) table2;
        Assert.assertEquals("file_tbl_parq", fileTable2.getName());
        Assert.assertEquals(hdfsPath, fileTable2.getTableLocation());
        Assert.assertEquals(RemoteFileInputFormat.PARQUET, fileTable2.getFileFormat());

        String createTableSql3 = "create external table db.file_tbl_parq (col1 int, col2 int) engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\")";
        CreateTableStmt
                createTableStmt3 = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql3, connectContext);
        Assert.assertThrows(DdlException.class,
                () -> createTable(createTableStmt3));

        String createTableSql4 = "create external table db.file_tbl_parq (col1 int, col2 int) engine=file properties " +
                "(\"format\"=\"parquet\")";
        CreateTableStmt
                createTableStmt4 = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql4, connectContext);
        Assert.assertThrows(DdlException.class,
                () -> createTable(createTableStmt4));

        String createTableSql5 = "create external table db.file_tbl_parq (col1 int, col2 int) engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"haha\")";
        CreateTableStmt
                createTableStmt5 = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql5, connectContext);
        Assert.assertThrows(DdlException.class,
                () -> createTable(createTableStmt5));
    }
}
