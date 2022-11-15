// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.starrocks.common.DdlException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TableFactory;
import com.starrocks.sql.ast.CreateTableStmt;
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

    @Test
    public void testCreateExternalTable(@Mocked MetadataMgr metadataMgr) throws Exception {
        String hdfsPath = "hdfs://127.0.0.1:10000/hive/";

        String createTableSql = "create external table db.file_tbl (col1 int, col2 int) engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"orc\")";
        CreateTableStmt
                createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = TableFactory.createTable(createTableStmt, Table.TableType.FILE);

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
        com.starrocks.catalog.Table table2 = TableFactory.createTable(createTableStmt2, Table.TableType.FILE);

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
                () -> TableFactory.createTable(createTableStmt3, Table.TableType.FILE));

        String createTableSql4 = "create external table db.file_tbl_parq (col1 int, col2 int) engine=file properties " +
                "(\"format\"=\"parquet\")";
        CreateTableStmt
                createTableStmt4 = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql4, connectContext);
        Assert.assertThrows(DdlException.class,
                () -> TableFactory.createTable(createTableStmt4, Table.TableType.FILE));

        String createTableSql5 = "create external table db.file_tbl_parq (col1 int, col2 int) engine=file properties " +
                "(\"path\"=\"hdfs://127.0.0.1:10000/hive/\", \"format\"=\"haha\")";
        CreateTableStmt
                createTableStmt5 = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql5, connectContext);
        Assert.assertThrows(DdlException.class,
                () -> TableFactory.createTable(createTableStmt5, Table.TableType.FILE));
    }
}
