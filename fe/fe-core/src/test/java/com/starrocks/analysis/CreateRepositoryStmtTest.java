// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateRepositoryStmtTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws UserException {
        String sql =
                "CREATE REPOSITORY `hdfs_repo` WITH BROKER `hdfs_broker` ON LOCATION \"hdfs://hadoop-name-node:54310/path/to/repo/\" " +
                        " PROPERTIES ( \"username\" = \"username1\", \"password\" = \"password1\" )";
        CreateRepositoryStmt stmt = (CreateRepositoryStmt) com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        Assert.assertTrue(stmt.needAuditEncryption());
        Assert.assertEquals("hdfs_broker", stmt.getBrokerName());
        Assert.assertEquals("hdfs_repo", stmt.getName());
        Assert.assertEquals("hdfs://hadoop-name-node:54310/path/to/repo/", stmt.getLocation());
        Assert.assertNotNull(stmt.getProperties());
    }

    @Test
    public void testToString() {
        String sql =
                "CREATE REPOSITORY `hdfs_repo` WITH BROKER `hdfs_broker` ON LOCATION \"hdfs://hadoop-name-node:54310/path/to/repo/\" " +
                        " PROPERTIES ( \"username\" = \"username1\", \"password\" = \"password1\" )";
        CreateRepositoryStmt stmt = (CreateRepositoryStmt) com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        Assert.assertEquals("CREATE REPOSITORY hdfs_repo WITH BROKER hdfs_broker ON LOCATION \"hdfs://hadoop-name-node:54310/path/to/repo/\" " +
                "PROPERTIES (\"password\" = \"***\", \"username\" = \"username1\" )", AstToStringBuilder.toString(stmt));
    }
}
