// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class DateTruncTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setQueryId(UUIDUtil.genUUID());
    }

    @Test
    public void testTruncAnalyze() throws Exception {
        String sql1 =
                "SELECT date_trunc('century', '2020-11-03 23:41:37')";
        StmtExecutor stmtExecutor1 = new StmtExecutor(ctx, sql1);
        stmtExecutor1.execute();
    }
}
