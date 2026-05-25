// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.sql.analyzer;

import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShowStmtAnalyzerFilesSchemaTest {

    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testDescFilesWithSchemaRejected() {
        String sql = "DESC FILES(" +
                "  'path' = 'fake://bucket/dir/'," +
                "  'format' = 'parquet'," +
                "  'schema' = 'a INT')";
        // UtFrameUtils.parseStmtWithNewParser wraps SemanticException as AnalysisException.
        AnalysisException e = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        assertInstanceOf(SemanticException.class, e.getCause());
        assertTrue(e.getMessage().contains("'schema' is not supported in DESC FILES"));
    }
}
