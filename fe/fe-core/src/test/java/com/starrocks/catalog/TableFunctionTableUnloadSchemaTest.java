// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.catalog;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TableFunctionTableUnloadSchemaTest {

    @Test
    public void testSchemaRejectedInUnload() {
        List<Column> cols = List.of(new Column("a", Type.INT));
        Map<String, String> p = new HashMap<>();
        p.put("path", "s3://bucket/dir/");
        p.put("format", "parquet");
        p.put("schema", "a INT");
        SemanticException e = assertThrows(SemanticException.class,
                () -> new TableFunctionTable(cols, p, new SessionVariable()));
        assertTrue(e.getMessage().contains("INSERT INTO FILES")
                && e.getMessage().contains("SELECT list"));
    }
}
