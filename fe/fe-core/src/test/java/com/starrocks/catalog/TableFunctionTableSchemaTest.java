// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package com.starrocks.catalog;

import com.starrocks.common.DdlException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.catalog.TableFunctionTable.FAKE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TableFunctionTableSchemaTest {

    private static Map<String, String> baseProps() {
        Map<String, String> m = new HashMap<>();
        m.put("path", FAKE_PATH + "some/dir/");
        m.put("format", "parquet");
        return m;
    }

    @Test
    public void testSchemaReplacesInferredColumns() throws Exception {
        Map<String, String> p = baseProps();
        p.put("schema", "user_id BIGINT, event_time DATETIME");
        TableFunctionTable t = new TableFunctionTable(p);
        assertEquals(2, t.getFullSchema().size());
        assertEquals("user_id", t.getFullSchema().get(0).getName());
        assertTrue(t.hasExplicitSchema());
    }

    @Test
    public void testSchemaMutexWithAutoDetectSampleFiles() {
        Map<String, String> p = baseProps();
        p.put("schema", "a INT");
        p.put("auto_detect_sample_files", "3");
        DdlException e = assertThrows(DdlException.class, () -> new TableFunctionTable(p));
        assertTrue(e.getMessage().contains("'auto_detect_sample_files'")
                && e.getMessage().contains("'schema'"));
    }

    @Test
    public void testSchemaMutexWithAutoDetectSampleRows() {
        Map<String, String> p = baseProps();
        p.put("schema", "a INT");
        p.put("auto_detect_sample_rows", "100");
        assertThrows(DdlException.class, () -> new TableFunctionTable(p));
    }

    @Test
    public void testSchemaMutexWithAutoDetectTypes() {
        Map<String, String> p = baseProps();
        p.put("schema", "a INT");
        p.put("auto_detect_types", "false");
        assertThrows(DdlException.class, () -> new TableFunctionTable(p));
    }

    @Test
    public void testSchemaDefaultsDoNotTriggerMutex() throws Exception {
        // schema alone should not fail just because the defaults for auto_detect_* are set on the field.
        Map<String, String> p = baseProps();
        p.put("schema", "a INT");
        TableFunctionTable t = new TableFunctionTable(p);
        assertTrue(t.hasExplicitSchema());
    }

    @Test
    public void testSchemaEmptyStringRejected() {
        Map<String, String> p = baseProps();
        p.put("schema", "");
        DdlException e = assertThrows(DdlException.class, () -> new TableFunctionTable(p));
        assertTrue(e.getMessage().contains("empty"));
    }

    @Test
    public void testSchemaBlankStringRejected() {
        Map<String, String> p = baseProps();
        p.put("schema", "   ");
        assertThrows(DdlException.class, () -> new TableFunctionTable(p));
    }

    @Test
    public void testSchemaInvalidGrammarWrapped() {
        Map<String, String> p = baseProps();
        p.put("schema", "a INT NOT NULL");
        DdlException e = assertThrows(DdlException.class, () -> new TableFunctionTable(p));
        assertTrue(e.getMessage().contains("invalid 'schema'"));
    }

    @Test
    public void testSchemaOverridesFakePathBuiltins() throws Exception {
        Map<String, String> p = baseProps();
        p.put("schema", "foo INT");
        TableFunctionTable t = new TableFunctionTable(p);
        assertEquals(1, t.getFullSchema().size());
        assertEquals("foo", t.getFullSchema().get(0).getName());
        // col_int / col_string (fake path built-ins) must NOT appear
        assertTrue(t.getFullSchema().stream().noneMatch(c -> c.getName().equals("col_int")));
    }

    @Test
    public void testSchemaPlusColumnsFromPathAppend() throws Exception {
        Map<String, String> p = baseProps();
        p.put("schema", "id BIGINT, name VARCHAR(64)");
        p.put("columns_from_path", "dt,region");
        TableFunctionTable t = new TableFunctionTable(p);
        assertEquals(4, t.getFullSchema().size());
        assertEquals("id", t.getFullSchema().get(0).getName());
        assertEquals("name", t.getFullSchema().get(1).getName());
        assertEquals("dt", t.getFullSchema().get(2).getName());
        assertEquals("region", t.getFullSchema().get(3).getName());
    }

    @Test
    public void testSchemaColumnsFromPathConflict() {
        Map<String, String> p = baseProps();
        p.put("schema", "dt BIGINT, name VARCHAR(64)");
        p.put("columns_from_path", "dt,region");
        DdlException e = assertThrows(DdlException.class, () -> new TableFunctionTable(p));
        assertTrue(e.getMessage().toLowerCase().contains("dup"));
    }

    @Test
    public void testSchemaIgnoredWhenListFilesOnly() throws Exception {
        Map<String, String> p = baseProps();
        p.put("schema", "a INT");
        p.put("list_files_only", "true");
        TableFunctionTable t = new TableFunctionTable(p);
        // LIST FILES schema, not the user's schema
        assertTrue(t.getFullSchema().stream()
                .anyMatch(c -> c.getName().equalsIgnoreCase("PATH")));
        assertFalse(t.hasExplicitSchema());
    }
}
