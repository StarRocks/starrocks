// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

package com.starrocks.sql.parser;

import com.starrocks.catalog.Column;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlParserFilesSchemaTest {

    @Test
    public void testScalarTypes() {
        List<Column> cols = SqlParser.parseFilesSchema(
                "a INT, b BIGINT, c VARCHAR(64), d DATETIME, e DECIMAL(20,4)");
        assertEquals(5, cols.size());
        assertEquals("a", cols.get(0).getName());
        assertEquals(PrimitiveType.INT, cols.get(0).getType().getPrimitiveType());
        assertEquals("b", cols.get(1).getName());
        assertEquals(PrimitiveType.BIGINT, cols.get(1).getType().getPrimitiveType());
        assertEquals(PrimitiveType.VARCHAR, cols.get(2).getType().getPrimitiveType());
        assertTrue(cols.get(0).isAllowNull(), "columns must default to nullable");
    }

    @Test
    public void testNestedTypes() {
        List<Column> cols = SqlParser.parseFilesSchema(
                "arr ARRAY<INT>, " +
                "m MAP<VARCHAR(32), BIGINT>, " +
                "s STRUCT<a INT, b VARCHAR(64)>");
        assertEquals(3, cols.size());
        assertTrue(cols.get(0).getType() instanceof ArrayType);
        assertTrue(cols.get(1).getType() instanceof MapType);
        assertTrue(cols.get(2).getType() instanceof StructType);
        StructType s = (StructType) cols.get(2).getType();
        assertEquals(2, s.getFields().size());
        assertEquals("a", s.getFields().get(0).getName());
    }

    @Test
    public void testDeeplyNestedStruct() {
        List<Column> cols = SqlParser.parseFilesSchema(
                "req STRUCT<device STRUCT<platform VARCHAR(64), ver INT>, ts BIGINT>");
        assertEquals(1, cols.size());
        StructType outer = (StructType) cols.get(0).getType();
        assertEquals(2, outer.getFields().size());
        assertTrue(outer.getFields().get(0).getType() instanceof StructType);
    }

    @Test
    public void testBacktickedAndReservedIdentifiers() {
        List<Column> cols = SqlParser.parseFilesSchema("`schema` BIGINT, `order` VARCHAR(16)");
        assertEquals("schema", cols.get(0).getName());
        assertEquals("order", cols.get(1).getName());
    }

    @Test
    public void testRejectsNotNull() {
        ParsingException e = assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a INT NOT NULL"));
        // ANTLR error message references the unexpected token
        assertTrue(e.getMessage().toLowerCase().contains("not") ||
                        e.getMessage().contains("extraneous"),
                "message should indicate grammar error, got: " + e.getMessage());
    }

    @Test
    public void testRejectsNullable() {
        assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a INT NULL"));
    }

    @Test
    public void testRejectsDefault() {
        assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a INT DEFAULT 0"));
    }

    @Test
    public void testRejectsComment() {
        assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a INT COMMENT 'x'"));
    }

    @Test
    public void testRejectsKey() {
        assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a INT KEY"));
    }

    @Test
    public void testRejectsAutoIncrement() {
        assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a BIGINT AUTO_INCREMENT"));
    }

    @Test
    public void testRejectsGeneratedColumn() {
        assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a INT AS (b + 1)"));
    }

    @Test
    public void testRejectsMissingType() {
        assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a, b INT"));
    }

    @Test
    public void testRejectsTrailingComma() {
        assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a INT,"));
    }

    @Test
    public void testRejectsDuplicateColumnName() {
        ParsingException e = assertThrows(ParsingException.class,
                () -> SqlParser.parseFilesSchema("a INT, A BIGINT"));
        assertTrue(e.getMessage().contains("duplicate column in 'schema'"),
                "message should call out duplicate, got: " + e.getMessage());
    }
}
