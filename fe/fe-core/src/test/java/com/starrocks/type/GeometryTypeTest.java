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

package com.starrocks.type;

import com.starrocks.catalog.TypeSerializer;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnDef.DefaultValueDef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.analyzer.ColumnDefAnalyzer;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNodeType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class GeometryTypeTest {

    // -----------------------------------------------------------------------
    // PrimitiveType enum
    // -----------------------------------------------------------------------

    @Test
    public void testPrimitiveTypeEnumExists() {
        PrimitiveType geom = PrimitiveType.GEOMETRY;
        Assertions.assertNotNull(geom);
        Assertions.assertEquals("GEOMETRY", geom.toString());
    }

    @Test
    public void testPrimitiveTypeSize() {
        // GEOMETRY is declared with size 16 (pointer-width, like VARBINARY)
        Assertions.assertEquals(16, PrimitiveType.GEOMETRY.getTypeSize());
    }

    // -----------------------------------------------------------------------
    // GeometryType class
    // -----------------------------------------------------------------------

    @Test
    public void testGeometryTypeSingleton() {
        ScalarType geom1 = GeometryType.GEOMETRY;
        ScalarType geom2 = GeometryType.GEOMETRY;
        Assertions.assertSame(geom1, geom2);
    }

    @Test
    public void testGeometryTypePrimitiveType() {
        Assertions.assertEquals(PrimitiveType.GEOMETRY, GeometryType.GEOMETRY.getPrimitiveType());
    }

    @Test
    public void testGeometryTypeLength() {
        Assertions.assertEquals(GeometryType.MAX_GEOMETRY_WKB_LENGTH, GeometryType.GEOMETRY.getLength());
    }

    @Test
    public void testGeometryTypeIsScalarType() {
        Assertions.assertTrue(GeometryType.GEOMETRY.isScalarType());
        Assertions.assertTrue(GeometryType.GEOMETRY.isScalarType(PrimitiveType.GEOMETRY));
    }

    // -----------------------------------------------------------------------
    // Type.isGeometryType()
    // -----------------------------------------------------------------------

    @Test
    public void testIsGeometryType() {
        Assertions.assertTrue(GeometryType.GEOMETRY.isGeometryType());
    }

    @Test
    public void testIsGeometryTypeFalseForOthers() {
        Assertions.assertFalse(HLLType.HLL.isGeometryType());
        Assertions.assertFalse(VarcharType.VARCHAR.isGeometryType());
        Assertions.assertFalse(IntegerType.INT.isGeometryType());
        Assertions.assertFalse(JsonType.JSON.isGeometryType());
    }

    // -----------------------------------------------------------------------
    // TypeParser
    // -----------------------------------------------------------------------

    @Test
    public void testTypeParserGeometry() {
        // TypeParser is package-private; exercise it via TypeFactory roundtrip
        ScalarType type = TypeFactory.createType(PrimitiveType.GEOMETRY);
        Assertions.assertNotNull(type);
        Assertions.assertEquals(PrimitiveType.GEOMETRY, type.getPrimitiveType());
    }

    // -----------------------------------------------------------------------
    // TypeSerializer — GEOMETRY → TPrimitiveType.GEOMETRY
    // -----------------------------------------------------------------------

    @Test
    public void testSerializeGeometryType() {
        TPrimitiveType thrift = TypeSerializer.toThrift(PrimitiveType.GEOMETRY);
        Assertions.assertEquals(TPrimitiveType.GEOMETRY, thrift);
    }

    @Test
    public void testSerializeGeometryTypeViaContainer() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        TypeSerializer.toThrift(GeometryType.GEOMETRY, container);

        Assertions.assertEquals(1, container.types.size());
        Assertions.assertEquals(TTypeNodeType.SCALAR, container.types.get(0).getType());
        Assertions.assertEquals(TPrimitiveType.GEOMETRY,
                container.types.get(0).getScalar_type().getType());
    }

    // -----------------------------------------------------------------------
    // ColumnDef with GEOMETRY type
    // -----------------------------------------------------------------------

    @Test
    public void testColumnDefGeometryType() throws Exception {
        TypeDef typeDef = new TypeDef(GeometryType.GEOMETRY);
        ColumnDef column = new ColumnDef("geom", typeDef, true, null, null,
                true, DefaultValueDef.NOT_SET, "");
        ColumnDefAnalyzer.analyze(column, true);

        Assertions.assertEquals("geom", column.getName());
        Assertions.assertEquals(PrimitiveType.GEOMETRY, column.getType().getPrimitiveType());
        Assertions.assertTrue(column.isAllowNull());
    }

    // -----------------------------------------------------------------------
    // No regression: existing types still serialize correctly
    // -----------------------------------------------------------------------

    @Test
    public void testExistingTypesUnaffected() {
        Assertions.assertEquals(TPrimitiveType.VARCHAR, TypeSerializer.toThrift(PrimitiveType.VARCHAR));
        Assertions.assertEquals(TPrimitiveType.VARBINARY, TypeSerializer.toThrift(PrimitiveType.VARBINARY));
        Assertions.assertEquals(TPrimitiveType.VARIANT, TypeSerializer.toThrift(PrimitiveType.VARIANT));
        Assertions.assertEquals(TPrimitiveType.JSON, TypeSerializer.toThrift(PrimitiveType.JSON));
        Assertions.assertEquals(TPrimitiveType.HLL, TypeSerializer.toThrift(PrimitiveType.HLL));
    }
}
