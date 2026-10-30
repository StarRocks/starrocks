// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql;

import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.VarbinaryType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

public class MysqlColTypeTest {

    @Test
    public void testGeographyUsesVarbinaryMetadata() {
        GeoTypeDescriptor descriptor = new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOGRAPHY,
                GeoTypeDescriptor.CoordinateSystem.SPHERICAL, GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL,
                "OGC:CRS84", null);
        ScalarType geography = ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, descriptor);
        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_BLOB, MysqlCodec.toMysqlType(PrimitiveType.GEOGRAPHY));
        Assertions.assertEquals(MysqlCodec.CHARSET_BINARY, MysqlCodec.getMysqlResultSetFieldCharsetIndex(geography));
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        ByteArrayOutputStream actual = new ByteArrayOutputStream();
        MysqlCodec.writeField(expected, "db", "table", "value", VarbinaryType.VARBINARY, false, null);
        MysqlCodec.writeField(actual, "db", "table", "value", geography, false, null);
        Assertions.assertArrayEquals(expected.toByteArray(), actual.toByteArray());
        Assertions.assertEquals(PrimitiveType.GEOGRAPHY, geography.getPrimitiveType());
        // This output change does not enable GEOMETRY or change ordinary string metadata.
        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_STRING, MysqlCodec.toMysqlType(PrimitiveType.GEOMETRY));
        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_VAR_STRING, MysqlCodec.toMysqlType(PrimitiveType.VARCHAR));
    }

    @Test
    public void testGetCode() {
        MysqlColType type;
        // decimal
        type = MysqlColType.MYSQL_TYPE_DECIMAL;
        Assertions.assertEquals(0, type.getCode());
        Assertions.assertEquals("DECIMAL", type.toString());

        // tiny
        type = MysqlColType.MYSQL_TYPE_TINY;
        Assertions.assertEquals(1, type.getCode());
        Assertions.assertEquals("TINY INT", type.toString());

        // SHORT
        type = MysqlColType.MYSQL_TYPE_SHORT;
        Assertions.assertEquals(2, type.getCode());
        Assertions.assertEquals("SMALL INT", type.toString());

        // LONG
        type = MysqlColType.MYSQL_TYPE_LONG;
        Assertions.assertEquals(3, type.getCode());
        Assertions.assertEquals("INT", type.toString());

        // FLOAT
        type = MysqlColType.MYSQL_TYPE_FLOAT;
        Assertions.assertEquals(4, type.getCode());
        Assertions.assertEquals("FLOAT", type.toString());

        // DOUBLE
        type = MysqlColType.MYSQL_TYPE_DOUBLE;
        Assertions.assertEquals(5, type.getCode());
        Assertions.assertEquals("DOUBLE", type.toString());

        // NULL
        type = MysqlColType.MYSQL_TYPE_NULL;
        Assertions.assertEquals(6, type.getCode());
        Assertions.assertEquals("NULL", type.toString());
    }

}
