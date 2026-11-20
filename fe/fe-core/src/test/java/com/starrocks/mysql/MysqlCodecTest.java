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

package com.starrocks.mysql;

import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class MysqlCodecTest {

    private static final class ColumnDef {
        int charset;
        int type;
        int flags;
    }

    private static ColumnDef writeAndRead(Type type) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MysqlCodec.writeField(out, "c", type);
        ByteBuffer buf = ByteBuffer.wrap(out.toByteArray());
        // catalog, schema, table, org_table, name, org_name
        for (int i = 0; i < 6; i++) {
            MysqlCodec.readLenEncodedString(buf);
        }
        Assertions.assertEquals(0x0c, MysqlCodec.readVInt(buf));
        ColumnDef def = new ColumnDef();
        def.charset = MysqlCodec.readInt2(buf);
        MysqlCodec.readInt4(buf);
        def.type = MysqlCodec.readInt1(buf);
        def.flags = MysqlCodec.readInt2(buf);
        MysqlCodec.readInt1(buf);
        MysqlCodec.readInt2(buf);
        Assertions.assertEquals(0, buf.remaining());
        return def;
    }

    @Test
    public void testVarbinaryIsSentWithBinaryFlag() {
        ColumnDef def = writeAndRead(VarbinaryType.VARBINARY);
        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_BLOB.getCode(), def.type);
        Assertions.assertEquals(MysqlCodec.CHARSET_BINARY, def.charset);
        Assertions.assertEquals(MysqlCodec.BINARY_FLAG, def.flags);
    }

    @Test
    public void testGeographyIsSentWithBinaryFlag() {
        GeoTypeDescriptor descriptor = new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOGRAPHY,
                GeoTypeDescriptor.CoordinateSystem.SPHERICAL, GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL,
                "OGC:CRS84", null);
        ColumnDef def = writeAndRead(ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, descriptor));
        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_BLOB.getCode(), def.type);
        Assertions.assertEquals(MysqlCodec.CHARSET_BINARY, def.charset);
        Assertions.assertEquals(MysqlCodec.BINARY_FLAG, def.flags);
    }

    @Test
    public void testOtherTypesKeepZeroFlags() {
        Assertions.assertEquals(0, writeAndRead(VarcharType.VARCHAR).flags);
        Assertions.assertEquals(0, writeAndRead(IntegerType.INT).flags);
    }
}
