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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StructTypeTest {
    @Test
    public void testTypeMatch() {
        Assertions.assertTrue(Type.ANY_STRUCT.matchesType(Type.ANY_STRUCT));
        Assertions.assertTrue(Type.ANY_STRUCT.matchesType(Type.ANY_ELEMENT));
        StructType structType = new StructType(Lists.newArrayList(Type.BIGINT, Type.DOUBLE));
        Assertions.assertTrue(Type.ANY_STRUCT.matchesType(structType));
    }

    @Test
    public void testUnnamedStruct() {
        StructType type = new StructType(Lists.newArrayList(Type.INT, Type.DATETIME));
        Assertions.assertEquals("struct<col1 int(11), col2 datetime>", type.toSql());
    }

    @Test
    public void testStructEquals() {
        // test equals() for unnamed struct
        StructType originType = new StructType(Lists.newArrayList(Type.INT, Type.VARCHAR));
        StructType comparedType = new StructType(Lists.newArrayList(Type.INT, Type.VARCHAR));
        Assertions.assertEquals(originType, comparedType);
        Assertions.assertEquals(comparedType, originType);

        comparedType = new StructType(Lists.newArrayList(Type.VARCHAR, Type.INT));
        Assertions.assertNotEquals(originType, comparedType);
        Assertions.assertNotEquals(comparedType, originType);

        // test equals() for unnamed struct & named struct
        StructField tmpField1 = new StructField("hello", Type.INT);
        StructField tmpField2 = new StructField("world", Type.VARCHAR);
        comparedType = new StructType(Lists.newArrayList(tmpField1, tmpField2));
        Assertions.assertNotEquals(originType, comparedType);
        Assertions.assertNotEquals(comparedType, originType);

        // test equals() for named struct & named struct
        StructField tmpField3 = new StructField("hello", Type.INT);
        StructField tmpField4 = new StructField("world", Type.VARCHAR);
        originType = new StructType(Lists.newArrayList(tmpField1, tmpField2));
        comparedType = new StructType(Lists.newArrayList(tmpField3, tmpField4));
        Assertions.assertEquals(originType, comparedType);
        Assertions.assertEquals(comparedType, originType);

        tmpField3 = new StructField("hello123", Type.INT);
        comparedType = new StructType(Lists.newArrayList(tmpField3, tmpField4));
        Assertions.assertNotEquals(originType, comparedType);
        Assertions.assertNotEquals(comparedType, originType);
    }

    @Test
    public void testStructMatchType() throws Exception {
        // "struct<struct_test:int,c1:struct<c1:int,cc1:string>>"
        StructType c1 = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultCatalogString())
        ));
        StructType root = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                new StructField("c1", c1)
        ));

        // PseudoType MapType
        Type t = new AnyMapType();
        Assertions.assertFalse(root.matchesType(t));

        // MapType
        Type keyType = ScalarType.createType(PrimitiveType.INT);
        Type valueType = ScalarType.createCharType(10);
        Type mapType = new MapType(keyType, valueType);

        Assertions.assertFalse(root.matchesType(mapType));

        // Different fields length
        StructType c = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT))));
        Assertions.assertFalse(root.matchesType(c));

        // Types will match with different field names
        StructType diffName = new StructType(Lists.newArrayList(
                new StructField("st", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc", c1)
        ));
        Assertions.assertFalse(root.matchesType(diffName));

        // Different field type
        StructType diffType = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                new StructField("c1", ScalarType.createType(PrimitiveType.INT))
        ));
        Assertions.assertFalse(root.matchesType(diffType));

        // matched
        StructType mc1 = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultCatalogString())
        ));
        StructType matched = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                new StructField("c1", mc1)
        ));
        Assertions.assertTrue(root.matchesType(matched));

        // Won't match with different subfield order
        StructType mc2 = new StructType(Lists.newArrayList(
                new StructField("cc1", ScalarType.createDefaultCatalogString()),
                new StructField("c1", ScalarType.createType(PrimitiveType.INT))
        ));
        StructType matchedDiffOrder = new StructType(Lists.newArrayList(
                new StructField("c1", mc2),
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT))
        ));
        Assertions.assertFalse(root.matchesType(matchedDiffOrder));
    }
}
