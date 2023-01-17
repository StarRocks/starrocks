// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class StructTypeTest {

    @Test
    public void testStructMatchType() throws Exception {
        // "struct<struct_test:int,c1:struct<c1:int,cc1:string>>"
        StructType c1 = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultExternalTableString())
        ));
        StructType root = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                new StructField("c1", c1)
        ));

        // PseudoType MapType
        Type t = new AnyMapType();
        Assert.assertFalse(root.matchesType(t));

        // MapType
        Type keyType = ScalarType.createType(PrimitiveType.INT);
        Type valueType = ScalarType.createCharType(10);
        Type mapType = new MapType(keyType, valueType);

        Assert.assertFalse(root.matchesType(mapType));

        // Different fields length
        StructType c = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT))));
        Assert.assertFalse(root.matchesType(c));

        // Types will match with different field names
        StructType diffName = new StructType(Lists.newArrayList(
                new StructField("st", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc", c1)
        ));
        Assert.assertTrue(root.matchesType(diffName));

        // Different field type
        StructType diffType = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                new StructField("c1", ScalarType.createType(PrimitiveType.INT))
        ));
        Assert.assertFalse(root.matchesType(diffType));

        // matched
        StructType mc1 = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultExternalTableString())
        ));
        StructType matched = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                new StructField("c1", mc1)
        ));
        Assert.assertTrue(root.matchesType(matched));

        // Won't match with different subfield order
        StructType mc2 = new StructType(Lists.newArrayList(
                new StructField("cc1", ScalarType.createDefaultExternalTableString()),
                new StructField("c1", ScalarType.createType(PrimitiveType.INT))
        ));
        StructType matchedDiffOrder = new StructType(Lists.newArrayList(
                new StructField("c1", mc2),
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT))
        ));
        Assert.assertFalse(root.matchesType(matchedDiffOrder));
    }
}
