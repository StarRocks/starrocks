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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PruneSubfieldsForComplexTypeTest {
    private Type mapType;
    private Type typeMapArrayMap;

    @Before
    public void setup() {
        Type keyType = ScalarType.createType(PrimitiveType.INT);
        Type valueType = ScalarType.createCharType(10);
        mapType = new MapType(keyType, valueType);
        Type arrayType = new ArrayType(mapType);
        Type keyTypeOuter = ScalarType.createType(PrimitiveType.INT);
        typeMapArrayMap = new MapType(keyTypeOuter, arrayType);
    }

    @Test
    public void testSetUsedSubfieldPosGroupEmpty() {
        PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(typeMapArrayMap, new ArrayList<>());
        Assert.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[0]);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assert.assertEquals(2, mapType.getSelectedFields().length);
        Assert.assertEquals(true, mapType.getSelectedFields()[0]);
        Assert.assertEquals(true, mapType.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupNotLeaf() {
        // if the last one is complextype, select all child
        List<Integer> usedSubfieldPos = new ArrayList<>();
        usedSubfieldPos.add(1);
        List<ImmutableList<Integer>> group = new ArrayList<>();
        group.add(ImmutableList.copyOf(usedSubfieldPos));
        PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(typeMapArrayMap, group);
        Assert.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assert.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assert.assertEquals(2, mapType.getSelectedFields().length);
        Assert.assertEquals(true, mapType.getSelectedFields()[0]);
        Assert.assertEquals(true, mapType.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupLeaf() {
        // if the last one is a leaf
        List<Integer> usedSubfieldPos = Arrays.asList(1, 1);
        List<ImmutableList<Integer>> group = new ArrayList<>();
        group.add(ImmutableList.copyOf(usedSubfieldPos));
        PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(typeMapArrayMap, group);
        Assert.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assert.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assert.assertEquals(2, mapType.getSelectedFields().length);
        Assert.assertEquals(false, mapType.getSelectedFields()[0]);
        Assert.assertEquals(true, mapType.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupTwoPos() {
        List<ImmutableList<Integer>> group = new ArrayList<>();
        List<Integer> usedSubfieldPos1 = Arrays.asList(1, 1);
        group.add(ImmutableList.copyOf(usedSubfieldPos1));
        List<Integer> usedSubfieldPos2 = Arrays.asList(1, 0);
        group.add(ImmutableList.copyOf(usedSubfieldPos2));
        PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(typeMapArrayMap, group);
        Assert.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assert.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assert.assertEquals(2, mapType.getSelectedFields().length);
        Assert.assertEquals(true, mapType.getSelectedFields()[0]);
        Assert.assertEquals(true, mapType.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupMapAllField() {
        // if the last one is a leaf
        List<ImmutableList<Integer>> group = new ArrayList<>();
        List<Integer> usedSubfieldPos = Arrays.asList(1, -1);
        group.add(ImmutableList.copyOf(usedSubfieldPos));
        PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(typeMapArrayMap, group);
        Assert.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assert.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assert.assertEquals(2, mapType.getSelectedFields().length);
        Assert.assertEquals(true, mapType.getSelectedFields()[0]);
        Assert.assertEquals(true, mapType.getSelectedFields()[1]);

        Type newMap = typeMapArrayMap.clone();
        group = new ArrayList<>();

        List<Integer> usedSubfield = Arrays.asList(-1);
        group.add(ImmutableList.copyOf(usedSubfield));
        PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(newMap, group);
        Assert.assertEquals(2, newMap.getSelectedFields().length);
        // the cloned map selected 1, 1
        Assert.assertEquals(true, newMap.getSelectedFields()[0]);
        Assert.assertEquals(true, newMap.getSelectedFields()[1]);
        // the origin map selected 0, 1
        Assert.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupMapWrongField() {
        // if the last one is a leaf
        List<ImmutableList<Integer>> group = new ArrayList<>();
        List<Integer> usedSubfieldPos = Arrays.asList(0, 0, 0);
        group.add(ImmutableList.copyOf(usedSubfieldPos));
        Assert.assertThrows(IllegalStateException.class,
                () -> PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(typeMapArrayMap, group));
    }

    @Test
    public void testStructSubfield() {
        Type field1 = ScalarType.createType(PrimitiveType.INT);
        Type field2Map =
                new MapType(ScalarType.createType(PrimitiveType.INT), ScalarType.createType(PrimitiveType.VARCHAR));
        Type field2Str = ScalarType.createType(PrimitiveType.VARCHAR);
        StructField structField1 = new StructField("subfield1", field2Map);
        StructField structField2 = new StructField("subfield2", field2Str);
        ArrayList<StructField> list1 = new ArrayList<>();
        list1.add(structField1);
        list1.add(structField2);
        Type field2 = new StructType(list1);

        StructField topLevel1 = new StructField("field1", field1);
        StructField topLevel2 = new StructField("field2", field2);
        ArrayList<StructField> list2 = new ArrayList<>();
        list2.add(topLevel1);
        list2.add(topLevel2);

        StructType topType = new StructType(list2);

        // type: {
        //   "field1": INT
        //   "field2": {
        //      "subfield1": Map<INT, VARCHAR>,
        //      "subfield2": VARCHAR
        //   }
        // }

        {
            StructType cloneType = topType.clone();
            PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(cloneType, new ArrayList<>());

            Assert.assertTrue(cloneType.getSelectedFields()[0]);
            Assert.assertTrue(cloneType.getSelectedFields()[1]);

            Type tmpType = cloneType.getField(1).getType();
            Assert.assertTrue(tmpType.getSelectedFields()[0]);
            Assert.assertTrue(tmpType.getSelectedFields()[1]);

            tmpType = ((StructType) tmpType).getField(0).getType();
            Assert.assertTrue(tmpType.getSelectedFields()[0]);
            Assert.assertTrue(tmpType.getSelectedFields()[1]);
        }

        {
            StructType cloneType = topType.clone();
            List<ImmutableList<Integer>> group = new ArrayList<>();
            group.add(ImmutableList.of(1));
            PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(cloneType, group);

            Assert.assertFalse(cloneType.getSelectedFields()[0]);
            Assert.assertTrue(cloneType.getSelectedFields()[1]);

            Type tmpType = cloneType.getField(1).getType();
            Assert.assertTrue(tmpType.getSelectedFields()[0]);
            Assert.assertTrue(tmpType.getSelectedFields()[1]);

            tmpType = ((StructType) tmpType).getField(0).getType();
            Assert.assertTrue(tmpType.getSelectedFields()[0]);
            Assert.assertTrue(tmpType.getSelectedFields()[1]);
        }

        {
            StructType cloneType = topType.clone();
            SlotDescriptor slot = new SlotDescriptor(SlotId.createGenerator().getNextId(),
                    "struct_type", cloneType, true);
            ColumnRefOperator colRef = new ColumnRefOperator(0, cloneType, "struct_type", true);
            List<ImmutableList<Integer>> group = new ArrayList<>();
            group.add(ImmutableList.of(1, 1));
            PruneSubfieldsForComplexType.Util.setUsedSubfieldPosGroup(cloneType, group);

            Assert.assertFalse(cloneType.getSelectedFields()[0]);
            Assert.assertTrue(cloneType.getSelectedFields()[1]);

            Type tmpType = cloneType.getField(1).getType();
            Assert.assertFalse(tmpType.getSelectedFields()[0]);
            Assert.assertTrue(tmpType.getSelectedFields()[1]);

            tmpType = ((StructType) tmpType).getField(0).getType();
            Assert.assertFalse(tmpType.getSelectedFields()[0]);
            Assert.assertFalse(tmpType.getSelectedFields()[1]);
        }

    }
}
