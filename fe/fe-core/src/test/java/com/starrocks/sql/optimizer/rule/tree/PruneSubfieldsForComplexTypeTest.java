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
import com.starrocks.catalog.ComplexTypeAccessGroup;
import com.starrocks.catalog.ComplexTypeAccessPaths;
import com.starrocks.type.ArrayType;
import com.starrocks.type.ComplexTypeAccessPath;
import com.starrocks.type.ComplexTypeAccessPathType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class PruneSubfieldsForComplexTypeTest {
    private Type mapType;
    private Type typeMapArrayMap;

    @BeforeEach
    public void setup() {
        Type keyType = IntegerType.INT;
        Type valueType = TypeFactory.createCharType(10);
        mapType = new MapType(keyType, valueType);
        Type arrayType = new ArrayType(mapType);
        Type keyTypeOuter = IntegerType.INT;
        typeMapArrayMap = new MapType(keyTypeOuter, arrayType);
    }

    @Test
    public void testSetUsedSubfieldPosGroupEmpty() {
        PruneComplexTypeUtil.setAccessGroup(typeMapArrayMap, new ComplexTypeAccessGroup());
        Assertions.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assertions.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assertions.assertEquals(false, typeMapArrayMap.getSelectedFields()[1]);
        Assertions.assertEquals(2, mapType.getSelectedFields().length);
        Assertions.assertEquals(false, mapType.getSelectedFields()[0]);
        Assertions.assertEquals(false, mapType.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupNotLeaf() {
        // if the last one is complextype, select all child
        List<ComplexTypeAccessPath> accessPaths = new ArrayList<>();
        accessPaths.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_VALUE));
        ComplexTypeAccessGroup group = new ComplexTypeAccessGroup();
        group.addAccessPaths(new ComplexTypeAccessPaths(ImmutableList.copyOf(accessPaths)));
        PruneComplexTypeUtil.setAccessGroup(typeMapArrayMap, group);
        Assertions.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assertions.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assertions.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assertions.assertEquals(2, mapType.getSelectedFields().length);
        Assertions.assertEquals(true, mapType.getSelectedFields()[0]);
        Assertions.assertEquals(true, mapType.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupLeaf() {
        // if the last one is a leaf
        List<ComplexTypeAccessPath> accessPaths = new ArrayList<>();
        accessPaths.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_VALUE));
        accessPaths.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_VALUE));
        ComplexTypeAccessGroup group = new ComplexTypeAccessGroup();
        group.addAccessPaths(new ComplexTypeAccessPaths(ImmutableList.copyOf(accessPaths)));
        PruneComplexTypeUtil.setAccessGroup(typeMapArrayMap, group);
        Assertions.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assertions.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assertions.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assertions.assertEquals(2, mapType.getSelectedFields().length);
        Assertions.assertEquals(false, mapType.getSelectedFields()[0]);
        Assertions.assertEquals(true, mapType.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupTwoPos() {
        ComplexTypeAccessGroup group = new ComplexTypeAccessGroup();

        List<ComplexTypeAccessPath> accessPaths1 = new ArrayList<>();
        accessPaths1.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_VALUE));
        accessPaths1.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_VALUE));
        group.addAccessPaths(new ComplexTypeAccessPaths(ImmutableList.copyOf(accessPaths1)));

        List<ComplexTypeAccessPath> accessPaths2 = new ArrayList<>();
        accessPaths2.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_VALUE));
        accessPaths2.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_KEY));
        group.addAccessPaths(new ComplexTypeAccessPaths(ImmutableList.copyOf(accessPaths2)));

        PruneComplexTypeUtil.setAccessGroup(typeMapArrayMap, group);
        Assertions.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assertions.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assertions.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assertions.assertEquals(2, mapType.getSelectedFields().length);
        Assertions.assertEquals(true, mapType.getSelectedFields()[0]);
        Assertions.assertEquals(true, mapType.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupMapAllField() {
        // if the last one is a leaf
        ComplexTypeAccessGroup group = new ComplexTypeAccessGroup();
        List<ComplexTypeAccessPath> accessPaths = new ArrayList<>();
        accessPaths.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_VALUE));
        accessPaths.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.ALL_SUBFIELDS));
        group.addAccessPaths(new ComplexTypeAccessPaths(ImmutableList.copyOf(accessPaths)));
        PruneComplexTypeUtil.setAccessGroup(typeMapArrayMap, group);
        Assertions.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assertions.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assertions.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assertions.assertEquals(2, mapType.getSelectedFields().length);
        Assertions.assertEquals(true, mapType.getSelectedFields()[0]);
        Assertions.assertEquals(true, mapType.getSelectedFields()[1]);

        Type newMap = typeMapArrayMap.clone();
        group = new ComplexTypeAccessGroup();
        accessPaths = new ArrayList<>();
        accessPaths.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.ALL_SUBFIELDS));
        group.addAccessPaths(new ComplexTypeAccessPaths(ImmutableList.copyOf(accessPaths)));

        PruneComplexTypeUtil.setAccessGroup(newMap, group);
        Assertions.assertEquals(2, newMap.getSelectedFields().length);
        // the cloned map selected 1, 1
        Assertions.assertEquals(true, newMap.getSelectedFields()[0]);
        Assertions.assertEquals(true, newMap.getSelectedFields()[1]);
        // the origin map selected 0, 1
        Assertions.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assertions.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupMapWrongField() {
        // if the last one is a leaf
        try {
            ComplexTypeAccessGroup group = new ComplexTypeAccessGroup();
            List<ComplexTypeAccessPath> usedSubfieldPos = new ArrayList<>();
            usedSubfieldPos.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_KEY));
            usedSubfieldPos.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_KEY));
            usedSubfieldPos.add(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_KEY));
            group.addAccessPaths(new ComplexTypeAccessPaths(ImmutableList.copyOf(usedSubfieldPos)));
            PruneComplexTypeUtil.setAccessGroup(typeMapArrayMap, group);

        } catch (Exception e) {
            Assertions.fail("Should not throw an exception");
        }
    }

    @Test
    public void testStructSubfield() {
        Type field1 = IntegerType.INT;
        Type field2Map = new MapType(IntegerType.INT,
                VarcharType.VARCHAR);
        Type field2Str = VarcharType.VARCHAR;
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
            PruneComplexTypeUtil.setAccessGroup(cloneType, new ComplexTypeAccessGroup());

            Assertions.assertFalse(cloneType.getSelectedFields()[0]);
            Assertions.assertFalse(cloneType.getSelectedFields()[1]);

            Type tmpType = cloneType.getField(1).getType();
            Assertions.assertFalse(tmpType.getSelectedFields()[0]);
            Assertions.assertFalse(tmpType.getSelectedFields()[1]);

            tmpType = ((StructType) tmpType).getField(0).getType();
            Assertions.assertFalse(tmpType.getSelectedFields()[0]);
            Assertions.assertFalse(tmpType.getSelectedFields()[1]);
        }

        {
            StructType cloneType = topType.clone();
            ComplexTypeAccessGroup group = new ComplexTypeAccessGroup();
            group.addAccessPaths(
                    new ComplexTypeAccessPaths(
                            ImmutableList.of(
                                    new ComplexTypeAccessPath(ComplexTypeAccessPathType.STRUCT_SUBFIELD, "field2"))));
            PruneComplexTypeUtil.setAccessGroup(cloneType, group);

            Assertions.assertFalse(cloneType.getSelectedFields()[0]);
            Assertions.assertTrue(cloneType.getSelectedFields()[1]);

            Type tmpType = cloneType.getField(1).getType();
            Assertions.assertTrue(tmpType.getSelectedFields()[0]);
            Assertions.assertTrue(tmpType.getSelectedFields()[1]);

            tmpType = ((StructType) tmpType).getField(0).getType();
            Assertions.assertTrue(tmpType.getSelectedFields()[0]);
            Assertions.assertTrue(tmpType.getSelectedFields()[1]);
        }

        {
            StructType cloneType = topType.clone();
            ComplexTypeAccessGroup group = new ComplexTypeAccessGroup();
            group.addAccessPaths(new ComplexTypeAccessPaths(
                    ImmutableList.of(
                            new ComplexTypeAccessPath(ComplexTypeAccessPathType.STRUCT_SUBFIELD, "field2"),
                            new ComplexTypeAccessPath(ComplexTypeAccessPathType.STRUCT_SUBFIELD, "subfield2"))));
            PruneComplexTypeUtil.setAccessGroup(cloneType, group);

            Assertions.assertFalse(cloneType.getSelectedFields()[0]);
            Assertions.assertTrue(cloneType.getSelectedFields()[1]);

            Type tmpType = cloneType.getField(1).getType();
            Assertions.assertFalse(tmpType.getSelectedFields()[0]);
            Assertions.assertTrue(tmpType.getSelectedFields()[1]);

            tmpType = ((StructType) tmpType).getField(0).getType();
            Assertions.assertFalse(tmpType.getSelectedFields()[0]);
            Assertions.assertFalse(tmpType.getSelectedFields()[1]);
        }

        {
            StructType cloneType = topType.clone();
            ComplexTypeAccessGroup group = new ComplexTypeAccessGroup();
            group.addAccessPaths(
                    new ComplexTypeAccessPaths(
                            ImmutableList.of(new ComplexTypeAccessPath(ComplexTypeAccessPathType.ALL_SUBFIELDS))));
            PruneComplexTypeUtil.setAccessGroup(cloneType, group);

            Assertions.assertTrue(cloneType.getSelectedFields()[0]);
            Assertions.assertTrue(cloneType.getSelectedFields()[1]);

            Type tmpType = cloneType.getField(1).getType();
            Assertions.assertTrue(tmpType.getSelectedFields()[0]);
            Assertions.assertTrue(tmpType.getSelectedFields()[1]);

            tmpType = ((StructType) tmpType).getField(0).getType();
            Assertions.assertTrue(tmpType.getSelectedFields()[0]);
            Assertions.assertTrue(tmpType.getSelectedFields()[1]);
        }

        {
            StructType cloneType = topType.clone();
            ComplexTypeAccessGroup group = new ComplexTypeAccessGroup();
            group.addAccessPaths(new ComplexTypeAccessPaths(ImmutableList.of()));
            PruneComplexTypeUtil.setAccessGroup(cloneType, group);

            Assertions.assertTrue(cloneType.getSelectedFields()[0]);
            Assertions.assertTrue(cloneType.getSelectedFields()[1]);

            Type tmpType = cloneType.getField(1).getType();
            Assertions.assertTrue(tmpType.getSelectedFields()[0]);
            Assertions.assertTrue(tmpType.getSelectedFields()[1]);

            tmpType = ((StructType) tmpType).getField(0).getType();
            Assertions.assertTrue(tmpType.getSelectedFields()[0]);
            Assertions.assertTrue(tmpType.getSelectedFields()[1]);
        }
    }
}
