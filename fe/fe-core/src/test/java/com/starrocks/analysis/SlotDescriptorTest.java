package com.starrocks.analysis;

import com.google.common.collect.ImmutableList;
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
import java.util.LinkedList;
import java.util.List;

public class SlotDescriptorTest {

    private Type mapType;
    private Type typeMapArrayMap;
    private SlotDescriptor slot;
    private ColumnRefOperator colRef;

    @Before
    public void setup() {
        Type keyType = ScalarType.createType(PrimitiveType.INT);
        Type valueType = ScalarType.createCharType(10);
        mapType = new MapType(keyType, valueType);
        Type arrayType = new ArrayType(mapType);
        Type keyTypeOuter = ScalarType.createType(PrimitiveType.INT);
        typeMapArrayMap = new MapType(keyTypeOuter, arrayType);
        slot = new SlotDescriptor(SlotId.createGenerator().getNextId(), "type_map_array_map", typeMapArrayMap, true);
        colRef = new ColumnRefOperator(0, typeMapArrayMap, "type_map_array_map", true);
    }

    @Test
    public void testSetUsedSubfieldPosGroupEmpty() {
        // if the usedSubfieldPosGroup is null, all subfields are selected
        slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup());
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
        colRef.addUsedSubfieldPos(ImmutableList.copyOf(usedSubfieldPos));
        slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup());
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
        colRef.addUsedSubfieldPos(ImmutableList.copyOf(usedSubfieldPos));
        slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup());
        Assert.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assert.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assert.assertEquals(2, mapType.getSelectedFields().length);
        Assert.assertEquals(false, mapType.getSelectedFields()[0]);
        Assert.assertEquals(true, mapType.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupTwoPos() {
        List<Integer> usedSubfieldPos1 = Arrays.asList(1, 1);
        colRef.addUsedSubfieldPos(ImmutableList.copyOf(usedSubfieldPos1));
        List<Integer> usedSubfieldPos2 = Arrays.asList(1, 0);
        colRef.addUsedSubfieldPos(ImmutableList.copyOf(usedSubfieldPos2));
        slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup());
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
        List<Integer> usedSubfieldPos = Arrays.asList(1, -1);
        colRef.addUsedSubfieldPos(ImmutableList.copyOf(usedSubfieldPos));
        slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup());
        Assert.assertEquals(2, typeMapArrayMap.getSelectedFields().length);
        Assert.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
        Assert.assertEquals(2, mapType.getSelectedFields().length);
        Assert.assertEquals(true, mapType.getSelectedFields()[0]);
        Assert.assertEquals(true, mapType.getSelectedFields()[1]);

        Type map_new = typeMapArrayMap.clone();
        SlotDescriptor slot_new = new SlotDescriptor(SlotId.createGenerator().getNextId(), "clone_map", map_new, true);
        ColumnRefOperator colRef_new = new ColumnRefOperator(0, map_new, "clone_map", true);
        List<Integer> usedSubfield = Arrays.asList(-1);
        colRef_new.addUsedSubfieldPos(ImmutableList.copyOf(usedSubfield));
        slot_new.setUsedSubfieldPosGroup(colRef_new.getUsedSubfieldPosGroup());
        Assert.assertEquals(2, map_new.getSelectedFields().length);
        // the cloned map selected 1, 1
        Assert.assertEquals(true, map_new.getSelectedFields()[0]);
        Assert.assertEquals(true, map_new.getSelectedFields()[1]);
        // the origin map selected 0, 1
        Assert.assertEquals(false, typeMapArrayMap.getSelectedFields()[0]);
        Assert.assertEquals(true, typeMapArrayMap.getSelectedFields()[1]);
    }

    @Test
    public void testSetUsedSubfieldPosGroupMapWrongField() {
        // if the last one is a leaf
        List<Integer> usedSubfieldPos = Arrays.asList(0, 0, 0);
        colRef.addUsedSubfieldPos(ImmutableList.copyOf(usedSubfieldPos));
        Assert.assertThrows(IllegalStateException.class, ()->slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup()));
    }

    @Test
    public void testStructSubfield() {
        Type field1 = ScalarType.createType(PrimitiveType.INT);
        Type field2Map = new MapType(ScalarType.createType(PrimitiveType.INT), ScalarType.createType(PrimitiveType.VARCHAR));
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
            SlotDescriptor slot = new SlotDescriptor(SlotId.createGenerator().getNextId(),
                    "struct_type", cloneType, true);
            ColumnRefOperator colRef = new ColumnRefOperator(0, cloneType, "struct_type", true);
            colRef.addUsedSubfieldPos(ImmutableList.copyOf(new LinkedList<>()));
            slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup());

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
            SlotDescriptor slot = new SlotDescriptor(SlotId.createGenerator().getNextId(),
                    "struct_type", cloneType, true);
            ColumnRefOperator colRef = new ColumnRefOperator(0, cloneType, "struct_type", true);
            colRef.addUsedSubfieldPos(ImmutableList.copyOf(Arrays.asList(1)));
            slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup());

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
            colRef.addUsedSubfieldPos(ImmutableList.copyOf(Arrays.asList(1, 1)));
            slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup());

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
