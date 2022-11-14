package com.starrocks.analysis;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
        colRef.addUsedSubfieldPos(usedSubfieldPos);
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
        colRef.addUsedSubfieldPos(usedSubfieldPos);
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
        colRef.addUsedSubfieldPos(usedSubfieldPos1);
        List<Integer> usedSubfieldPos2 = Arrays.asList(1, 0);
        colRef.addUsedSubfieldPos(usedSubfieldPos2);
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
        colRef.addUsedSubfieldPos(usedSubfieldPos);
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
        colRef_new.addUsedSubfieldPos(usedSubfield);
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
        colRef.addUsedSubfieldPos(usedSubfieldPos);
        Assert.assertThrows(IllegalStateException.class, ()->slot.setUsedSubfieldPosGroup(colRef.getUsedSubfieldPosGroup()));
    }

}
