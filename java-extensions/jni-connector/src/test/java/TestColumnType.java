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

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.SelectedFields;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TestColumnType {

    @Test
    public void parsePrimitiveType() {
        String s = "int";
        ColumnType t = new ColumnType(s);
        Assertions.assertEquals(t.getTypeValue(), ColumnType.TypeValue.INT);
    }

    @Test
    public void parseArrayType() {
        String s = "array<string>";
        ColumnType t = new ColumnType(s);
        Assertions.assertEquals(t.getTypeValue(), ColumnType.TypeValue.ARRAY);
        Assertions.assertEquals(t.getChildTypes().size(), 1);
        Assertions.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
    }

    @Test
    public void parseMapType() {
        String s = "map<int,string>";
        ColumnType t = new ColumnType(s);
        Assertions.assertEquals(t.getTypeValue(), ColumnType.TypeValue.MAP);
        Assertions.assertEquals(t.getChildTypes().size(), 2);
        Assertions.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assertions.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRING);
    }

    @Test
    public void parseMapType1() {
        String s = "map<int,struct<a:string,b:array<int>>>";
        ColumnType t = new ColumnType(s);
        Assertions.assertEquals(t.getTypeValue(), ColumnType.TypeValue.MAP);
        Assertions.assertEquals(t.getChildTypes().size(), 2);
        Assertions.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assertions.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRUCT);

        ColumnType c = t.getChildTypes().get(1);
        Assertions.assertEquals(c.getChildTypes().size(), 2);
        Assertions.assertEquals(c.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
        ColumnType c2 = c.getChildTypes().get(1);
        Assertions.assertTrue(c2.isArray());
        Assertions.assertEquals(c2.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
    }

    @Test
    public void parseStructType() {
        String s = "struct<a:int,b:string,c:struct<a:int,b:string,c:array<int>>,d:struct<a:array<string>>>";
        ColumnType t = new ColumnType(s);
        Assertions.assertEquals(t.getTypeValue(), ColumnType.TypeValue.STRUCT);
        Assertions.assertEquals(t.getChildTypes().size(), 4);
        Assertions.assertEquals(t.getChildNames().get(3), "d");
        Assertions.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assertions.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRING);
        Assertions.assertEquals(t.getChildTypes().get(2).getTypeValue(), ColumnType.TypeValue.STRUCT);
        Assertions.assertEquals(t.getChildTypes().get(3).getTypeValue(), ColumnType.TypeValue.STRUCT);
        {
            ColumnType c = t.getChildTypes().get(2);
            Assertions.assertEquals(c.getChildTypes().size(), 3);
            Assertions.assertEquals(c.getChildNames().get(2), "c");
            Assertions.assertEquals(c.getChildTypes().get(2).getTypeValue(), ColumnType.TypeValue.ARRAY);
            ColumnType c2 = c.getChildTypes().get(2);
            Assertions.assertEquals(c2.getChildTypes().size(), 1);
            Assertions.assertEquals(c2.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        }
        {
            ColumnType c = t.getChildTypes().get(3);
            Assertions.assertEquals(c.getChildTypes().size(), 1);
            Assertions.assertEquals(c.getChildNames().get(0), "a");
            Assertions.assertEquals(c.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.ARRAY);
            ColumnType c2 = c.getChildTypes().get(0);
            Assertions.assertEquals(c2.getChildTypes().size(), 1);
            Assertions.assertEquals(c2.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
        }
    }

    @Test
    public void parseVariantType() {
        ColumnType type = new ColumnType("v", "variant");
        Assertions.assertEquals(ColumnType.TypeValue.VARIANT, type.getTypeValue());
        Assertions.assertTrue(type.isVariant());
        Assertions.assertEquals(Arrays.asList("metadata", "value"), type.getChildNames());
        Assertions.assertEquals(ColumnType.TypeValue.BINARY, type.getChildTypes().get(0).getTypeValue());
        Assertions.assertEquals(ColumnType.TypeValue.BINARY, type.getChildTypes().get(1).getTypeValue());
        Assertions.assertEquals(Arrays.asList(0, 1), type.getFieldIndex());
        // variant column meta: [null] + 2 binary children, each [null | offset | data] => 1 + 3 + 3
        Assertions.assertEquals(7, type.computeColumnSize());
        Assertions.assertEquals("variant", type.getTypeValueString());
    }

    @Test
    public void parseNestedVariantType() {
        // Guards the trailing-token parser trait: a variant child's fixed metadata/value children
        // must parse correctly even when a sibling field follows it in the enclosing struct.
        String s = "struct<v:variant,i:int>";
        ColumnType t = new ColumnType("s", s);
        Assertions.assertEquals(ColumnType.TypeValue.STRUCT, t.getTypeValue());
        Assertions.assertEquals(2, t.getChildTypes().size());
        Assertions.assertEquals(Arrays.asList("v", "i"), t.getChildNames());

        ColumnType v = t.getChildTypes().get(0);
        Assertions.assertEquals(ColumnType.TypeValue.VARIANT, v.getTypeValue());
        Assertions.assertTrue(v.isVariant());
        Assertions.assertEquals(Arrays.asList("metadata", "value"), v.getChildNames());
        Assertions.assertEquals(ColumnType.TypeValue.BINARY, v.getChildTypes().get(0).getTypeValue());
        Assertions.assertEquals(ColumnType.TypeValue.BINARY, v.getChildTypes().get(1).getTypeValue());

        ColumnType i = t.getChildTypes().get(1);
        Assertions.assertEquals(ColumnType.TypeValue.INT, i.getTypeValue());
    }

    @Test
    public void pruneStructType() {
        String s = "struct<a:int,b:string,c:struct<a:int,b:string,c:array<int>>,d:struct<a:array<string>>>";
        ColumnType t = new ColumnType(s);
        SelectedFields ssf = new SelectedFields();
        ssf.addMultipleNestedPath("d.a,c.c");

        t.pruneOnSelectedFields(ssf);
        Assertions.assertTrue(t.isStruct());
        Assertions.assertEquals(t.getChildTypes().size(), 2);
        Assertions.assertEquals(String.join(",", t.getChildNames()), "d,c");
        {
            ColumnType d = t.getChildTypes().get(0);
            Assertions.assertTrue(d.isStruct());
            Assertions.assertEquals(d.getChildNames().size(), 1);
        }
        {
            ColumnType c = t.getChildTypes().get(1);
            Assertions.assertTrue(c.isStruct());
            Assertions.assertEquals(c.getChildNames().size(), 1);
            Assertions.assertEquals(c.getChildNames().get(0), "c");
            Assertions.assertTrue(c.getChildTypes().get(0).isArray());
        }
    }
}

