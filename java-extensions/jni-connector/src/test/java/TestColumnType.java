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
import org.junit.Assert;
import org.junit.Test;

public class TestColumnType {

    @Test
    public void parsePrimitiveType() {
        String s = "int";
        ColumnType t = new ColumnType(s);
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.INT);
    }

    @Test
    public void parseArrayType() {
        String s = "array<string>";
        ColumnType t = new ColumnType(s);
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.ARRAY);
        Assert.assertEquals(t.getChildTypes().size(), 1);
        Assert.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
    }

    @Test
    public void parseMapType() {
        String s = "map<int,string>";
        ColumnType t = new ColumnType(s);
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.MAP);
        Assert.assertEquals(t.getChildTypes().size(), 2);
        Assert.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assert.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRING);
    }

    @Test
    public void parseMapType1() {
        String s = "map<int,struct<a:string,b:array<int>>>";
        ColumnType t = new ColumnType(s);
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.MAP);
        Assert.assertEquals(t.getChildTypes().size(), 2);
        Assert.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assert.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRUCT);

        ColumnType c = t.getChildTypes().get(1);
        Assert.assertEquals(c.getChildTypes().size(), 2);
        Assert.assertEquals(c.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
        ColumnType c2 = c.getChildTypes().get(1);
        Assert.assertTrue(c2.isArray());
        Assert.assertEquals(c2.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
    }

    @Test
    public void parseStructType() {
        String s = "struct<a:int,b:string,c:struct<a:int,b:string,c:array<int>>,d:struct<a:array<string>>>";
        ColumnType t = new ColumnType(s);
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.STRUCT);
        Assert.assertEquals(t.getChildTypes().size(), 4);
        Assert.assertEquals(t.getChildNames().get(3), "d");
        Assert.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assert.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRING);
        Assert.assertEquals(t.getChildTypes().get(2).getTypeValue(), ColumnType.TypeValue.STRUCT);
        Assert.assertEquals(t.getChildTypes().get(3).getTypeValue(), ColumnType.TypeValue.STRUCT);
        {
            ColumnType c = t.getChildTypes().get(2);
            Assert.assertEquals(c.getChildTypes().size(), 3);
            Assert.assertEquals(c.getChildNames().get(2), "c");
            Assert.assertEquals(c.getChildTypes().get(2).getTypeValue(), ColumnType.TypeValue.ARRAY);
            ColumnType c2 = c.getChildTypes().get(2);
            Assert.assertEquals(c2.getChildTypes().size(), 1);
            Assert.assertEquals(c2.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        }
        {
            ColumnType c = t.getChildTypes().get(3);
            Assert.assertEquals(c.getChildTypes().size(), 1);
            Assert.assertEquals(c.getChildNames().get(0), "a");
            Assert.assertEquals(c.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.ARRAY);
            ColumnType c2 = c.getChildTypes().get(0);
            Assert.assertEquals(c2.getChildTypes().size(), 1);
            Assert.assertEquals(c2.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
        }
    }

    @Test
    public void pruneSturctType() {
        String s = "struct<a:int,b:string,c:struct<a:int,b:string,c:array<int>>,d:struct<a:array<string>>>";
        ColumnType t = new ColumnType(s);
        SelectedFields ssf = new SelectedFields();
        ssf.addMultipleNestedPath("d.a,c.c");

        t.pruneOnSelectedFields(ssf);
        Assert.assertTrue(t.isStruct());
        Assert.assertEquals(t.getChildTypes().size(), 2);
        Assert.assertEquals(String.join(",", t.getChildNames()), "d,c");
        {
            ColumnType d = t.getChildTypes().get(0);
            Assert.assertTrue(d.isStruct());
            Assert.assertEquals(d.getChildNames().size(), 1);
        }
        {
            ColumnType c = t.getChildTypes().get(1);
            Assert.assertTrue(c.isStruct());
            Assert.assertEquals(c.getChildNames().size(), 1);
            Assert.assertEquals(c.getChildNames().get(0), "c");
            Assert.assertTrue(c.getChildTypes().get(0).isArray());
        }
    }
}

