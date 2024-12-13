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
<<<<<<< HEAD
import org.junit.Assert;
import org.junit.Test;
=======
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

public class TestColumnType {

    @Test
    public void parsePrimitiveType() {
        String s = "int";
        ColumnType t = new ColumnType(s);
<<<<<<< HEAD
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.INT);
=======
        Assertions.assertEquals(t.getTypeValue(), ColumnType.TypeValue.INT);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void parseArrayType() {
        String s = "array<string>";
        ColumnType t = new ColumnType(s);
<<<<<<< HEAD
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.ARRAY);
        Assert.assertEquals(t.getChildTypes().size(), 1);
        Assert.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
=======
        Assertions.assertEquals(t.getTypeValue(), ColumnType.TypeValue.ARRAY);
        Assertions.assertEquals(t.getChildTypes().size(), 1);
        Assertions.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void parseMapType() {
        String s = "map<int,string>";
        ColumnType t = new ColumnType(s);
<<<<<<< HEAD
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.MAP);
        Assert.assertEquals(t.getChildTypes().size(), 2);
        Assert.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assert.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRING);
=======
        Assertions.assertEquals(t.getTypeValue(), ColumnType.TypeValue.MAP);
        Assertions.assertEquals(t.getChildTypes().size(), 2);
        Assertions.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assertions.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRING);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void parseMapType1() {
        String s = "map<int,struct<a:string,b:array<int>>>";
        ColumnType t = new ColumnType(s);
<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void parseStructType() {
        String s = "struct<a:int,b:string,c:struct<a:int,b:string,c:array<int>>,d:struct<a:array<string>>>";
        ColumnType t = new ColumnType(s);
<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }

    @Test
<<<<<<< HEAD
    public void pruneSturctType() {
=======
    public void pruneStructType() {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        String s = "struct<a:int,b:string,c:struct<a:int,b:string,c:array<int>>,d:struct<a:array<string>>>";
        ColumnType t = new ColumnType(s);
        SelectedFields ssf = new SelectedFields();
        ssf.addMultipleNestedPath("d.a,c.c");

        t.pruneOnSelectedFields(ssf);
<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }
}

