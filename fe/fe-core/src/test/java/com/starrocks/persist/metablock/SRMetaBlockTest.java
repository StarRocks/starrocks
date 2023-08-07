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


package com.starrocks.persist.metablock;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SRMetaBlockTest {
    private static Path tmpDir;

    @BeforeClass
    public static void setUp() throws Exception {
        tmpDir = Files.createTempDirectory(Paths.get("."), "SRMetaBlockTest");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(tmpDir.toFile());
    }

    private DataInputStream openInput(String name) throws IOException {
        File file = Paths.get(tmpDir.toFile().getAbsolutePath(), name).toFile();
        return new DataInputStream(Files.newInputStream(file.toPath()));
    }

    private DataOutputStream openOutput(String name) throws IOException {
        File file = Paths.get(tmpDir.toFile().getAbsolutePath(), name).toFile();
        return new DataOutputStream(Files.newOutputStream(file.toPath()));
    }

    static class SimpleStruct {
        @SerializedName(value = "s")
        String str;
        @SerializedName(value = "i")
        int num;
        @SerializedName(value = "l")
        List<String> list;
        static final Type LIST_TYPE = new TypeToken<ArrayList<String>>() {
        }.getType();
        @SerializedName(value = "m")
        Map<String, Integer> map;
        static final Type MAP_TYPE = new TypeToken<HashMap<String, Integer>>() {
        }.getType();
    }

    @Test
    public void testSimple() throws Exception {
        SimpleStruct simpleStruct = new SimpleStruct();
        simpleStruct.str = "lalala";
        simpleStruct.num = 10;
        simpleStruct.list = Arrays.asList("petals", "on", "a", "black", "wet", "bough");
        simpleStruct.map = new HashMap<>();
        simpleStruct.map.put("uno", 1);
        simpleStruct.map.put("dos", 2);

        String name = "simple";
        // will write 5 json
        DataOutputStream dos = openOutput(name);
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, name, 5);
        // 1. String
        writer.writeJson(simpleStruct.str);
        // 2. int
        writer.writeJson(simpleStruct.num);
        // 3. list
        writer.writeJson(simpleStruct.list);
        // 4. map
        writer.writeJson(simpleStruct.map);
        // 5. all
        writer.writeJson(simpleStruct);
        writer.close();
        dos.close();

        DataInputStream dis = openInput(name);
        SRMetaBlockReader reader = new SRMetaBlockReader(dis, name);

        // 1. String
        String retStr = reader.readJson(String.class);
        Assert.assertEquals(simpleStruct.str, retStr);
        // 2. int
        Integer retInt = reader.readJson(Integer.class);
        Assert.assertEquals(simpleStruct.num, retInt.intValue());
        // 3. list
        List<String> retList = (List<String>) reader.readJson(SimpleStruct.LIST_TYPE);
        Assert.assertEquals(simpleStruct.list.size(), retList.size());
        for (int i = 0; i != simpleStruct.list.size(); ++i) {
            Assert.assertEquals(simpleStruct.list.get(i), retList.get(i));
        }
        // 4. map
        Map<String, Integer> retMap = (Map<String, Integer>) reader.readJson(SimpleStruct.MAP_TYPE);
        Assert.assertEquals(simpleStruct.map.size(), retMap.size());
        for (String k : simpleStruct.map.keySet()) {
            Assert.assertEquals(simpleStruct.map.get(k), retMap.get(k));
        }
        // 5. all
        SimpleStruct ret = reader.readJson(SimpleStruct.class);
        Assert.assertEquals(simpleStruct.str, ret.str);
        Assert.assertEquals(simpleStruct.num, ret.num);
        Assert.assertEquals(simpleStruct.list.size(), ret.list.size());
        for (int i = 0; i != simpleStruct.list.size(); ++i) {
            Assert.assertEquals(simpleStruct.list.get(i), ret.list.get(i));
        }
        Assert.assertEquals(simpleStruct.map.size(), ret.map.size());
        for (String k : simpleStruct.map.keySet()) {
            Assert.assertEquals(simpleStruct.map.get(k), ret.map.get(k));
        }

        reader.close();
        dis.close();
    }

    @Test
    public void testMultiBlock() throws Exception {
        SimpleStruct simpleStruct = new SimpleStruct();
        simpleStruct.str = "lalala";
        simpleStruct.num = 10;
        simpleStruct.list = Arrays.asList("petals", "on", "a", "black", "wet", "bough");
        simpleStruct.map = new HashMap<>();
        simpleStruct.map.put("uno", 1);
        simpleStruct.map.put("dos", 2);
        String block1 = "block1";
        String block2 = "block2";
        String name = "multiblock";

        //
        // start to write 2 block
        //
        DataOutputStream dos = openOutput(name);

        // first block, 4 json
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, block1, 4);
        // 1. String
        writer.writeJson(simpleStruct.str);
        // 2. int
        writer.writeJson(simpleStruct.num);
        // 3. list
        writer.writeJson(simpleStruct.list);
        // 4. map
        writer.writeJson(simpleStruct.map);
        writer.close();

        // second block, 1 json
        writer = new SRMetaBlockWriter(dos, block2, 1);
        writer.writeJson(simpleStruct);
        writer.close();

        dos.close();

        //
        // start to read 2 blocks
        //

        DataInputStream dis = openInput(name);

        // first block, 4 json
        SRMetaBlockReader reader = new SRMetaBlockReader(dis, block1);
        // 1. String
        String retStr = reader.readJson(String.class);
        Assert.assertEquals(simpleStruct.str, retStr);
        // 2. int
        Integer retInt = reader.readJson(Integer.class);
        Assert.assertEquals(simpleStruct.num, retInt.intValue());
        // 3. list
        List<String> retList = (List<String>) reader.readJson(SimpleStruct.LIST_TYPE);
        Assert.assertEquals(simpleStruct.list.size(), retList.size());
        for (int i = 0; i != simpleStruct.list.size(); ++i) {
            Assert.assertEquals(simpleStruct.list.get(i), retList.get(i));
        }
        // 4. map
        Map<String, Integer> retMap = (Map<String, Integer>) reader.readJson(SimpleStruct.MAP_TYPE);
        Assert.assertEquals(simpleStruct.map.size(), retMap.size());
        for (String k : simpleStruct.map.keySet()) {
            Assert.assertEquals(simpleStruct.map.get(k), retMap.get(k));
        }
        reader.close();

        // second block, 1 json
        reader = new SRMetaBlockReader(dis, block2);
        SimpleStruct ret = reader.readJson(SimpleStruct.class);
        Assert.assertEquals(simpleStruct.str, ret.str);
        Assert.assertEquals(simpleStruct.num, ret.num);
        Assert.assertEquals(simpleStruct.list.size(), ret.list.size());
        for (int i = 0; i != simpleStruct.list.size(); ++i) {
            Assert.assertEquals(simpleStruct.list.get(i), ret.list.get(i));
        }
        Assert.assertEquals(simpleStruct.map.size(), ret.map.size());
        for (String k : simpleStruct.map.keySet()) {
            Assert.assertEquals(simpleStruct.map.get(k), ret.map.get(k));
        }
        reader.close();

        dis.close();

        //
        // read 2 blocks, but the first one only read a string
        //
        dis = openInput(name);

        // first block, only read 1 string
        reader = new SRMetaBlockReader(dis, block1);
        // 1. String
        retStr = reader.readJson(String.class);
        Assert.assertEquals(simpleStruct.str, retStr);
        // skip the rest
        reader.close();

        // second block, 1 json
        reader = new SRMetaBlockReader(dis, block2);
        ret = reader.readJson(SimpleStruct.class);
        Assert.assertEquals(simpleStruct.str, ret.str);
        Assert.assertEquals(simpleStruct.num, ret.num);
        Assert.assertEquals(simpleStruct.list.size(), ret.list.size());
        for (int i = 0; i != simpleStruct.list.size(); ++i) {
            Assert.assertEquals(simpleStruct.list.get(i), ret.list.get(i));
        }
        Assert.assertEquals(simpleStruct.map.size(), ret.map.size());
        for (String k : simpleStruct.map.keySet()) {
            Assert.assertEquals(simpleStruct.map.get(k), ret.map.get(k));
        }
        reader.close();

        dis.close();

    }

    @Test
    public void testWriteBadBlock0Json() throws Exception {
        String name = "badWriter0Json";
        DataOutputStream dos = openOutput(name);
        // write 0 json
        try {
            new SRMetaBlockWriter(dos, name, 0);
            Assert.fail();
        } catch (SRMetaBlockException e) {
            Assert.assertTrue(e.getMessage().contains("invalid numJson: 0"));
        }
    }

    @Test
    public void testWriteBadBlockMoreJson() throws Exception {
        String name = "badWriterMoreJson";
        DataOutputStream dos = openOutput(name);
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, name, 1);
        writer.writeJson("xxx");
        // write more json than declared
        try {
            writer.writeJson("won't write!");
            Assert.fail();
        } catch (SRMetaBlockException e) {
            Assert.assertTrue(e.getMessage().contains("About to write json more than"));
        }
    }

    @Test
    public void testWriteBadBlockLessJson() throws Exception {
        String name = "badWriterLessJson";
        DataOutputStream dos = openOutput(name);
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, name, 2);
        writer.writeJson("xxx");
        // write less json than declared
        try {
            writer.close();
            Assert.fail();
        } catch (SRMetaBlockException e) {
            Assert.assertTrue(e.getMessage().contains("Block json number mismatch: expect 2 actual 1"));
        }
    }

    private void write2String(String name) throws Exception {
        DataOutputStream dos = openOutput(name);
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, name, 2);
        writer.writeJson("Muchos años después");
        writer.writeJson("frente al pelotón de fusilamiento");
        writer.close();
        dos.close();
    }

    @Test
    public void testReadBadBlockBadID() throws Exception {
        DataOutputStream dos = openOutput("blockid");
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.CATALOG_MGR, 2);
        writer.writeJson("Muchos años después");
        writer.writeJson("frente al pelotón de fusilamiento");
        writer.close();
        dos.close();

        DataInputStream dis = openInput("blockid");
        SRMetaBlockReader reader = new SRMetaBlockReader(dis);
        Assert.assertEquals(reader.getHeader().getSrMetaBlockID(), SRMetaBlockID.CATALOG_MGR);
    }

    @Test
    public void testReadBadBlockMoreJson() throws Exception {
        String name = "badReaderMoreJson";
        write2String(name);
        // read more than expect
        DataInputStream dis = openInput(name);
        SRMetaBlockReader reader = new SRMetaBlockReader(dis, name);
        reader.readJson(String.class);
        reader.readJson(String.class);
        try {
            reader.readJson(String.class);
            Assert.fail();
        } catch (SRMetaBlockEOFException e) {
            Assert.assertTrue(e.getMessage().contains("Read json more than expect: 2 >= 2"));
        } finally {
            dis.close();
        }
    }

    @Test
    public void emptyFile() throws Exception {
        String name = "empty";
        DataOutputStream dos = openOutput(name);
        // only 1 block
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, name, 1);
        writer.writeJson("Muchos años después");
        writer.close();
        dos.close();

        DataInputStream dis = openInput(name);
        // first block, everything's fine
        SRMetaBlockReader reader = new SRMetaBlockReader(dis, name);
        reader.readJson(String.class);
        reader.close();
        // second block, get EOF

        try {
            reader = new SRMetaBlockReader(dis, "xxx");
            reader.readJson(String.class);
            Assert.fail();
        } catch (EOFException exception) {
            Assert.assertTrue(true);
        }
        dis.close();
    }
}
