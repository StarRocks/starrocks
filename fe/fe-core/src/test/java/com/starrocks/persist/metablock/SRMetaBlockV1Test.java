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
import com.google.gson.stream.JsonReader;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.utframe.UtFrameUtils;
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
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SRMetaBlockV1Test {
    private static Path tmpDir;

    @BeforeClass
    public static void setUp() throws Exception {
        tmpDir = Files.createTempDirectory(Paths.get("."), "SRMetaBlockV1Test");
        UtFrameUtils.PseudoImage.setUpImageVersion();
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
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(dos, SRMetaBlockID.RESOURCE_MGR, 5);
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
        SRMetaBlockReader reader = new SRMetaBlockReaderV1(dis);

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
        String name = "multiblock";

        //
        // start to write 2 block
        //
        DataOutputStream dos = openOutput(name);

        // first block, 4 json
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(dos, SRMetaBlockID.RESOURCE_MGR, 4);
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
        writer = new SRMetaBlockWriterV1(dos, SRMetaBlockID.TASK_MGR, 1);
        writer.writeJson(simpleStruct);
        writer.close();

        dos.close();

        //
        // start to read 2 blocks
        //

        DataInputStream dis = openInput(name);
        JsonReader jsonReader = new JsonReader(new InputStreamReader(dis));

        // first block, 4 json
        SRMetaBlockReader reader = new SRMetaBlockReaderV1(dis);
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
        reader = new SRMetaBlockReaderV1(dis);
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
        reader = new SRMetaBlockReaderV1(dis);
        // 1. String
        retStr = reader.readJson(String.class);
        Assert.assertEquals(simpleStruct.str, retStr);
        // skip the rest
        reader.close();

        // second block, 1 json
        reader = new SRMetaBlockReaderV1(dis);
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
            new SRMetaBlockWriterV1(dos, SRMetaBlockID.TASK_MGR, 0);
            Assert.fail();
        } catch (SRMetaBlockException e) {
            Assert.assertTrue(e.getMessage().contains("invalid numJson: 0"));
        }
    }

    @Test
    public void testWriteBadBlockMoreJson() throws Exception {
        String name = "badWriterMoreJson";
        DataOutputStream dos = openOutput(name);
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(dos, SRMetaBlockID.TASK_MGR, 1);
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
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(dos, SRMetaBlockID.TASK_MGR, 2);
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
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(dos, SRMetaBlockID.TASK_MGR, 2);
        writer.writeJson("Muchos años después");
        writer.writeJson("frente al pelotón de fusilamiento");
        writer.close();
        dos.close();
    }

    @Test
    public void testReadBadBlockBadID() throws Exception {
        DataOutputStream dos = openOutput("blockid");
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(dos, SRMetaBlockID.CATALOG_MGR, 2);
        writer.writeJson("Muchos años después");
        writer.writeJson("frente al pelotón de fusilamiento");
        writer.close();
        dos.close();

        DataInputStream dis = openInput("blockid");
        SRMetaBlockReader reader = new SRMetaBlockReaderV1(dis);
        Assert.assertEquals(reader.getHeader().getSrMetaBlockID(), SRMetaBlockID.CATALOG_MGR);
    }

    @Test
    public void testReadBadBlockMoreJson() throws Exception {
        String name = "badReaderMoreJson";
        write2String(name);
        // read more than expect
        DataInputStream dis = openInput(name);
        SRMetaBlockReader reader = new SRMetaBlockReaderV1(dis);
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
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(dos, SRMetaBlockID.TASK_MGR, 1);
        writer.writeJson("Muchos años después");
        writer.close();
        dos.close();

        DataInputStream dis = openInput(name);
        // first block, everything's fine
        SRMetaBlockReader reader = new SRMetaBlockReaderV1(dis);
        reader.readJson(String.class);
        reader.close();
        // second block, get EOF

        try {
            reader = new SRMetaBlockReaderV1(dis);
            reader.readJson(String.class);
            Assert.fail();
        } catch (EOFException exception) {
            Assert.assertTrue(true);
        }
        dis.close();
    }

    public static class TestTable {
        @SerializedName("clazz")
        private String clazz = "unknownTable";

        public TestTable() {
        }
    }

    @Test
    public void testSubTypeRollbackInReadCollection() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(image.getDataOutputStream(),
                SRMetaBlockID.LOCAL_META_STORE, 5);
        writer.writeInt(4);
        writer.writeJson(new MysqlTable());
        writer.writeJson(new HiveTable());
        writer.writeJson(new JDBCTable());
        writer.writeJson(new TestTable());
        writer.close();

        final List<Table> tables = new ArrayList<>();
        final SRMetaBlockReader reader = new SRMetaBlockReaderV1(image.getDataInputStream());
        Assert.assertThrows(SubtypeNotFoundException.class,
                () -> reader.readCollection(Table.class, tables::add));

        List<Table> tables2 = new ArrayList<>();
        SRMetaBlockReader reader2 = new SRMetaBlockReaderV1(image.getDataInputStream());
        Config.metadata_ignore_unknown_subtype = true;
        try {
            reader2.readCollection(Table.class, tables2::add);
            reader2.close();
        } finally {
            Config.metadata_ignore_unknown_subtype = false;
        }

        Assert.assertEquals(3, tables2.size());
        Assert.assertTrue(tables2.get(0) instanceof MysqlTable);
        Assert.assertTrue(tables2.get(1) instanceof HiveTable);
        Assert.assertTrue(tables2.get(2) instanceof JDBCTable);
    }

    @Test
    public void testSubTypeRollbackInMapEntry() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(image.getDataOutputStream(),
                SRMetaBlockID.LOCAL_META_STORE, 9);
        writer.writeInt(4);
        writer.writeLong(1);
        writer.writeJson(new MysqlTable());
        writer.writeLong(2);
        writer.writeJson(new HiveTable());
        writer.writeLong(3);
        writer.writeJson(new JDBCTable());
        writer.writeLong(4);
        writer.writeJson(new TestTable());
        writer.close();

        final Map<Long, Table> tables = new HashMap<>();
        final SRMetaBlockReader reader = new SRMetaBlockReaderV1(image.getDataInputStream());
        Assert.assertThrows(SubtypeNotFoundException.class,
                () -> reader.readMap(Long.class, Table.class, tables::put));

        Map<Long, Table> tables2 = new HashMap<>();
        SRMetaBlockReader reader2 = new SRMetaBlockReaderV1(image.getDataInputStream());
        Config.metadata_ignore_unknown_subtype = true;
        try {
            reader2.readMap(Long.class, Table.class, tables2::put);
            reader2.close();
        } finally {
            Config.metadata_ignore_unknown_subtype = false;
        }

        Assert.assertEquals(3, tables2.size());
        Assert.assertTrue(tables2.get(1L) instanceof MysqlTable);
        Assert.assertTrue(tables2.get(2L) instanceof HiveTable);
        Assert.assertTrue(tables2.get(3L) instanceof JDBCTable);
    }

    @Test
    public void testPrimitiveType() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        SRMetaBlockWriter writer = new SRMetaBlockWriterV1(image.getDataOutputStream(),
                SRMetaBlockID.LOCAL_META_STORE, 9);
        writer.writeByte((byte) 1);
        writer.writeString("xxx");
        writer.writeInt(3);
        writer.writeLong(4L);
        writer.writeBoolean(false);
        writer.writeChar('a');
        writer.writeDouble(3.3);
        writer.writeFloat(3.4f);
        writer.writeShort((short) 6);
        writer.close();

        final SRMetaBlockReader reader = new SRMetaBlockReaderV1(image.getDataInputStream());
        Assert.assertEquals((byte) 1, reader.readByte());
        Assert.assertEquals("xxx", reader.readString());
        Assert.assertEquals(3, reader.readInt());
        Assert.assertEquals(4L, reader.readLong());
        Assert.assertFalse(reader.readBoolean());
        Assert.assertEquals('a', reader.readChar());
        Assert.assertTrue(Math.abs(reader.readDouble() - 3.3) < 1e-6);
        Assert.assertTrue(Math.abs(reader.readFloat() - 3.4f) < 1e-6);
        Assert.assertEquals((short) 6, reader.readShort());
        reader.close();
    }
}
