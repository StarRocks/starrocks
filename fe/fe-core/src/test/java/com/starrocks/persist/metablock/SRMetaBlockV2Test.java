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
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SRMetaBlockV2Test {

    private static Path tmpDir;

    @BeforeClass
    public static void setUp() throws Exception {
        tmpDir = Files.createTempDirectory(Paths.get("."), "SRMetaBlockV2Test");
        UtFrameUtils.PseudoImage.setUpImageVersion();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(tmpDir.toFile());
    }

    private InputStream openInput(String name) throws IOException {
        return Files.newInputStream(Paths.get(tmpDir.toFile().getAbsolutePath(), name));
    }

    private OutputStream openOutput(String name) throws IOException {
        return Files.newOutputStream(Paths.get(tmpDir.toFile().getAbsolutePath(), name));
    }

    static class SimpleObject {
        @SerializedName("name")
        String name;

        @SerializedName("value")
        int value;

        public SimpleObject(String name, int value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SimpleObject)) {
                return false;
            }

            SimpleObject sb = (SimpleObject) obj;

            return name.equals(sb.name) && value == sb.value;
        }
    }

    @Test
    public void testSimple() throws Exception {
        String fileName = "simple";

        // write 5 json
        OutputStream out = openOutput(fileName);
        SRMetaBlockWriter writer = new SRMetaBlockWriterV2(new JsonWriter(new OutputStreamWriter(out)),
                SRMetaBlockID.RESOURCE_MGR, 6);
        writer.writeInt(5);
        writer.writeJson(new SimpleObject("n1", 1));
        writer.writeJson(new SimpleObject("n2", 2));
        writer.writeJson(new SimpleObject("n3", 3));
        writer.writeJson(new SimpleObject("n4", 4));
        writer.writeJson(new SimpleObject("n5", 5));
        writer.close();
        out.close();

        InputStream in = openInput(fileName);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(new JsonReader(new InputStreamReader(in)));
        Assert.assertEquals(SRMetaBlockID.RESOURCE_MGR, reader.getHeader().getSrMetaBlockID());
        Assert.assertEquals(5, reader.readInt());
        Assert.assertEquals(new SimpleObject("n1", 1), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n2", 2), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n3", 3), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n4", 4), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n5", 5), reader.readJson(SimpleObject.class));
        Assert.assertThrows(SRMetaBlockEOFException.class, () -> reader.readJson(SimpleObject.class));
        reader.close();
        in.close();
    }

    @Test
    public void testMultiBlock() throws Exception {
        String fileName = "multi_block";
        OutputStream out = openOutput(fileName);
        JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(out));

        // write block1
        SRMetaBlockWriter writer = new SRMetaBlockWriterV2(jsonWriter, SRMetaBlockID.RESOURCE_MGR, 2);
        writer.writeJson(new SimpleObject("n1", 1));
        writer.writeJson(new SimpleObject("n2", 2));
        writer.close();

        // write block2
        writer = new SRMetaBlockWriterV2(jsonWriter, SRMetaBlockID.TASK_MGR, 4);
        writer.writeJson(new SimpleObject("n1", 1));
        writer.writeJson(new SimpleObject("n2", 2));
        writer.writeJson(new SimpleObject("n3", 3));
        writer.writeJson(new SimpleObject("n4", 4));
        writer.close();

        out.close();

        // ======= normal read =======
        InputStream in = openInput(fileName);
        JsonReader jsonReader = new JsonReader(new InputStreamReader(in));
        //read block1
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(jsonReader);
        Assert.assertEquals(SRMetaBlockID.RESOURCE_MGR, reader.getHeader().getSrMetaBlockID());
        Assert.assertEquals(new SimpleObject("n1", 1), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n2", 2), reader.readJson(SimpleObject.class));
        reader.close();

        //read block2
        reader = new SRMetaBlockReaderV2(jsonReader);
        Assert.assertEquals(SRMetaBlockID.TASK_MGR, reader.getHeader().getSrMetaBlockID());
        Assert.assertEquals(new SimpleObject("n1", 1), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n2", 2), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n3", 3), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n4", 4), reader.readJson(SimpleObject.class));
        reader.close();

        in.close();

        // ======= skip read ========
        in = openInput(fileName);
        jsonReader = new JsonReader(new InputStreamReader(in));

        // read block1, but only read 1 json, close() will skip the rest
        reader = new SRMetaBlockReaderV2(jsonReader);
        Assert.assertEquals(SRMetaBlockID.RESOURCE_MGR, reader.getHeader().getSrMetaBlockID());
        Assert.assertEquals(new SimpleObject("n1", 1), reader.readJson(SimpleObject.class));
        reader.close();

        // read block2, and read more object, SRMetaBlockEOFException will be thrown
        reader = new SRMetaBlockReaderV2(jsonReader);
        Assert.assertEquals(SRMetaBlockID.TASK_MGR, reader.getHeader().getSrMetaBlockID());
        Assert.assertEquals(new SimpleObject("n1", 1), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n2", 2), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n3", 3), reader.readJson(SimpleObject.class));
        Assert.assertEquals(new SimpleObject("n4", 4), reader.readJson(SimpleObject.class));
        SRMetaBlockReader finalReader = reader;
        Assert.assertThrows(SRMetaBlockEOFException.class, () -> finalReader.readJson(SimpleObject.class));
        reader.close();

        // read block3, and it does not exist, EOFException will be thrown
        JsonReader finalJsonReader = jsonReader;
        Assert.assertThrows(EOFException.class, () -> new SRMetaBlockReaderV2(finalJsonReader));
        in.close();
    }

    @Test
    public void testWriteBadBlock0Json() throws Exception {
        String name = "badWriter0Json";
        OutputStream out = openOutput(name);
        // write 0 json
        try {
            new SRMetaBlockWriterV2(new JsonWriter(new OutputStreamWriter(out)), SRMetaBlockID.TASK_MGR, 0);
            Assert.fail();
        } catch (SRMetaBlockException e) {
            Assert.assertTrue(e.getMessage().contains("invalid numJson: 0"));
        }
    }

    @Test
    public void testWriteBadBlockMoreJson() throws Exception {
        String name = "badWriterMoreJson";
        OutputStream dos = openOutput(name);
        SRMetaBlockWriter writer = new SRMetaBlockWriterV2(new JsonWriter(new OutputStreamWriter(dos)),
                SRMetaBlockID.TASK_MGR, 1);
        writer.writeJson(new SimpleObject("n1", 1));
        // write more json than declared
        try {
            writer.writeJson(new SimpleObject("n2", 2));
            Assert.fail();
        } catch (SRMetaBlockException e) {
            Assert.assertTrue(e.getMessage().contains("About to write json more than"));
        }
    }

    @Test
    public void testWriteBadBlockLessJson() throws Exception {
        String name = "badWriterLessJson";
        OutputStream dos = openOutput(name);
        SRMetaBlockWriter writer = new SRMetaBlockWriterV2(new JsonWriter(new OutputStreamWriter(dos)),
                SRMetaBlockID.TASK_MGR, 2);
        writer.writeJson(new SimpleObject("n1", 1));
        // write less json than declared
        try {
            writer.close();
            Assert.fail();
        } catch (SRMetaBlockException e) {
            Assert.assertTrue(e.getMessage().contains("Block json number mismatch: expect 2 actual 1"));
        }
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
        SRMetaBlockWriter writer = new SRMetaBlockWriterV2(
                new JsonWriter(new OutputStreamWriter(image.getDataOutputStream())),
                SRMetaBlockID.LOCAL_META_STORE, 5);
        writer.writeInt(4);
        writer.writeJson(new MysqlTable());
        writer.writeJson(new HiveTable());
        writer.writeJson(new JDBCTable());
        writer.writeJson(new TestTable());
        writer.close();

        final List<Table> tables = new ArrayList<>();
        final SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        Assert.assertThrows(SubtypeNotFoundException.class,
                () -> reader.readCollection(Table.class, tables::add));

        List<Table> tables2 = new ArrayList<>();
        SRMetaBlockReader reader2 =  new SRMetaBlockReaderV2(image.getJsonReader());
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
        SRMetaBlockWriter writer = new SRMetaBlockWriterV2(
                new JsonWriter(new OutputStreamWriter(image.getDataOutputStream())),
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
        final SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        Assert.assertThrows(SubtypeNotFoundException.class,
                () -> reader.readMap(Long.class, Table.class, tables::put));

        Map<Long, Table> tables2 = new HashMap<>();
        SRMetaBlockReader reader2 = new SRMetaBlockReaderV2(image.getJsonReader());
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
        SRMetaBlockWriter writer = new SRMetaBlockWriterV2(
                new JsonWriter(new OutputStreamWriter(image.getDataOutputStream())),
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

        final SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
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

    @Test
    public void testPrimaryTypeInMapEntry() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        SRMetaBlockWriter writer = new SRMetaBlockWriterV2(
                new JsonWriter(new OutputStreamWriter(image.getDataOutputStream())),
                SRMetaBlockID.LOCAL_META_STORE, 15);
        // Map<Long, Boolean>
        writer.writeInt(1);
        writer.writeLong(1);
        writer.writeBoolean(false);

        // Map<Integer, String>
        writer.writeInt(1);
        writer.writeInt(1);
        writer.writeString("xxx");

        // Map<Byte, Character>
        writer.writeInt(1);
        writer.writeByte((byte) 4);
        writer.writeChar('a');

        // Map<Short, Double>
        writer.writeInt(1);
        writer.writeShort((short) 3);
        writer.writeDouble(3.3);

        // Map<Integer, Float>
        writer.writeInt(1);
        writer.writeInt(1);
        writer.writeFloat(3.3f);

        writer.close();

        final boolean[] found = {false};
        final SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        reader.readMap(Long.class, Boolean.class, (MapEntryConsumer<Long, Boolean>) (aLong, aBoolean) -> {
            Assert.assertEquals(1L, (long) aLong);
            Assert.assertFalse(aBoolean);
            found[0] = true;
        });
        Assert.assertTrue(found[0]);

        found[0] = false;
        reader.readMap(Integer.class, String.class, (MapEntryConsumer<Integer, String>) (aInt, aString) -> {
            Assert.assertEquals(1, (int) aInt);
            Assert.assertEquals("xxx", aString);
            found[0] = true;
        });
        Assert.assertTrue(found[0]);

        found[0] = false;
        reader.readMap(Byte.class, Character.class, (MapEntryConsumer<Byte, Character>) (aByte, aChar) -> {
            Assert.assertEquals(4, (byte) aByte);
            Assert.assertEquals('a', (char) aChar);
            found[0] = true;
        });
        Assert.assertTrue(found[0]);

        found[0] = false;
        reader.readMap(Short.class, Double.class, (MapEntryConsumer<Short, Double>) (aShort, aDouble) -> {
            Assert.assertEquals(3, (short) aShort);
            Assert.assertTrue(Math.abs(aDouble - 3.3) < 1e-6);
            found[0] = true;
        });
        Assert.assertTrue(found[0]);

        found[0] = false;
        reader.readMap(Integer.class, Float.class, (MapEntryConsumer<Integer, Float>) (aInt, aFloat) -> {
            Assert.assertEquals(1, (int) aInt);
            Assert.assertTrue(Math.abs(aFloat - 3.3) < 1e-6);
            found[0] = true;
        });
        Assert.assertTrue(found[0]);

        reader.close();
    }
}
