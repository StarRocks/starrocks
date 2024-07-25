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

public class SRMetaBlockV2Test {

    private static Path tmpDir;

    @BeforeClass
    public static void setUp() throws Exception {
        tmpDir = Files.createTempDirectory(Paths.get("."), "SRMetaBlockV2Test");
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
}
