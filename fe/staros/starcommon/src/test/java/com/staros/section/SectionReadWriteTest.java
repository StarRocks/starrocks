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

package com.staros.section;
import com.staros.common.SampleComponent;
import com.staros.proto.SectionType;
import com.staros.stream.ChunkedIOStreamTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

public class SectionReadWriteTest {
    private static final Logger LOG = LogManager.getLogger(ChunkedIOStreamTest.class);

    public static class SampleDumper {
        public List<SampleComponent.Component> components;

        public SampleDumper(SampleComponent.Component ... components) {
            this.components = Arrays.asList(components);
        }

        public void dump(OutputStream os) throws IOException {
            SectionWriter writer = new SectionWriter(os);
            for (SampleComponent.Component c : components) {
                try (OutputStream stream = writer.appendSection(c.type())) {
                    c.dump(stream);
                }
            }
            writer.close();
        }
    }

    public static class SampleParser {
        public List<SampleComponent.Component> components;
        public SampleParser(SampleComponent.Component ... components) {
            this.components = Arrays.asList(components);
        }

        public void parse(InputStream in) throws IOException {
            SectionReader reader = new SectionReader(in);
            reader.forEach(this::parseSection);
        }

        public void parse(SectionReader reader) throws IOException {
            reader.forEach(this::parseSection);
        }

        private void parseSection(Section section) {
            try {
                SectionType type = section.getHeader().getSectionType();
                InputStream stream = section.getStream();
                boolean found = false;
                for (SampleComponent.Component c : components) {
                    if (c.type() == type) {
                        c.restore(stream);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.error("Unknown type:{}", type);
                }
            } catch (Exception exception) {
                // ignore the exception in test
            }
        }
    }

    @Test
    public void testSerializationAndDeserializationBasic() throws IOException {
        SampleComponent.ManualComponent manual = new SampleComponent.ManualComponent(true, "hello");
        SampleComponent.PbComponent pb = new SampleComponent.PbComponent("test", 1, "data");
        SampleComponent.CompositeComponent comp = new SampleComponent.CompositeComponent(manual, pb);

        ByteArrayOutputStream baseOut = new ByteArrayOutputStream();
        comp.dump(baseOut);

        ByteArrayInputStream inStream = new ByteArrayInputStream(baseOut.toByteArray());
        SampleComponent.CompositeComponent newComp = SampleComponent.CompositeComponent.parseFrom(inStream);

        // inStream reaches EOF
        Assert.assertEquals(-1, inStream.read());
        // restore to the same state
        Assert.assertEquals(comp, newComp);
    }

    @Test
    public void testReaderWriterMatches() throws IOException {
        SampleComponent.ManualComponent manualVar = new SampleComponent.ManualComponent(false, "TEXT");
        SampleComponent.PbComponent pbVar = new SampleComponent.PbComponent("PROTOBUF", 123456789, "CLOUD NATIVE");
        SampleComponent.JsonComponent jsVar = new SampleComponent.JsonComponent("GSON_SER", 2.71828);

        SampleDumper dumper = new SampleDumper(pbVar, jsVar, manualVar);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        dumper.dump(os);

        SampleComponent.ManualComponent manualVar2 = new SampleComponent.ManualComponent();
        SampleComponent.PbComponent pbVar2 = new SampleComponent.PbComponent();
        SampleComponent.JsonComponent jsVar2 = new SampleComponent.JsonComponent();

        Assert.assertNotEquals(manualVar, manualVar2);
        Assert.assertNotEquals(pbVar, pbVar2);
        Assert.assertNotEquals(jsVar, jsVar2);

        SampleParser parser = new SampleParser(pbVar2, jsVar2, manualVar2);
        ByteArrayInputStream inStream = new ByteArrayInputStream(os.toByteArray());
        parser.parse(inStream);

        // now all the vars are restored to the same states
        Assert.assertEquals(manualVar, manualVar2);
        Assert.assertEquals(pbVar, pbVar2);
        Assert.assertEquals(jsVar, jsVar2);
    }

    @Test
    public void testReaderIllegalStateException() throws IOException {
        ByteArrayInputStream inStream = new ByteArrayInputStream(createSectionStreamForTest());
        SectionReader reader = new SectionReader(inStream);
        // reader is already consumed some data
        reader.next();

        SampleParser parser = new SampleParser();
        Assert.assertThrows(IllegalStateException.class, () -> parser.parse(reader));
    }

    @Test
    public void testReaderEOFException() throws IOException {
        SampleParser parser = new SampleParser();
        byte[] data = createSectionStreamForTest();
        ByteArrayInputStream inStream = new ByteArrayInputStream(data, 0, data.length - 1);
        Assert.assertThrows(EOFException.class, () -> parser.parse(inStream));
    }

    @Test
    public void testReaderConsumesAllDataWhenClose() throws IOException {
        byte[] data = createSectionStreamForTest();
        ByteArrayInputStream inStream = new ByteArrayInputStream(data);
        Assert.assertEquals(data.length, inStream.available());

        SectionReader reader = new SectionReader(inStream);
        // no reader triggers
        Assert.assertEquals(data.length, inStream.available());

        // all data consumed
        reader.close();
        Assert.assertEquals(0, inStream.available());
        Assert.assertEquals(-1, inStream.read());
    }

    @Test
    public void testReaderStopsInForEachException() throws IOException {
        byte[] data = createSectionStreamForTest();
        ByteArrayInputStream inStream = new ByteArrayInputStream(data);
        SectionReader reader = new SectionReader(inStream);
        // Not consume any data yet
        Assert.assertEquals(data.length, inStream.available());

        SectionConsumer<Section> consumer = (Section section) -> {
            throw new IOException("InjectedIOException");
        };
        Assert.assertThrows(IOException.class, () -> reader.forEach(consumer));

        // SectionHeader is processed
        Assert.assertTrue(inStream.available() < data.length);
        // Still has data unread in inStream
        Assert.assertTrue(inStream.available() > 0);
    }

    private byte[] createSectionStreamForTest() throws IOException {
        SampleComponent.ManualComponent manualVar = new SampleComponent.ManualComponent(false, "TEXT");
        SampleComponent.PbComponent pbVar = new SampleComponent.PbComponent("PROTOBUF", 123456789, "CLOUD NATIVE");
        SampleComponent.JsonComponent jsVar = new SampleComponent.JsonComponent("GSON_SER", 2.71828);

        SampleDumper dumper = new SampleDumper(pbVar, jsVar, manualVar);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        dumper.dump(os);
        return os.toByteArray();
    }

    @Test
    public void testReaderWriterMismatch() throws IOException {
        // v1: ManualComponent, PbComponent, CompositeComponent
        // v2: CompositeComponent, ManualComponentV2, JsonComponent
        // * ManualComponent upgraded to V2
        // * PbComponent is deleted
        // * JsonComponent is added
        // Can take it as v1 upgrade to v2, or take it as v1 downgrade to v2
        SampleComponent.ManualComponent manualVar = new SampleComponent.ManualComponent(false, "TEXT");
        SampleComponent.PbComponent pbVar = new SampleComponent.PbComponent("PROTOBUF", 123456789, "CLOUD NATIVE");
        SampleComponent.CompositeComponent compositeVar;
        {
            SampleComponent.ManualComponent manualVarX = new SampleComponent.ManualComponent(true, "NESTED");
            SampleComponent.PbComponent pbVarX =
                    new SampleComponent.PbComponent("FLAT_Buffer", 987654321, "CLOUD NATIVE");
            compositeVar = new SampleComponent.CompositeComponent(manualVarX, pbVarX);
        }

        // DUMPER: ManualComponent, PbComponent, CompositeComponent
        SampleDumper dumper = new SampleDumper(manualVar, pbVar, compositeVar);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        dumper.dump(os);

        SampleComponent.ManualComponentV2 manualVarV2 = new SampleComponent.ManualComponentV2();
        SampleComponent.JsonComponent jsVar2 = new SampleComponent.JsonComponent();
        SampleComponent.CompositeComponent compositeVar2 = new SampleComponent.CompositeComponent();

        // PARSER: CompositeComponent, ManualComponentV2, JsonComponent
        SampleParser parser = new SampleParser(compositeVar2, manualVarV2, jsVar2);
        ByteArrayInputStream inStream = new ByteArrayInputStream(os.toByteArray());
        parser.parse(inStream);

        // ManualV2 is restored, except the new added field
        Assert.assertEquals(manualVar.aField, manualVarV2.aField);
        Assert.assertEquals(manualVar.bField, manualVarV2.bField);
        Assert.assertEquals(0, manualVarV2.cField);
        // JSON var is still empty
        Assert.assertEquals(new SampleComponent.JsonComponent(), jsVar2);
        // CompositeVar is fully restored
        Assert.assertEquals(compositeVar, compositeVar2);
    }
}
