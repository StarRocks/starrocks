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

package com.staros.common;

import com.google.gson.Gson;
import com.staros.proto.ComponentInfo;
import com.staros.proto.SectionType;
import com.staros.section.Section;
import com.staros.section.SectionReader;
import com.staros.section.SectionWriter;
import org.apache.commons.io.IOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;


public class SampleComponent {

    public abstract static class Component {
        public abstract void dump(OutputStream os) throws IOException;

        public abstract void restore(InputStream in) throws IOException;

        public abstract SectionType type();
    }

    // Simulate manual serialization
    public static class ManualComponent extends Component {
        public static SectionType TYPE = SectionType.SECTION_COMPONENT_MANUAL;
        public boolean aField;
        public String bField;

        public ManualComponent() {
            this.bField = "";
        }

        public ManualComponent(boolean b, String s) {
            this.aField = b;
            this.bField = s;
        }

        @Override
        public void dump(OutputStream os) throws IOException {
            DataOutputStream out = new DataOutputStream(os);
            out.writeBoolean(aField);
            out.writeInt(bField.length());
            out.writeBytes(bField);
        }

        @Override
        public void restore(InputStream in) throws IOException {
            DataInputStream input = new DataInputStream(in);
            this.aField = input.readBoolean();
            int len = input.readInt();
            byte[] data = new byte[len];
            input.read(data);
            bField = new String(data);
        }

        @Override
        public SectionType type() {
            return TYPE;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || other.getClass() != this.getClass()) {
                return false;
            }
            ManualComponent rhs = (ManualComponent) other;
            return aField == rhs.aField && bField.equals(rhs.bField);
        }
    }

    // Simulate manual serialization upgrade to V2 version, a new field is added
    public static class ManualComponentV2 extends Component {
        public static SectionType TYPE = SectionType.SECTION_COMPONENT_MANUAL;
        public boolean aField;
        public String bField;
        public int cField;

        public ManualComponentV2() {
            this.bField = "";
        }

        public ManualComponentV2(boolean b, String s, int c) {
            this.aField = b;
            this.bField = s;
            this.cField = c;
        }

        @Override
        public void dump(OutputStream os) throws IOException {
            DataOutputStream out = new DataOutputStream(os);
            out.writeBoolean(aField);
            out.writeInt(bField.length());
            out.writeBytes(bField);
            out.writeInt(cField);
        }

        @Override
        public void restore(InputStream in) throws IOException {
            DataInputStream input = new DataInputStream(in);
            this.aField = input.readBoolean();
            int len = input.readInt();
            byte[] data = new byte[len];
            input.read(data);
            bField = new String(data);
            cField = input.readInt();
        }

        @Override
        public SectionType type() {
            return TYPE;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || other.getClass() != this.getClass()) {
                return false;
            }
            ManualComponentV2 rhs = (ManualComponentV2) other;
            return aField == rhs.aField && bField.equals(rhs.bField) && cField == rhs.cField;
        }
    }

    // Simulate GSON serialization
    public static class JsonComponent extends Component {
        public static SectionType TYPE = SectionType.SECTION_COMPONENT_JSON;
        public String iField;
        public double jField;

        public JsonComponent() {
            this.iField = "";
        }

        public JsonComponent(String s, double f) {
            this.iField = s;
            this.jField = f;
        }

        @Override
        public void dump(OutputStream os) throws IOException {
            String js = new Gson().toJson(this);
            os.write(js.getBytes());
        }

        @Override
        public void restore(InputStream in) throws IOException {
            String js = IOUtils.toString(in, Charset.defaultCharset());
            JsonComponent b = new Gson().fromJson(js, JsonComponent.class);
            this.iField = b.iField;
            this.jField = b.jField;
        }

        @Override
        public SectionType type() {
            return TYPE;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || other.getClass() != this.getClass()) {
                return false;
            }
            JsonComponent rhs = (JsonComponent) other;
            return jField == rhs.jField && iField.equals(rhs.iField);
        }
    }

    // Simulate protobuf serialization
    public static class PbComponent extends Component {
        public static SectionType TYPE = SectionType.SECTION_COMPONENT_PB;
        public String name;
        public int version;
        public String data;

        public PbComponent() {
            this.name = "";
            this.data = "";
        }

        public PbComponent(String s, int v, String d) {
            this.name = s;
            this.version = v;
            this.data = d;
        }

        @Override
        public void dump(OutputStream os) throws IOException {
            ComponentInfo.newBuilder()
                    .setName(name)
                    .setVersion(version)
                    .setData(data)
                    .build().writeTo(os);
        }

        @Override
        public void restore(InputStream in) throws IOException {
            ComponentInfo info = ComponentInfo.parseFrom(in);
            this.name = info.getName();
            this.data = info.getData();
            this.version = info.getVersion();
        }

        @Override
        public SectionType type() {
            return TYPE;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || other.getClass() != this.getClass()) {
                return false;
            }
            PbComponent rhs = (PbComponent) other;
            return version == rhs.version && name.equals(rhs.name) && data.equals(rhs.data);
        }
    }

    // Simulate nested section reader/writer serialization
    public static class CompositeComponent extends Component {
        public static SectionType TYPE = SectionType.SECTION_COMPONENT_COMPOSITE;
        public ManualComponent objA;
        public PbComponent objC;

        // private no parameter constructor
        public CompositeComponent() {
            this.objA = new ManualComponent();
            this.objC = new PbComponent();
        }

        public CompositeComponent(ManualComponent a, PbComponent c) {
            this.objA = a;
            this.objC = c;
        }

        @Override
        public void dump(OutputStream os) throws IOException {
            try (SectionWriter writer = new SectionWriter(os)) {
                List<Component> components = Arrays.asList(objA, objC);
                for (Component m : components) {
                    try (OutputStream stream = writer.appendSection(m.type())) {
                        m.dump(stream);
                    }
                }
            }
        }

        private void restoreInternal(Section section) {
            try {
                switch (section.getHeader().getSectionType()) {
                    case SECTION_COMPONENT_MANUAL:
                        objA.restore(section.getStream());
                        break;
                    case SECTION_COMPONENT_PB:
                        objC.restore(section.getStream());
                        break;
                    default:
                        section.skip();
                        break;
                }
            } catch (IOException exception) {
                // ignore the exception
            }
        }

        @Override
        public void restore(InputStream in) throws IOException {
            try (SectionReader reader = new SectionReader(in)) {
                reader.forEach(this::restoreInternal);
            }
        }

        @Override
        public SectionType type() {
            return TYPE;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || other.getClass() != this.getClass()) {
                return false;
            }
            CompositeComponent rhs = (CompositeComponent) other;
            return objA.equals(rhs.objA) && objC.equals(rhs.objC);
        }

        public static CompositeComponent parseFrom(InputStream in) throws IOException {
            CompositeComponent comp = new CompositeComponent();
            comp.restore(in);
            return comp;
        }
    }
}
