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

package com.starrocks.persist.image;

import com.google.protobuf.UnknownFieldSet;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.DatabasePB;
import com.starrocks.persist.proto.FooterPB;
import com.starrocks.persist.proto.FunctionPB;
import com.starrocks.persist.proto.ScalarTypePB;
import com.starrocks.persist.proto.TypePB;
import com.starrocks.type.TypeProtoConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ImageV3SubtypeTest {
    private static final SectionId TYPES = new SectionId(901, "TEST_TYPES");
    private static final SectionId NEXT = new SectionId(902, "TEST_NEXT");
    private static final TypePB KNOWN = TypePB.newBuilder().setKind(TypePB.Kind.SCALAR)
            .setScalar(ScalarTypePB.newBuilder().setType(ScalarTypePB.PrimitiveType.INT)).build();
    private static final TypePB UNKNOWN = TypePB.newBuilder().setKindValue(999)
            .setUnknownFields(UnknownFieldSet.newBuilder().addField(111,
                    UnknownFieldSet.Field.newBuilder().addLengthDelimited(
                            com.google.protobuf.ByteString.copyFromUtf8("future instance")).build()).build()).build();

    @TempDir
    Path tempDir;
    private boolean originalIgnore;

    @BeforeEach
    public void saveConfig() {
        originalIgnore = Config.metadata_ignore_unknown_subtype;
    }

    @AfterEach
    public void restoreConfig() {
        Config.metadata_ignore_unknown_subtype = originalIgnore;
    }

    static Stream<Arguments> sequences() {
        List<List<TypePB>> sequences = List.of(
                List.of(UNKNOWN, KNOWN, KNOWN), List.of(KNOWN, UNKNOWN, KNOWN),
                List.of(KNOWN, KNOWN, UNKNOWN), List.of(UNKNOWN, UNKNOWN, UNKNOWN),
                List.of(KNOWN, KNOWN, KNOWN), List.of());
        return sequences.stream().flatMap(entries -> Stream.of(false, true)
                .map(ignore -> Arguments.of(entries, ignore)));
    }

    @ParameterizedTest
    @MethodSource("sequences")
    public void testSkipOrStopAtTheEntryBoundary(List<TypePB> entries, boolean ignore)
            throws Exception {
        Config.metadata_ignore_unknown_subtype = ignore;
        Path file = write(entries);
        List<TypePB> installed = new ArrayList<>();
        AtomicBoolean nextSection = new AtomicBoolean();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            List<Pair<SectionId, SectionLoader>> loaders = List.of(
                    Pair.create(TYPES, r -> {
                        assertEquals(entries.size(), r.numEntries());
                        r.readEntries(TypePB.parser(), pb -> {
                            TypeProtoConverter.fromProto(pb);
                            installed.add(pb);
                        });
                    }),
                    Pair.create(NEXT, r -> r.readEntries(TypePB.parser(), pb -> nextSection.set(true))));
            int firstUnknown = entries.indexOf(UNKNOWN);
            if (firstUnknown >= 0 && !ignore) {
                ImageFormatException failure = assertThrows(ImageFormatException.class, () -> reader.load(loaders));
                SubtypeNotFoundException cause = assertInstanceOf(SubtypeNotFoundException.class, failure.getCause());
                assertEquals("999", cause.getSubtype());
                assertTrue(failure.getMessage().contains(TYPES.toString()));
                assertTrue(failure.getMessage().contains("entry " + firstUnknown));
                assertEquals(entries.subList(0, firstUnknown), installed);
                assertEquals(false, nextSection.get());
            } else {
                reader.load(loaders);
                assertEquals(entries.stream().filter(KNOWN::equals).toList(), installed);
                assertTrue(nextSection.get());
            }
        }
    }

    @Test
    public void testIgnoringDoesNotDisableEntryCounts() throws Exception {
        Config.metadata_ignore_unknown_subtype = true;
        Path file = write(List.of(UNKNOWN, UNKNOWN, UNKNOWN));
        byte[] bytes = Files.readAllBytes(file);
        int footerSize = ByteBuffer.wrap(bytes, bytes.length - 8, 4).getInt();
        int footerStart = bytes.length - 8 - footerSize;
        FooterPB footer = FooterPB.parseFrom(java.util.Arrays.copyOfRange(bytes, footerStart, bytes.length - 8));
        for (int declared : new int[] {2, 4}) {
            FooterPB changed = footer.toBuilder().setSections(0,
                    footer.getSections(0).toBuilder().setNumEntries(declared)).build();
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            output.write(bytes, 0, footerStart);
            changed.writeTo(output);
            output.write(ByteBuffer.allocate(8).putInt(changed.getSerializedSize()).putInt(0).array());
            byte[] modified = output.toByteArray();
            repairCrc(modified);
            Files.write(file, modified);
            try (ImageReaderV3 reader = new ImageReaderV3(file)) {
                ImageFormatException failure = assertThrows(ImageFormatException.class, () -> reader.load(List.of(
                        Pair.create(TYPES, r -> r.readEntries(TypePB.parser(), TypeProtoConverter::fromProto)))));
                assertTrue(failure.getMessage().contains(declared == 2 ? "continues after" : "ended after"));
            }
        }
    }

    @Test
    public void testMalformedKnownInstancesAreNotIgnored() throws Exception {
        Config.metadata_ignore_unknown_subtype = true;
        for (TypePB bad : List.of(TypePB.getDefaultInstance(), KNOWN.toBuilder()
                .setScalar(ScalarTypePB.newBuilder().setTypeValue(999)).build())) {
            Path file = write(List.of(UNKNOWN, bad, KNOWN));
            try (ImageReaderV3 reader = new ImageReaderV3(file)) {
                assertThrows(IllegalArgumentException.class, () -> reader.load(List.of(
                        Pair.create(TYPES, r -> r.readEntries(TypePB.parser(), TypeProtoConverter::fromProto)))));
            }
        }
    }

    @Test
    public void testOtherConsumerFailuresAreNotIgnored() throws Exception {
        Config.metadata_ignore_unknown_subtype = true;
        Path file = write(List.of(KNOWN));
        RuntimeException expected = new IllegalStateException("installation failed");
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            assertSame(expected, assertThrows(IllegalStateException.class, () -> reader.load(List.of(
                    Pair.create(TYPES, r -> r.readEntries(TypePB.parser(), pb -> {
                        throw expected;
                    }))))));
        }
    }

    @Test
    public void testUnreadableProtobufIsNotIgnored() throws Exception {
        Config.metadata_ignore_unknown_subtype = true;
        Path file = write(List.of(UNKNOWN));
        long offset;
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            offset = reader.sections().get(0).getOffset();
        }
        byte[] bytes = Files.readAllBytes(file);
        // This entry has a one-byte length prefix; zero is an invalid protobuf tag.
        assertTrue((bytes[(int) offset] & 0x80) == 0);
        bytes[(int) offset + 1] = 0;
        repairCrc(bytes);
        Files.write(file, bytes);
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            ImageFormatException failure = assertThrows(ImageFormatException.class, () -> reader.load(List.of(
                    Pair.create(TYPES, r -> r.readEntries(TypePB.parser(), TypeProtoConverter::fromProto)))));
            assertTrue(failure.getMessage().contains("unreadable entry 0"));
        }
    }

    @Test
    public void testDatabaseFunctionPolicy() {
        Config.metadata_ignore_unknown_subtype = true;
        DatabasePB pb = DatabasePB.newBuilder().addFunctions(FunctionPB.newBuilder().setKindValue(999)).build();
        assertTrue(com.starrocks.catalog.Database.fromProto(pb).getFunctions().isEmpty());
        Config.metadata_ignore_unknown_subtype = false;
        assertThrows(SubtypeNotFoundException.class, () -> com.starrocks.catalog.Database.fromProto(pb));
    }

    @Test
    public void testManagerConversionIsOutsideTheEntryPolicy() throws Exception {
        Config.metadata_ignore_unknown_subtype = true;
        Path file = tempDir.resolve("manager");
        try (ImageWriterV3 writer = new ImageWriterV3(Files.newOutputStream(file), 1L)) {
            try (SectionWriter section = writer.beginSection(TYPES)) {
                section.writeManager(UNKNOWN);
            }
            writer.finish();
        }
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            assertThrows(SubtypeNotFoundException.class, () -> reader.load(List.of(
                    Pair.create(TYPES, r -> TypeProtoConverter.fromProto(r.readManager(TypePB.parser()))))));
        }
    }

    private Path write(List<TypePB> entries) throws IOException {
        Path file = Files.createTempFile(tempDir, "subtypes-", ".image");
        try (ImageWriterV3 writer = new ImageWriterV3(Files.newOutputStream(file), 1L)) {
            try (SectionWriter section = writer.beginSection(TYPES)) {
                for (TypePB entry : entries) {
                    section.writeEntry(entry);
                }
            }
            try (SectionWriter section = writer.beginSection(NEXT)) {
                section.writeEntry(KNOWN);
            }
            writer.finish();
        }
        return file;
    }

    private static void repairCrc(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes, bytes.length - 4, 4).putInt((int) crc.getValue());
    }
}
