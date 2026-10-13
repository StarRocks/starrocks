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

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import com.starrocks.common.Pair;
import com.starrocks.persist.proto.CompressionType;
import com.starrocks.persist.proto.FooterPB;
import com.starrocks.persist.proto.ImageHeaderPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.test.TestEntryPB;
import com.starrocks.persist.proto.test.TestManagerPB;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public class ImageV3IndexValidationTest {
    private static final SectionId DATA = new SectionId(101, "TEST_DATA");
    private static final SectionId EMPTY = new SectionId(102, "TEST_EMPTY");

    @TempDir
    Path tempDir;

    private Path writeImage(CompressionType compression) throws IOException {
        Path file = tempDir.resolve("image-" + compression.name());
        try (var output = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(output, 17, compression)) {
            try (SectionWriter section = writer.beginSection(DATA)) {
                section.writeManager(TestManagerPB.newBuilder().setCounter(42).build());
                section.writeEntry(TestEntryPB.newBuilder().setId(1).build());
            }
            try (SectionWriter section = writer.beginSection(EMPTY)) {
                // Empty sections have a valid zero-length range at the end of the section data.
            }
            writer.finish();
        }
        return file;
    }

    private static int footerStart(byte[] image) {
        int footerLength = ByteBuffer.wrap(image, image.length - ImageV3Format.TRAILER_LENGTH, 4).getInt();
        return image.length - ImageV3Format.TRAILER_LENGTH - footerLength;
    }

    private static int headerEnd(byte[] image) throws IOException {
        int start = ImageV3Format.MAGIC.length;
        ByteArrayInputStream input = new ByteArrayInputStream(image, start, image.length - start);
        ImageHeaderPB.parseDelimitedFrom(input);
        return image.length - input.available();
    }

    private Path writeWithFooter(String name, byte[] prefix, FooterPB footer) throws IOException {
        byte[] footerBytes = footer.toByteArray();
        ByteBuffer output = ByteBuffer.allocate(prefix.length + footerBytes.length + ImageV3Format.TRAILER_LENGTH);
        output.put(prefix).put(footerBytes).putInt(footerBytes.length);
        CRC32 crc = new CRC32();
        crc.update(output.array(), 0, output.position());
        output.putInt((int) crc.getValue());
        Path file = tempDir.resolve(name);
        Files.write(file, output.array());
        return file;
    }

    private Path rewriteFooter(Path original, String name, Consumer<FooterPB.Builder> edit) throws IOException {
        byte[] image = Files.readAllBytes(original);
        int end = footerStart(image);
        FooterPB.Builder footer = FooterPB.parseFrom(Arrays.copyOfRange(
                image, end, image.length - ImageV3Format.TRAILER_LENGTH)).toBuilder();
        edit.accept(footer);
        return writeWithFooter(name, Arrays.copyOf(image, end), footer.build());
    }

    private void assertDataLoads(Path file) throws IOException {
        AtomicInteger entries = new AtomicInteger();
        AtomicInteger emptySections = new AtomicInteger();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            reader.load(List.of(
                    Pair.create(DATA, section -> {
                        Assertions.assertEquals(42, section.readManager(TestManagerPB.parser()).getCounter());
                        section.readEntries(TestEntryPB.parser(), entry -> {
                            Assertions.assertEquals(1, entry.getId());
                            entries.incrementAndGet();
                        });
                    }),
                    Pair.create(EMPTY, section -> {
                        Assertions.assertEquals(0, section.numEntries());
                        section.readEntries(TestEntryPB.parser(), entry -> Assertions.fail("unexpected empty-section entry"));
                        emptySections.incrementAndGet();
                    })));
        }
        Assertions.assertEquals(1, entries.get());
        Assertions.assertEquals(1, emptySections.get());
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2147483647L, 2147483648L, 4294967295L})
    public void rejectsUnsignedFutureVersionBeforeCallingLoader(long version) throws IOException {
        Path file = rewriteFooter(writeImage(CompressionType.COMPRESSION_NONE), "future-version", footer ->
                footer.setSections(0, footer.getSections(0).toBuilder().setVersion((int) version)));
        AtomicInteger loaded = new AtomicInteger();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            ImageFormatException error = Assertions.assertThrows(ImageFormatException.class, () ->
                    reader.load(List.of(Pair.create(DATA, section -> loaded.incrementAndGet()))));
            Assertions.assertTrue(error.getMessage().contains("written with version " + version), error.getMessage());
            Assertions.assertTrue(error.getMessage().contains(file.toString()), error.getMessage());
        }
        Assertions.assertEquals(0, loaded.get());
    }

    @ParameterizedTest
    @ValueSource(longs = {2147483648L, 4294967295L})
    public void skipsUnregisteredSectionWithUnsignedFutureVersion(long version) throws IOException {
        Path file = rewriteFooter(writeImage(CompressionType.COMPRESSION_NONE), "unregistered-version", footer ->
                footer.setSections(0, footer.getSections(0).toBuilder().setVersion((int) version)));
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            reader.load(List.of(Pair.create(EMPTY, section -> Assertions.assertEquals(0, section.numEntries()))));
        }
    }

    private static Stream<Arguments> invalidRanges() {
        return Stream.of(CompressionType.COMPRESSION_NONE, CompressionType.COMPRESSION_ZSTD).flatMap(compression ->
                Stream.of("before-header", "header-last-byte", "negative-offset", "negative-length", "past-footer",
                        "into-footer", "overflow-offset", "overflow-length").map(range -> Arguments.of(compression, range)));
    }

    @ParameterizedTest
    @MethodSource("invalidRanges")
    public void rejectsInvalidRangeBeforeLoading(CompressionType compression, String range) throws IOException {
        Path original = writeImage(compression);
        byte[] image = Files.readAllBytes(original);
        int headerEnd = headerEnd(image);
        int footerStart = footerStart(image);
        Path invalid = rewriteFooter(original, range, footer -> {
            IndexEntryPB.Builder section = footer.getSections(0).toBuilder();
            switch (range) {
                case "before-header":
                    section.setOffset(ImageV3Format.MAGIC.length).setLength(0).setManagerLength(0);
                    break;
                case "header-last-byte":
                    section.setOffset(headerEnd - 1).setLength(0).setManagerLength(0);
                    break;
                case "negative-offset":
                    section.setOffset(-1);
                    break;
                case "negative-length":
                    section.setLength(-1);
                    break;
                case "past-footer":
                    section.setOffset(footerStart + 1).setLength(0).setManagerLength(0);
                    break;
                case "into-footer":
                    section.setLength(footerStart - section.getOffset() + 1);
                    break;
                case "overflow-offset":
                    section.setOffset(Long.MAX_VALUE - 3).setLength(8).setManagerLength(0);
                    break;
                case "overflow-length":
                    section.setLength(Long.MAX_VALUE);
                    break;
                default:
                    throw new AssertionError(range);
            }
            footer.setSections(0, section);
        });
        ImageFormatException error = Assertions.assertThrows(ImageFormatException.class, () -> {
            try (ImageReaderV3 ignored = new ImageReaderV3(invalid)) {
                // Invalid ranges must fail construction, including for unregistered sections.
            }
        });
        Assertions.assertTrue(error.getMessage().contains("invalid range"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains(invalid.toString()), error.getMessage());
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"COMPRESSION_NONE", "COMPRESSION_ZSTD"})
    public void loadsValidRangesIncludingEmptySectionAtFooter(CompressionType compression) throws IOException {
        Path file = writeImage(compression);
        byte[] image = Files.readAllBytes(file);
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            Assertions.assertEquals(headerEnd(image), reader.sections().get(0).getOffset());
            Assertions.assertEquals(footerStart(image), reader.sections().get(1).getOffset());
            Assertions.assertEquals(0, reader.sections().get(1).getLength());
        }
        assertDataLoads(file);
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"COMPRESSION_NONE", "COMPRESSION_ZSTD"})
    public void tracksCompleteHeaderWithMultiByteLengthPrefix(CompressionType compression) throws IOException {
        byte[] image = Files.readAllBytes(writeImage(compression));
        int oldHeaderEnd = headerEnd(image);
        int oldFooterStart = footerStart(image);
        ImageHeaderPB header = ImageHeaderPB.parseDelimitedFrom(new ByteArrayInputStream(
                image, ImageV3Format.MAGIC.length, oldHeaderEnd - ImageV3Format.MAGIC.length));
        UnknownFieldSet fields = UnknownFieldSet.newBuilder().addField(100, UnknownFieldSet.Field.newBuilder()
                .addLengthDelimited(ByteString.copyFrom(new byte[256])).build()).build();
        ByteArrayOutputStream prefix = new ByteArrayOutputStream();
        prefix.write(ImageV3Format.MAGIC);
        header.toBuilder().setUnknownFields(fields).build().writeDelimitedTo(prefix);
        int newHeaderEnd = prefix.size();
        prefix.write(image, oldHeaderEnd, oldFooterStart - oldHeaderEnd);
        FooterPB.Builder footer = FooterPB.parseFrom(Arrays.copyOfRange(
                image, oldFooterStart, image.length - ImageV3Format.TRAILER_LENGTH)).toBuilder();
        for (int i = 0; i < footer.getSectionsCount(); i++) {
            IndexEntryPB section = footer.getSections(i);
            footer.setSections(i, section.toBuilder().setOffset(section.getOffset() + newHeaderEnd - oldHeaderEnd));
        }
        Path file = writeWithFooter("extended-header", prefix.toByteArray(), footer.build());
        Assertions.assertNotEquals(0, Files.readAllBytes(file)[ImageV3Format.MAGIC.length] & 0x80);
        assertDataLoads(file);
        Path invalid = rewriteFooter(file, "extended-header-overlap", index -> index.setSections(0,
                index.getSections(0).toBuilder().setOffset(newHeaderEnd - 1).setLength(0).setManagerLength(0)));
        Assertions.assertThrows(ImageFormatException.class, () -> {
            try (ImageReaderV3 ignored = new ImageReaderV3(invalid)) {
                // Even the final byte of an extended header lies outside section data.
            }
        });
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"COMPRESSION_NONE", "COMPRESSION_ZSTD"})
    public void acceptsImageWithoutSections(CompressionType compression) throws IOException {
        Path file = tempDir.resolve("no-sections");
        try (var output = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(output, 17, compression)) {
            writer.finish();
        }
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            Assertions.assertTrue(reader.sections().isEmpty());
        }
    }
}
