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

import com.starrocks.common.Pair;
import com.starrocks.persist.proto.ChecksumType;
import com.starrocks.persist.proto.CompressionType;
import com.starrocks.persist.proto.FooterPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.PayloadFormat;
import com.starrocks.persist.proto.test.TestEntryPB;
import com.starrocks.persist.proto.test.TestManagerPB;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.zip.CRC32;

public class ImageV3RoundTripTest {
    private static final SectionId MANAGER_AND_ENTRIES = new SectionId(101, "TEST_MANAGER_AND_ENTRIES");
    private static final SectionId ENTRIES_ONLY = new SectionId(102, "TEST_ENTRIES_ONLY");
    private static final SectionId MANAGER_ONLY = new SectionId(103, "TEST_MANAGER_ONLY");
    private static final SectionId EMPTY = new SectionId(104, "TEST_EMPTY");
    private static final SectionId UNKNOWN_TO_READER = new SectionId(105, "TEST_UNKNOWN_TO_READER");

    private static final long JOURNAL_ID = 12345L;
    private static final int ENTRIES = 5000;

    @TempDir
    Path tempDir;

    private static TestEntryPB entry(long id) {
        return TestEntryPB.newBuilder().setId(id).setPayload("payload-" + id + "-" + "x".repeat(50)).build();
    }

    private Path writeImage(CompressionType compression) throws IOException {
        Path file = tempDir.resolve("image-" + compression.name() + "-" + System.nanoTime());
        try (OutputStream os = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(os, JOURNAL_ID, compression, 1)) {
            try (SectionWriter s = writer.beginSection(MANAGER_AND_ENTRIES)) {
                s.writeManager(TestManagerPB.newBuilder().setCounter(777).setName("manager").build());
                for (long i = 0; i < ENTRIES; i++) {
                    s.writeEntry(entry(i));
                }
            }
            try (SectionWriter s = writer.beginSection(ENTRIES_ONLY)) {
                for (long i = 0; i < ENTRIES; i++) {
                    s.writeEntry(entry(i));
                }
            }
            try (SectionWriter s = writer.beginSection(MANAGER_ONLY)) {
                s.writeManager(TestManagerPB.newBuilder().setCounter(1).build());
            }
            try (SectionWriter s = writer.beginSection(EMPTY)) {
                // nothing written: the section still gets an index entry
            }
            try (SectionWriter s = writer.beginSection(UNKNOWN_TO_READER)) {
                s.writeEntry(entry(1));
            }
            writer.finish();
            Assertions.assertEquals(
                    Arrays.asList(MANAGER_AND_ENTRIES, ENTRIES_ONLY, MANAGER_ONLY, EMPTY, UNKNOWN_TO_READER),
                    writer.writtenSections());
        }
        return file;
    }

    private static IndexEntryPB section(ImageReaderV3 reader, SectionId id) {
        return reader.sections().stream().filter(e -> e.getType() == id.getValue()).findFirst().orElseThrow();
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"COMPRESSION_NONE", "COMPRESSION_ZSTD"})
    public void testRoundTrip(CompressionType compression) throws IOException {
        Path file = writeImage(compression);
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            Assertions.assertEquals(0, reader.header().getVersion());
            Assertions.assertEquals(JOURNAL_ID, reader.header().getJournalId());
            Assertions.assertEquals(ChecksumType.CHECKSUM_CRC32, reader.header().getChecksum());
            Assertions.assertEquals(compression, reader.header().getCompression());

            // index shape
            Assertions.assertEquals(Arrays.asList(101, 102, 103, 104, 105),
                    reader.sections().stream().map(IndexEntryPB::getType).collect(Collectors.toList()));
            IndexEntryPB withManager = section(reader, MANAGER_AND_ENTRIES);
            Assertions.assertEquals(ENTRIES, withManager.getNumEntries());
            Assertions.assertTrue(withManager.getManagerLength() > 0, "the manager message opens the section");
            Assertions.assertTrue(withManager.getManagerLength() < withManager.getLength(), "the entries follow it");
            Assertions.assertTrue(withManager.hasCrc32());
            IndexEntryPB entriesOnly = section(reader, ENTRIES_ONLY);
            Assertions.assertEquals(0, entriesOnly.getManagerLength(), "no manager message");
            Assertions.assertTrue(entriesOnly.getLength() > 0);
            IndexEntryPB managerOnly = section(reader, MANAGER_ONLY);
            Assertions.assertEquals(0, managerOnly.getNumEntries());
            Assertions.assertEquals(managerOnly.getLength(), managerOnly.getManagerLength(),
                    "the manager message is the whole section");
            IndexEntryPB empty = section(reader, EMPTY);
            Assertions.assertEquals(0, empty.getLength());
            Assertions.assertEquals(0, empty.getNumEntries());
            Assertions.assertEquals(0, empty.getManagerLength());
            for (IndexEntryPB e : reader.sections()) {
                Assertions.assertEquals(0, e.getVersion());
                Assertions.assertEquals(PayloadFormat.PAYLOAD_FORMAT_PB, e.getPayloadFormat());
            }

            // contents
            List<Long> withManagerIds = new ArrayList<>();
            List<Long> entriesOnlyIds = new ArrayList<>();
            AtomicInteger managerOnlyEntries = new AtomicInteger();
            AtomicInteger emptyEntries = new AtomicInteger();
            List<Pair<SectionId, SectionLoader>> loaders = List.of(
                    Pair.create(MANAGER_AND_ENTRIES, r -> {
                        TestManagerPB manager = r.readManager(TestManagerPB.parser());
                        Assertions.assertEquals(777, manager.getCounter());
                        Assertions.assertEquals("manager", manager.getName());
                        Assertions.assertEquals(ENTRIES, r.numEntries());
                        r.readEntries(TestEntryPB.parser(), pb -> withManagerIds.add(pb.getId()));
                    }),
                    Pair.create(ENTRIES_ONLY, r -> {
                        Assertions.assertThrows(ImageFormatException.class, () -> r.readManager(TestManagerPB.parser()));
                        r.readEntries(TestEntryPB.parser(), pb -> {
                            Assertions.assertEquals("payload-" + pb.getId() + "-" + "x".repeat(50), pb.getPayload());
                            entriesOnlyIds.add(pb.getId());
                        });
                    }),
                    Pair.create(MANAGER_ONLY, r -> {
                        Assertions.assertEquals(1, r.readManager(TestManagerPB.parser()).getCounter());
                        r.readEntries(TestEntryPB.parser(), e -> managerOnlyEntries.incrementAndGet());
                    }),
                    Pair.create(EMPTY, r -> {
                        Assertions.assertEquals(0, r.numEntries());
                        Assertions.assertThrows(ImageFormatException.class, () -> r.readManager(TestManagerPB.parser()));
                        r.readEntries(TestEntryPB.parser(), e -> emptyEntries.incrementAndGet());
                    }));
            reader.load(loaders);

            // entries come back in file order
            List<Long> expected = LongStream.range(0, ENTRIES).boxed().collect(Collectors.toList());
            Assertions.assertEquals(expected, withManagerIds);
            Assertions.assertEquals(expected, entriesOnlyIds);
            Assertions.assertEquals(0, managerOnlyEntries.get());
            Assertions.assertEquals(0, emptyEntries.get());
        }
    }

    @Test
    public void testLoadOrderFollowsTheLoaderListNotTheFile() throws IOException {
        Path file = writeImage(CompressionType.COMPRESSION_ZSTD);
        List<SectionId> visited = new ArrayList<>();
        SectionId notInImage = new SectionId(999, "TEST_NOT_IN_IMAGE");
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            // file order is 101, 102, 103, 104, 105; register the reverse, plus a section the image lacks
            reader.load(List.of(
                    Pair.create(EMPTY, r -> visited.add(r.id())),
                    Pair.create(notInImage, r -> Assertions.fail("a section absent from the image must be skipped")),
                    Pair.create(MANAGER_ONLY, r -> visited.add(r.id())),
                    Pair.create(ENTRIES_ONLY, r -> visited.add(r.id())),
                    Pair.create(MANAGER_AND_ENTRIES, r -> visited.add(r.id()))));
            Assertions.assertEquals(Arrays.asList(EMPTY, MANAGER_ONLY, ENTRIES_ONLY, MANAGER_AND_ENTRIES), visited);

            Assertions.assertThrows(IllegalArgumentException.class, () -> reader.load(List.of(
                    Pair.create(EMPTY, r -> { }),
                    Pair.create(EMPTY, r -> { }))), "a section registered twice");
        }
    }

    @Test
    public void testWriterEnforcesSectionRules() throws IOException {
        Path file = tempDir.resolve("rules");
        try (OutputStream os = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(os, 1, CompressionType.COMPRESSION_ZSTD)) {
            SectionWriter first = writer.beginSection(MANAGER_AND_ENTRIES);
            first.writeManager(TestManagerPB.getDefaultInstance());
            Assertions.assertThrows(IllegalStateException.class, () -> first.writeManager(TestManagerPB.getDefaultInstance()),
                    "second manager message");
            Assertions.assertThrows(IllegalStateException.class, () -> writer.beginSection(ENTRIES_ONLY),
                    "beginSection while another section is open");
            Assertions.assertThrows(IllegalStateException.class, writer::finish, "finish while a section is open");
            first.close();
            Assertions.assertThrows(IllegalStateException.class, () -> first.writeEntry(entry(1)), "write after close");
            Assertions.assertThrows(IllegalStateException.class, () -> writer.beginSection(MANAGER_AND_ENTRIES),
                    "same section twice");

            SectionWriter second = writer.beginSection(ENTRIES_ONLY);
            second.writeEntry(entry(1));
            Assertions.assertThrows(IllegalStateException.class, () -> second.writeManager(TestManagerPB.getDefaultInstance()),
                    "manager message after an entry");
            second.close();
            writer.finish();
            Assertions.assertThrows(IllegalStateException.class, writer::finish, "finish twice");
        }
    }

    @Test
    public void testCorruptionIsDetectedBeforeParsing() throws IOException {
        Path file = writeImage(CompressionType.COMPRESSION_ZSTD);
        long entriesOffset;
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            IndexEntryPB withManager = section(reader, MANAGER_AND_ENTRIES);
            entriesOffset = withManager.getOffset() + withManager.getManagerLength();
        }

        // one flipped byte inside the entries of section 101
        byte[] bytes = Files.readAllBytes(file);
        bytes[(int) entriesOffset + 3] ^= 0x5a;
        Path flipped = tempDir.resolve("flipped");
        Files.write(flipped, bytes);
        ImageFormatException e = Assertions.assertThrows(ImageFormatException.class, () -> new ImageReaderV3(flipped));
        Assertions.assertTrue(e.getMessage().contains("crc32 mismatch"), e.getMessage());

        // truncated
        Path truncated = tempDir.resolve("truncated");
        Files.write(truncated, Arrays.copyOf(bytes, bytes.length - 100));
        Assertions.assertThrows(ImageFormatException.class, () -> new ImageReaderV3(truncated));

        // not an image at all
        Path garbage = tempDir.resolve("garbage");
        Files.write(garbage, "this is not a v3 image, definitely not".getBytes());
        ImageFormatException magic = Assertions.assertThrows(ImageFormatException.class, () -> new ImageReaderV3(garbage));
        Assertions.assertTrue(magic.getMessage().contains("bad magic"), magic.getMessage());
    }

    /** Rewrites the footer of an image, fixing up the trailer, to simulate images from other versions. */
    private Path rewriteFooter(Path file, String name, Function<FooterPB, FooterPB> edit) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        ByteBuffer trailer = ByteBuffer.wrap(bytes, bytes.length - ImageV3Format.TRAILER_LENGTH, ImageV3Format.TRAILER_LENGTH);
        int footerLength = trailer.getInt();
        int footerStart = bytes.length - ImageV3Format.TRAILER_LENGTH - footerLength;
        FooterPB footer = FooterPB.parseFrom(Arrays.copyOfRange(bytes, footerStart, footerStart + footerLength));
        byte[] newFooter = edit.apply(footer).toByteArray();
        ByteBuffer out = ByteBuffer.allocate(footerStart + newFooter.length + ImageV3Format.TRAILER_LENGTH);
        out.put(bytes, 0, footerStart).put(newFooter).putInt(newFooter.length);
        CRC32 crc = new CRC32();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
        Path rewritten = tempDir.resolve(name);
        Files.write(rewritten, out.array());
        return rewritten;
    }

    @Test
    public void testSectionsThisBuildCannotDecodeAreRejected() throws IOException {
        Path file = writeImage(CompressionType.COMPRESSION_ZSTD);
        Path newerVersion = rewriteFooter(file, "newer-version", footer -> {
            FooterPB.Builder b = footer.toBuilder();
            for (int i = 0; i < b.getSectionsCount(); i++) {
                if (b.getSections(i).getType() == ENTRIES_ONLY.getValue()) {
                    b.setSections(i, b.getSections(i).toBuilder().setVersion(ImageV3Format.SUPPORTED_SECTION_VERSION + 1));
                }
            }
            return b.build();
        });
        Path unknownPayload = rewriteFooter(file, "unknown-payload", footer -> {
            FooterPB.Builder b = footer.toBuilder();
            for (int i = 0; i < b.getSectionsCount(); i++) {
                if (b.getSections(i).getType() == MANAGER_ONLY.getValue()) {
                    b.setSections(i, b.getSections(i).toBuilder().setPayloadFormat(PayloadFormat.PAYLOAD_FORMAT_UNSPECIFIED));
                }
            }
            return b.build();
        });
        AtomicInteger loaded = new AtomicInteger();
        List<Pair<SectionId, SectionLoader>> loaders = List.of(
                Pair.create(MANAGER_AND_ENTRIES, r -> loaded.incrementAndGet()),
                Pair.create(ENTRIES_ONLY, r -> loaded.incrementAndGet()),
                Pair.create(MANAGER_ONLY, r -> loaded.incrementAndGet()),
                Pair.create(EMPTY, r -> loaded.incrementAndGet()));

        try (ImageReaderV3 reader = new ImageReaderV3(newerVersion)) {
            ImageFormatException e = Assertions.assertThrows(ImageFormatException.class, () -> reader.load(loaders));
            Assertions.assertTrue(e.getMessage().contains("written with version 1"), e.getMessage());
            Assertions.assertEquals(1, loaded.get(), "sections before the offending one were loaded, nothing after it");
        }
        loaded.set(0);
        try (ImageReaderV3 reader = new ImageReaderV3(unknownPayload)) {
            ImageFormatException e = Assertions.assertThrows(ImageFormatException.class, () -> reader.load(loaders));
            Assertions.assertTrue(e.getMessage().contains("unsupported payload format"), e.getMessage());
            Assertions.assertEquals(2, loaded.get());
        }
        // a section nobody registered for may carry anything: it is skipped, not decoded
        try (ImageReaderV3 reader = new ImageReaderV3(newerVersion)) {
            reader.load(List.of(Pair.create(MANAGER_AND_ENTRIES, r -> { })));
        }
    }

    @Test
    public void testInconsistentIndexIsRejected() throws IOException {
        Path file = writeImage(CompressionType.COMPRESSION_NONE);
        // a manager message longer than its section is caught when the index is validated
        Path badManagerLength = rewriteFooter(file, "bad-manager-length", footer -> {
            FooterPB.Builder b = footer.toBuilder();
            IndexEntryPB.Builder first = b.getSections(0).toBuilder();
            first.setManagerLength(first.getLength() + 1);
            return b.setSections(0, first).build();
        });
        ImageFormatException e = Assertions.assertThrows(ImageFormatException.class, () -> new ImageReaderV3(badManagerLength));
        Assertions.assertTrue(e.getMessage().contains("manager message"), e.getMessage());

        // an entry count the stream cannot honor is caught while the section is read
        Path tooManyEntries = rewriteFooter(file, "too-many-entries", footer -> {
            FooterPB.Builder b = footer.toBuilder();
            IndexEntryPB.Builder first = b.getSections(0).toBuilder();
            first.setNumEntries(first.getNumEntries() + 1);
            return b.setSections(0, first).build();
        });
        try (ImageReaderV3 reader = new ImageReaderV3(tooManyEntries)) {
            ImageFormatException ended = Assertions.assertThrows(ImageFormatException.class, () -> reader.load(List.of(
                    Pair.create(MANAGER_AND_ENTRIES, r -> r.readEntries(TestEntryPB.parser(), pb -> { })))));
            Assertions.assertTrue(ended.getMessage().contains("ended after " + ENTRIES + " of " + (ENTRIES + 1)),
                    ended.getMessage());
        }

        // a manager frame that runs on into the entries (uncompressed, so the longer region still parses)
        Path longManager = rewriteFooter(file, "long-manager", footer -> {
            FooterPB.Builder b = footer.toBuilder();
            IndexEntryPB.Builder first = b.getSections(0).toBuilder();
            first.setManagerLength(first.getLength());
            return b.setSections(0, first).build();
        });
        try (ImageReaderV3 reader = new ImageReaderV3(longManager)) {
            ImageFormatException continues = Assertions.assertThrows(ImageFormatException.class, () -> reader.load(List.of(
                    Pair.create(MANAGER_AND_ENTRIES, r -> r.readManager(TestManagerPB.parser())))));
            Assertions.assertTrue(continues.getMessage().contains("manager message frame continues"),
                    continues.getMessage());
        }
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"COMPRESSION_NONE", "COMPRESSION_ZSTD"})
    public void testEntriesTheIndexDoesNotDeclareAreRejected(CompressionType compression) throws IOException {
        Path file = writeImage(compression);
        // the frame holds one entry more than the index declares
        Path fewerEntries = rewriteFooter(file, "fewer-entries-" + compression.name(), footer -> {
            FooterPB.Builder b = footer.toBuilder();
            IndexEntryPB.Builder first = b.getSections(0).toBuilder();
            first.setNumEntries(first.getNumEntries() - 1);
            return b.setSections(0, first).build();
        });
        try (ImageReaderV3 reader = new ImageReaderV3(fewerEntries)) {
            ImageFormatException continues = Assertions.assertThrows(ImageFormatException.class, () -> reader.load(List.of(
                    Pair.create(MANAGER_AND_ENTRIES, r -> r.readEntries(TestEntryPB.parser(), pb -> { })))));
            Assertions.assertTrue(continues.getMessage().contains("continues after the " + (ENTRIES - 1) + " entries"),
                    continues.getMessage());
        }

        // the index declares no entries for a section that has entry data
        Path noEntries = rewriteFooter(file, "no-entries-" + compression.name(), footer -> {
            FooterPB.Builder b = footer.toBuilder();
            IndexEntryPB.Builder first = b.getSections(0).toBuilder();
            first.setNumEntries(0);
            return b.setSections(0, first).build();
        });
        try (ImageReaderV3 reader = new ImageReaderV3(noEntries)) {
            ImageFormatException data = Assertions.assertThrows(ImageFormatException.class, () -> reader.load(List.of(
                    Pair.create(MANAGER_AND_ENTRIES, r -> r.readEntries(TestEntryPB.parser(), pb -> { })))));
            Assertions.assertTrue(data.getMessage().contains("declares no entries"), data.getMessage());
        }
    }

    @Test
    public void testConsumerFailureStopsLoading() throws IOException {
        Path file = writeImage(CompressionType.COMPRESSION_ZSTD);
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            AtomicInteger consumed = new AtomicInteger();
            RuntimeException boom = Assertions.assertThrows(RuntimeException.class, () -> reader.load(List.of(
                    Pair.create(MANAGER_AND_ENTRIES, r -> r.readEntries(TestEntryPB.parser(), pb -> {
                        if (consumed.incrementAndGet() == 10) {
                            throw new RuntimeException("consumer failed on purpose");
                        }
                    })))));
            Assertions.assertEquals("consumer failed on purpose", boom.getMessage());
            Assertions.assertEquals(10, consumed.get(), "nothing after the failing entry was consumed");
            // the reader is still usable afterwards
            AtomicInteger count = new AtomicInteger();
            reader.load(List.of(Pair.create(ENTRIES_ONLY,
                    r -> r.readEntries(TestEntryPB.parser(), pb -> count.incrementAndGet()))));
            Assertions.assertEquals(ENTRIES, count.get());
        }
    }
}
