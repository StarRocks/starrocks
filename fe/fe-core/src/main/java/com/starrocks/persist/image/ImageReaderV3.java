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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.starrocks.common.Pair;
import com.starrocks.persist.proto.ChecksumType;
import com.starrocks.persist.proto.CompressionType;
import com.starrocks.persist.proto.FooterPB;
import com.starrocks.persist.proto.ImageHeaderPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.PayloadFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * Reads a format-v3 image. Construction bootstraps from the tail of the file: it verifies the
 * whole-file crc32 first (a corrupt image fails here, before anything is parsed), then reads the
 * footer, the header and validates the index. {@link #load} then visits the sections <b>in the order
 * of the loader list</b>, locating each one through the index: the load order is owned by the reading
 * code and never inherited from the order the writer happened to use. Sections nobody registered
 * for are skipped without touching their bytes, and so is a registered section missing from the
 * image (an older image). A registered section this build cannot decode, because it was written
 * with a newer version or an unknown payload format, fails the load: skipping it would silently
 * drop metadata.
 */
public final class ImageReaderV3 implements Closeable {
    private static final Logger LOG = LogManager.getLogger(ImageReaderV3.class);
    private static final int CRC_BUFFER_SIZE = 1 << 20;

    private final Path path;
    private final FileChannel channel;
    private final ImageHeaderPB header;
    private final FooterPB footer;
    private final SectionCodec codec;

    public ImageReaderV3(Path path) throws IOException {
        this.path = path;
        FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
        Skeleton skeleton;
        try {
            skeleton = readSkeleton(ch, path);
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
        this.channel = ch;
        this.header = skeleton.header;
        this.footer = skeleton.footer;
        this.codec = SectionCodec.forType(header.getCompression(), ImageV3Format.DEFAULT_ZSTD_LEVEL);
    }

    public ImageHeaderPB header() {
        return header;
    }

    /** The section index, in file order. */
    public List<IndexEntryPB> sections() {
        return footer.getSectionsList();
    }

    /**
     * Loads the registered sections in the order of {@code loaders}. Each section is located through
     * the index, so the order in which the writer emitted them is irrelevant; when a load-time
     * dependency between managers changes, only this list changes.
     *
     * <p>A registered section that the image does not contain is skipped with an INFO log (the image
     * predates the section), and every section in the image that has no loader is skipped with a
     * warning. A registered section whose version is newer than
     * {@link ImageV3Format#SUPPORTED_SECTION_VERSION} or whose payload format is unknown throws an
     * {@link ImageFormatException}: metadata must never be dropped silently.
     */
    public void load(List<Pair<SectionId, SectionLoader>> loaders) throws IOException {
        Map<Integer, IndexEntryPB> entriesByType = new HashMap<>();
        for (IndexEntryPB entry : footer.getSectionsList()) {
            entriesByType.put(entry.getType(), entry);
        }
        Set<Integer> registered = new HashSet<>();
        for (Pair<SectionId, SectionLoader> loader : loaders) {
            SectionId id = loader.first;
            if (!registered.add(id.getValue())) {
                throw new IllegalArgumentException("section " + id + " is registered twice");
            }
            IndexEntryPB entry = entriesByType.get(id.getValue());
            if (entry == null) {
                LOG.info("section {} is not present in {}, nothing to load", id, path);
                continue;
            }
            // A registered section this build cannot decode is fatal: skipping it would silently drop
            // metadata. Only sections nobody asked for may be ignored.
            if (entry.getVersion() > ImageV3Format.SUPPORTED_SECTION_VERSION) {
                throw new ImageFormatException(String.format(
                        "%s: section %s was written with version %d, this build supports up to %d",
                        path, id, entry.getVersion(), ImageV3Format.SUPPORTED_SECTION_VERSION));
            }
            if (entry.getPayloadFormat() != PayloadFormat.PAYLOAD_FORMAT_PB) {
                throw new ImageFormatException(String.format("%s: section %s has unsupported payload format %d",
                        path, id, entry.getPayloadFormatValue()));
            }
            long startNs = System.nanoTime();
            loader.second.load(new SectionReaderImpl(id, entry));
            LOG.info("loaded section {}: {} entries, {} bytes, {} ms", id, entry.getNumEntries(), entry.getLength(),
                    (System.nanoTime() - startNs) / 1_000_000);
        }
        for (IndexEntryPB entry : footer.getSectionsList()) {
            if (!registered.contains(entry.getType())) {
                LOG.warn("ignore unknown section type {} in {}: {} entries, {} bytes at offset {}",
                        entry.getType(), path, entry.getNumEntries(), entry.getLength(), entry.getOffset());
            }
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    // ---------------------------------------------------------------------------------------------
    // bootstrap

    private static final class Skeleton {
        final ImageHeaderPB header;
        final FooterPB footer;

        Skeleton(ImageHeaderPB header, FooterPB footer) {
            this.header = header;
            this.footer = footer;
        }
    }

    private static Skeleton readSkeleton(FileChannel ch, Path path) throws IOException {
        long size = ch.size();
        int magicLength = ImageV3Format.MAGIC.length;
        if (size < magicLength + ImageV3Format.TRAILER_LENGTH) {
            throw new ImageFormatException(path + ": file too short to be a format v3 image (" + size + " bytes)");
        }
        if (!Arrays.equals(readFully(ch, 0, magicLength), ImageV3Format.MAGIC)) {
            throw new ImageFormatException(path + ": not a format v3 image (bad magic)");
        }
        ByteBuffer trailer = ByteBuffer.wrap(readFully(ch, size - ImageV3Format.TRAILER_LENGTH, ImageV3Format.TRAILER_LENGTH));
        int footerLength = trailer.getInt();
        int expectedCrc = trailer.getInt();

        // Whole-file check before anything is parsed: a corrupt image fails here with a clear message.
        int actualCrc = (int) crc32(ch, 0, size - 4);
        if (actualCrc != expectedCrc) {
            throw new ImageFormatException(String.format("%s: crc32 mismatch, expected %08x but computed %08x",
                    path, expectedCrc, actualCrc));
        }
        long footerStart = size - ImageV3Format.TRAILER_LENGTH - footerLength;
        if (footerLength < 0 || footerStart < magicLength) {
            throw new ImageFormatException(path + ": invalid footer length " + footerLength);
        }
        FooterPB footer;
        try {
            footer = FooterPB.parseFrom(readFully(ch, footerStart, footerLength));
        } catch (InvalidProtocolBufferException e) {
            throw new ImageFormatException(path + ": unreadable footer", e);
        }
        ImageHeaderPB header;
        try (InputStream in = new ChannelRegionInputStream(ch, magicLength, footerStart - magicLength)) {
            header = ImageHeaderPB.parseDelimitedFrom(in);
        } catch (InvalidProtocolBufferException e) {
            throw new ImageFormatException(path + ": unreadable header", e);
        }
        if (header == null) {
            throw new ImageFormatException(path + ": missing header");
        }
        if (header.getChecksum() != ChecksumType.CHECKSUM_CRC32) {
            throw new ImageFormatException(path + ": unsupported checksum type " + header.getChecksumValue());
        }
        if (header.getCompression() == CompressionType.UNRECOGNIZED) {
            throw new ImageFormatException(path + ": unsupported compression type " + header.getCompressionValue());
        }
        validateIndex(footer, path, magicLength, footerStart);
        return new Skeleton(header, footer);
    }

    private static void validateIndex(FooterPB footer, Path path, long dataStart, long dataEnd) throws ImageFormatException {
        long previousEnd = dataStart;
        Set<Integer> types = new HashSet<>();
        for (IndexEntryPB e : footer.getSectionsList()) {
            if (!types.add(e.getType())) {
                throw new ImageFormatException(path + ": section type " + e.getType() + " appears twice in the index");
            }
            long start = e.getOffset();
            long end = start + e.getLength();
            if (e.getLength() < 0 || start < previousEnd || end > dataEnd) {
                throw new ImageFormatException(String.format("%s: section type %d has an invalid range [%d, %d)",
                        path, e.getType(), start, end));
            }
            if (e.getManagerLength() < 0 || e.getManagerLength() > e.getLength()) {
                throw new ImageFormatException(String.format(
                        "%s: section type %d declares a manager message of %d bytes in a section of %d bytes",
                        path, e.getType(), e.getManagerLength(), e.getLength()));
            }
            previousEnd = end;
        }
    }

    private static long crc32(FileChannel ch, long offset, long length) throws IOException {
        CRC32 crc = new CRC32();
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.max(1, Math.min(CRC_BUFFER_SIZE, length)));
        long position = offset;
        long remaining = length;
        while (remaining > 0) {
            buffer.clear();
            buffer.limit((int) Math.min(buffer.capacity(), remaining));
            int n = ch.read(buffer, position);
            if (n <= 0) {
                throw new EOFException("unexpected end of file at offset " + position);
            }
            buffer.flip();
            crc.update(buffer);
            position += n;
            remaining -= n;
        }
        return crc.getValue();
    }

    private static byte[] readFully(FileChannel ch, long offset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        long position = offset;
        while (buffer.hasRemaining()) {
            int n = ch.read(buffer, position);
            if (n <= 0) {
                throw new EOFException("unexpected end of file at offset " + position);
            }
            position += n;
        }
        return buffer.array();
    }

    // ---------------------------------------------------------------------------------------------
    // sections

    private final class SectionReaderImpl implements SectionReader {
        private final SectionId id;
        private final IndexEntryPB entry;

        SectionReaderImpl(SectionId id, IndexEntryPB entry) {
            this.id = id;
            this.entry = entry;
        }

        @Override
        public SectionId id() {
            return id;
        }

        @Override
        public long numEntries() {
            return entry.getNumEntries();
        }

        @Override
        public <M extends Message> M readManager(Parser<M> parser) throws IOException {
            if (entry.getManagerLength() == 0) {
                throw new ImageFormatException("section " + id + " has no manager message");
            }
            try (InputStream in = codec.decompress(
                    new ChannelRegionInputStream(channel, entry.getOffset(), entry.getManagerLength()))) {
                M manager = parser.parseDelimitedFrom(in);
                if (manager == null) {
                    throw new ImageFormatException("section " + id + " has an empty manager message frame");
                }
                expectEnd(in, "section " + id + ": the manager message frame continues after the message");
                return manager;
            } catch (InvalidProtocolBufferException e) {
                throw new ImageFormatException("section " + id + ": unreadable manager message", e);
            }
        }

        @Override
        public <E extends Message> void readEntries(Parser<E> parser, Consumer<E> consume) throws IOException {
            long count = entry.getNumEntries();
            long start = entry.getOffset() + entry.getManagerLength();
            long length = entry.getLength() - entry.getManagerLength();
            if (count == 0) {
                // The writer emits no entries frame for a section without entries.
                if (length != 0) {
                    throw new ImageFormatException(String.format(
                            "section %s: the index declares no entries but the section has %d bytes of entry data",
                            id, length));
                }
                return;
            }
            try (InputStream in = codec.decompress(new ChannelRegionInputStream(channel, start, length))) {
                for (long i = 0; i < count; i++) {
                    E pb;
                    try {
                        pb = parser.parseDelimitedFrom(in);
                    } catch (InvalidProtocolBufferException e) {
                        throw new ImageFormatException(String.format("section %s: unreadable entry %d", id, i), e);
                    }
                    if (pb == null) {
                        throw new ImageFormatException(String.format(
                                "section %s: the entries ended after %d of %d", id, i, count));
                    }
                    consume.accept(pb);
                }
                expectEnd(in, String.format(
                        "section %s: the entries frame continues after the %d entries the index declares", id, count));
            }
        }

        /**
         * The frame must end where the index says it does: an entry the index does not count, or a second manager
         * message, is an inconsistent image, not something to read past.
         */
        private static void expectEnd(InputStream in, String message) throws IOException {
            if (in.read() != -1) {
                throw new ImageFormatException(message);
            }
        }
    }
}
