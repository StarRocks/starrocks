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

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.starrocks.persist.proto.ChecksumType;
import com.starrocks.persist.proto.CompressionType;
import com.starrocks.persist.proto.FooterPB;
import com.starrocks.persist.proto.ImageHeaderPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.PayloadFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Writes a format-v3 image: the skeleton (MAGIC, header, footer, trailer), one compressed frame for
 * the manager message and one for the entries of every section, the two levels of crc32 and the
 * index. Managers only see {@link SectionWriter}.
 *
 * <p>Usage: construct, then for every section {@code try (SectionWriter s = beginSection(id)) {...}}
 * in the order the sections must be loaded, then {@link #finish()} and {@link #close()}. Sections
 * are written strictly one at a time; the file order is the load order.
 */
public final class ImageWriterV3 implements Closeable {
    private static final Logger LOG = LogManager.getLogger(ImageWriterV3.class);

    private final CountingOutput out;
    private final SectionCodec codec;
    private final List<IndexEntryPB> index = new ArrayList<>();
    private final List<SectionId> writtenSections = new ArrayList<>();
    private final Set<Integer> writtenTypes = new HashSet<>();
    private SectionWriterImpl openSection;
    private boolean finished;

    public ImageWriterV3(OutputStream out, long journalId, CompressionType compression) throws IOException {
        this(out, journalId, compression, ImageV3Format.DEFAULT_ZSTD_LEVEL);
    }

    /** @param zstdLevel compression level, only used with {@code COMPRESSION_ZSTD} */
    public ImageWriterV3(OutputStream out, long journalId, CompressionType compression, int zstdLevel)
            throws IOException {
        this.codec = SectionCodec.forType(compression, zstdLevel);
        this.out = new CountingOutput(new BufferedOutputStream(out, 1 << 16));
        this.out.write(ImageV3Format.MAGIC);
        ImageHeaderPB.newBuilder()
                .setVersion(0)
                .setJournalId(journalId)
                .setChecksum(ChecksumType.CHECKSUM_CRC32)
                .setCompression(compression)
                .build()
                .writeDelimitedTo(this.out);
    }

    /** Opens the next section. Only one section may be open at a time and each id may appear once. */
    public SectionWriter beginSection(SectionId id) throws IOException {
        Preconditions.checkState(!finished, "image is already finished");
        if (openSection != null) {
            throw new IllegalStateException("section " + openSection.id + " is still open, close it before beginning " + id);
        }
        if (!writtenTypes.add(id.getValue())) {
            throw new IllegalStateException("section " + id + " was already written");
        }
        openSection = new SectionWriterImpl(id);
        return openSection;
    }

    /** Writes footer and trailer. No section may be open. The stream is flushed but not closed. */
    public void finish() throws IOException {
        Preconditions.checkState(!finished, "image is already finished");
        if (openSection != null) {
            throw new IllegalStateException("section " + openSection.id + " is still open");
        }
        byte[] footer = FooterPB.newBuilder().addAllSections(index).build().toByteArray();
        out.write(footer);
        out.writeIntBigEndian(footer.length);
        // The running crc now covers exactly [0, EOF-4).
        out.writeIntBigEndian((int) out.fileCrc());
        out.flush();
        finished = true;
        LOG.info("finished writing image v3: {} sections, {} bytes", index.size(), out.position());
    }

    /** Sections written so far, in file order. */
    public List<SectionId> writtenSections() {
        return Collections.unmodifiableList(writtenSections);
    }

    /** Bytes written so far, including the skeleton. */
    public long position() {
        return out.position();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    private final class SectionWriterImpl implements SectionWriter {
        private final SectionId id;
        private final long sectionOffset;
        private boolean managerWritten;
        private long managerLength;
        private boolean closed;
        private long numEntries;
        // the entries frame, opened at the first entry
        private OutputStream entriesStream;

        SectionWriterImpl(SectionId id) {
            this.id = id;
            this.sectionOffset = out.position();
            out.startSectionCrc();
        }

        @Override
        public void writeManager(Message manager) throws IOException {
            ensureOpen();
            Preconditions.checkState(!managerWritten, "section %s already has a manager message", id);
            Preconditions.checkState(numEntries == 0, "section %s: the manager message must precede the entries", id);
            int size = manager.getSerializedSize();
            if (size > ImageV3Format.MANAGER_MESSAGE_SOFT_LIMIT_BYTES) {
                LOG.warn("manager message of section {} is {} bytes, above the {} bytes soft limit; " +
                        "large lists belong in entries", id, size, ImageV3Format.MANAGER_MESSAGE_SOFT_LIMIT_BYTES);
            }
            // Its own frame, so a loader can read the manager message without decoding the entries.
            try (OutputStream frame = codec.compress(out)) {
                manager.writeDelimitedTo(frame);
            }
            managerWritten = true;
            managerLength = out.position() - sectionOffset;
        }

        @Override
        public void writeEntry(Message entry) throws IOException {
            ensureOpen();
            if (entriesStream == null) {
                entriesStream = codec.compress(out);
            }
            entry.writeDelimitedTo(entriesStream);
            numEntries++;
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            if (entriesStream != null) {
                entriesStream.close();
                entriesStream = null;
            }
            long length = out.position() - sectionOffset;
            index.add(IndexEntryPB.newBuilder()
                    .setType(id.getValue())
                    .setVersion(ImageV3Format.CURRENT_SECTION_VERSION)
                    .setPayloadFormat(PayloadFormat.PAYLOAD_FORMAT_PB)
                    .setOffset(sectionOffset)
                    .setLength(length)
                    .setNumEntries(numEntries)
                    .setManagerLength(managerLength)
                    .setCrc32(out.finishSectionCrc())
                    .build());
            writtenSections.add(id);
            closed = true;
            openSection = null;
            LOG.debug("wrote section {}: {} entries, {} bytes", id, numEntries, length);
        }

        private void ensureOpen() {
            Preconditions.checkState(!closed, "section %s is already closed", id);
        }
    }

    /** Tracks the absolute position, the whole-file crc32 and, while a section is open, its crc32. */
    private static final class CountingOutput extends OutputStream {
        private final OutputStream delegate;
        private final CRC32 fileCrc = new CRC32();
        private CRC32 sectionCrc;
        private long position;

        CountingOutput(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            position++;
            fileCrc.update(b);
            if (sectionCrc != null) {
                sectionCrc.update(b);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            position += len;
            fileCrc.update(b, off, len);
            if (sectionCrc != null) {
                sectionCrc.update(b, off, len);
            }
        }

        void writeIntBigEndian(int v) throws IOException {
            write(new byte[] {(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v});
        }

        long position() {
            return position;
        }

        long fileCrc() {
            return fileCrc.getValue();
        }

        void startSectionCrc() {
            sectionCrc = new CRC32();
        }

        int finishSectionCrc() {
            int value = (int) sectionCrc.getValue();
            sectionCrc = null;
            return value;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
